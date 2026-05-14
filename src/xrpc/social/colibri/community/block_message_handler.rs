use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriModeration, ColibriModerationSubject};
use crate::lib::community_authz::{self, ActorAuthz};
use crate::lib::moderation::{self, ACTION_HIDE_MESSAGE};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data;

#[derive(Serialize, Debug)]
pub struct BlockMessageResponse {
    pub message: String,
}

async fn block_message_with<V, W>(
    community_uri: String,
    message_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: V,
    load_authz_fn: W,
    write_record_fn: impl FnOnce(
        DatabaseConnection,
        AtUri,
        ColibriModeration,
    ) -> BoxFuture<'static, Result<record_data::Model, DbErr>>,
) -> Result<Json<BlockMessageResponse>, ErrorResponse>
where
    V: Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>,
    W: Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<ActorAuthz, DbErr>>,
{
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    if AtUri::parse(&message_uri).is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid message AT-URI."),
            }),
        });
    }

    let caller_did = verify_auth_fn(auth, String::from("social.colibri.community.blockMessage"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let caller_authz = load_authz_fn(db.clone(), community_uri.clone(), caller_did.clone()).await?;
    if !caller_authz.has(Permission::MessageDelete, None) {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("Forbidden"),
                message: format!("Missing permission: {}", Permission::MessageDelete),
            }),
        });
    }

    let record = moderation::moderation_record(
        ACTION_HIDE_MESSAGE,
        ColibriModerationSubject {
            did: None,
            uri: Some(message_uri.clone()),
        },
        caller_did,
        current_iso8601_utc(),
        None,
    );

    write_record_fn(db, community, record).await?;

    Ok(Json(BlockMessageResponse {
        message: message_uri,
    }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn load_authz_boxed(
    db: DatabaseConnection,
    community_uri: String,
    did: String,
) -> BoxFuture<'static, Result<ActorAuthz, DbErr>> {
    Box::pin(async move { community_authz::load_actor_authz(&db, &community_uri, &did).await })
}

fn write_moderation_boxed(
    db: DatabaseConnection,
    community: AtUri,
    record: ColibriModeration,
) -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
    Box::pin(async move { moderation::write_moderation_record(&db, &community, &record).await })
}

#[post("/xrpc/social.colibri.community.blockMessage?<community>&<message>&<auth>")]
/// Hides a message in a community by writing a `hideMessage` moderation record.
pub async fn block_message(
    community: &str,
    message: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BlockMessageResponse>, ErrorResponse> {
    block_message_with(
        community.to_string(),
        message.to_string(),
        auth.to_string(),
        db.inner().clone(),
        verify_auth_boxed,
        load_authz_boxed,
        write_moderation_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn block_message_writes_hide_record_when_authorized() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let captured: Arc<Mutex<Option<ColibriModeration>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let result = block_message_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            |_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: true,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            move |_, _, record| {
                let captured = captured_clone.clone();
                Box::pin(async move {
                    *captured.lock().unwrap() = Some(record);
                    Ok(record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.moderation"),
                        rkey: String::from("mod-1"),
                        data: serde_json::json!({}),
                    })
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(
            result.message,
            "at://did:plc:alice/social.colibri.message/msg-1"
        );
        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(written.action, "hideMessage");
        assert_eq!(
            written.subject.uri.as_deref(),
            Some("at://did:plc:alice/social.colibri.message/msg-1")
        );
    }

    #[tokio::test]
    async fn block_message_rejects_invalid_message_uri() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = block_message_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("not-a-uri"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { panic!("should not authenticate when uri is invalid") }),
            |_, _, _| Box::pin(async { panic!("should not load authz") }),
            |_, _, _| Box::pin(async { panic!("should not write") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn block_message_rejects_when_caller_lacks_permission() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = block_message_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            |_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            |_, _, _| Box::pin(async { panic!("should not write") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }
}
