use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriModeration, ColibriModerationSubject};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation::{self, ACTION_HIDE_MESSAGE};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data;

#[derive(Serialize, Debug)]
pub struct BlockMessageResponse {
    pub message: String,
}

type WriteRecordFn = dyn Fn(
        DatabaseConnection,
        AtUri,
        ColibriModeration,
    ) -> BoxFuture<'static, Result<record_data::Model, DbErr>>
    + Send
    + Sync;

async fn block_message_with(
    community_uri: String,
    message_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    write_record_fn: &WriteRecordFn,
) -> Result<Json<BlockMessageResponse>, ErrorResponse> {
    if AtUri::parse(&message_uri).is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid message AT-URI."),
            }),
        });
    }

    with_community_authz(
        auth,
        "social.colibri.community.blockMessage",
        community_uri,
        Some(Permission::MessageDelete),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let record = moderation::moderation_record(
                ACTION_HIDE_MESSAGE,
                ColibriModerationSubject {
                    did: None,
                    uri: Some(message_uri.clone()),
                },
                ctx.caller_did,
                current_iso8601_utc(),
                None,
            );
            write_record_fn(db, ctx.community, record).await?;
            Ok(Json(BlockMessageResponse {
                message: message_uri,
            }))
        },
    )
    .await
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
        &verify_auth_boxed,
        &load_authz_boxed,
        &write_moderation_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::community_authz::ActorAuthz;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn block_message_writes_hide_record_when_authorized() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let captured: Arc<Mutex<Option<ColibriModeration>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let write_record = move |_: DatabaseConnection,
                                 _: AtUri,
                                 record: ColibriModeration|
              -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
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
        };
        let result = block_message_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: true,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            &write_record,
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
            &|_, _| Box::pin(async { panic!("should not authenticate when uri is invalid") }),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _, _| Box::pin(async { panic!("should not write") }),
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
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not write") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }
}
