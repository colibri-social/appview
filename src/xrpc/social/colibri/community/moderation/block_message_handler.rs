use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriModerationSubject;
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz_scoped,
};
use crate::lib::moderation::{self, ACTION_HIDE_MESSAGE, WriteRecordFn, write_moderation_boxed};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

const MESSAGE_NSID: &str = "social.colibri.message";

#[derive(Serialize, Debug)]
pub struct BlockMessageResponse {
    pub message: String,
}

#[derive(Deserialize)]
struct StoredMessageChannel {
    channel: String,
}

/// The bare channel rkey a message belongs to, so `blockMessage` can be
/// gated with the channel scope a moderator's permissions might be
/// overridden for. `None` if the message isn't indexed or its `channel`
/// field is missing/malformed.
async fn message_channel_rkey(
    db: &DatabaseConnection,
    message: &AtUri,
) -> Result<Option<String>, DbErr> {
    let record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&message.authority))
        .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .filter(record_data::Column::Rkey.eq(&message.rkey))
        .one(db)
        .await?;
    Ok(record
        .and_then(|r| serde_json::from_value::<StoredMessageChannel>(r.data).ok())
        .map(|m| AtUri::rkey_or_value(&m.channel)))
}

async fn block_message_with(
    community_uri: String,
    message_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    write_record_fn: &WriteRecordFn,
) -> Result<Json<BlockMessageResponse>, ErrorResponse> {
    let Some(message) = AtUri::parse(&message_uri) else {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid message AT-URI."),
            }),
        });
    };
    let channel_rkey = message_channel_rkey(&db, &message).await?;

    with_community_authz_scoped(
        auth,
        "social.colibri.community.blockMessage",
        community_uri,
        Some(Permission::MessageDelete),
        channel_rkey.as_deref(),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            moderation::issue_action(
                write_record_fn,
                db,
                ctx.community,
                ACTION_HIDE_MESSAGE,
                ColibriModerationSubject {
                    did: None,
                    uri: Some(message_uri.clone()),
                },
                ctx.caller_did,
                None,
            )
            .await?;
            Ok(Json(BlockMessageResponse {
                message: message_uri,
            }))
        },
    )
    .await
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
    use crate::lib::colibri::ColibriModeration;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::mock_db;
    use crate::models::record_data;
    use futures::future::BoxFuture;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, DbErr, MockDatabase};
    use std::sync::{Arc, Mutex};

    /// A mock DB whose only queued query result is "no indexed message
    /// record found".
    fn db_with_no_message_record() -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<record_data::Model>::new()])
            .into_connection()
    }

    #[tokio::test]
    async fn block_message_writes_hide_record_when_authorized() {
        let db = db_with_no_message_record();
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
                    indexed_at: String::from(""),
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
        let db = mock_db();
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
        let db = db_with_no_message_record();
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

    fn db_with_message_in_channel(channel: &str) -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![record_data::Model {
                id: 1,
                did: String::from("did:plc:alice"),
                nsid: String::from(MESSAGE_NSID),
                rkey: String::from("msg-1"),
                data: serde_json::json!({ "channel": channel }),
                indexed_at: String::new(),
            }]])
            .into_connection()
    }

    // A moderator holds `message.hide` in general but has it explicitly denied
    // in `chan-a` via a channel override. blockMessage must resolve the
    // message's channel and consult that override, not just the flat
    // permission list.
    #[tokio::test]
    async fn block_message_rejects_when_channel_override_denies_message_delete() {
        use crate::lib::test_fixtures::{member, role_with_override};

        let db = db_with_message_in_channel("chan-a");
        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:mod", vec!["mod"])),
            roles: vec![role_with_override(
                "Moderator",
                10,
                vec![Permission::MessageDelete],
                "chan-a",
                vec![],
                vec![Permission::MessageDelete],
            )],
        };

        let result = block_message_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &move |_, _, _| {
                let authz = authz.clone();
                Box::pin(async move { Ok(authz) })
            },
            &|_, _, _| Box::pin(async { panic!("should not write: override denies this") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    // The same override only applies to `chan-a` — a message in a different
    // channel is unaffected and the base permission still grants access.
    #[tokio::test]
    async fn block_message_allows_when_override_targets_a_different_channel() {
        use crate::lib::colibri::ColibriModeration;
        use crate::lib::test_fixtures::{member, role_with_override};

        let db = db_with_message_in_channel("chan-b");
        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:mod", vec!["mod"])),
            roles: vec![role_with_override(
                "Moderator",
                10,
                vec![Permission::MessageDelete],
                "chan-a",
                vec![],
                vec![Permission::MessageDelete],
            )],
        };

        let write_record = |_: DatabaseConnection,
                            _: AtUri,
                            _: ColibriModeration|
         -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
            Box::pin(async {
                Ok(record_data::Model {
                    id: 1,
                    did: String::from("did:plc:owner"),
                    nsid: String::from("social.colibri.moderation"),
                    rkey: String::from("mod-1"),
                    data: serde_json::json!({}),
                    indexed_at: String::from(""),
                })
            })
        };

        let result = block_message_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &move |_, _, _| {
                let authz = authz.clone();
                Box::pin(async move { Ok(authz) })
            },
            &write_record,
        )
        .await;

        assert!(result.is_ok());
    }
}
