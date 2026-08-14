use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::handler::{LoadAuthzFn, load_authz_boxed};
use crate::lib::moderation::{
    self, ACTION_SUPPRESS_EMBEDS, ACTION_UNSUPPRESS_EMBEDS, WriteRecordFn, write_moderation_boxed,
};
use crate::lib::permissions::Permission;
use crate::lib::relay::{RelayContext, WriteDeps, with_community_write};
use crate::lib::responses::{ErrorCode, ErrorResponse};
use crate::models::record_data;

const MESSAGE_NSID: &str = "social.colibri.message";

const MAX_EMBEDS_PER_ACTION: usize = 32;

#[derive(Serialize, Deserialize, Debug)]
pub struct SuppressMessageEmbedsResponse {
    pub message: String,
}

#[derive(Deserialize)]
struct StoredMessageChannel {
    channel: String,
}

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

fn validate_embeds(embeds: Vec<String>) -> Result<Vec<String>, ErrorResponse> {
    let mut seen: Vec<String> = Vec::new();
    for embed in embeds {
        let trimmed = embed.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !seen.iter().any(|e| e == trimmed) {
            seen.push(trimmed.to_string());
        }
    }
    if seen.is_empty() {
        return Err(ErrorCode::InvalidRequest.with("At least one embed URL is required."));
    }
    if seen.len() > MAX_EMBEDS_PER_ACTION {
        return Err(
            ErrorCode::InvalidRequest.with("Too many embed URLs for a single action (max 32).")
        );
    }
    Ok(seen)
}

#[allow(clippy::too_many_arguments)]
async fn suppress_embeds_with(
    relay: RelayContext,
    action: &'static str,
    lxm: &'static str,
    community_uri: String,
    message_uri: String,
    embeds: Vec<String>,
    auth: String,
    db: DatabaseConnection,
    deps: WriteDeps<'_>,
    load_authz_fn: &LoadAuthzFn,
    write_record_fn: &WriteRecordFn,
) -> Result<Json<SuppressMessageEmbedsResponse>, ErrorResponse> {
    let Some(message) = AtUri::parse(&message_uri) else {
        return Err(ErrorCode::InvalidRequest.with("Invalid message AT-URI."));
    };
    let embeds = validate_embeds(embeds)?;
    let channel_rkey = message_channel_rkey(&db, &message).await?;

    let deps = WriteDeps {
        load_authz_fn,
        ..deps
    };

    with_community_write(
        relay,
        auth,
        lxm,
        community_uri,
        Some(Permission::MessageDelete),
        channel_rkey.as_deref(),
        db,
        &deps,
        |ctx, db| async move {
            moderation::issue_embed_action(
                write_record_fn,
                db,
                ctx.community,
                action,
                message_uri.clone(),
                embeds,
                ctx.caller_did,
            )
            .await?;
            Ok(Json(SuppressMessageEmbedsResponse {
                message: message_uri,
            }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.community.suppressMessageEmbeds?<community>&<message>&<embeds>&<auth>"
)]
pub async fn suppress_message_embeds(
    relay: RelayContext,
    community: &str,
    message: &str,
    embeds: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<SuppressMessageEmbedsResponse>, ErrorResponse> {
    suppress_embeds_with(
        relay,
        ACTION_SUPPRESS_EMBEDS,
        "social.colibri.community.suppressMessageEmbeds",
        community.to_string(),
        message.to_string(),
        embeds,
        auth.to_string(),
        db.inner().clone(),
        WriteDeps::production(),
        &load_authz_boxed,
        &write_moderation_boxed,
    )
    .await
}

#[post(
    "/xrpc/social.colibri.community.unsuppressMessageEmbeds?<community>&<message>&<embeds>&<auth>"
)]
pub async fn unsuppress_message_embeds(
    relay: RelayContext,
    community: &str,
    message: &str,
    embeds: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<SuppressMessageEmbedsResponse>, ErrorResponse> {
    suppress_embeds_with(
        relay,
        ACTION_UNSUPPRESS_EMBEDS,
        "social.colibri.community.unsuppressMessageEmbeds",
        community.to_string(),
        message.to_string(),
        embeds,
        auth.to_string(),
        db.inner().clone(),
        WriteDeps::production(),
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
    use crate::lib::test_fixtures::{
        local_write_deps, mock_db, relay_ctx, write_deps_never_authenticating,
    };
    use futures::future::BoxFuture;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

    const COMMUNITY: &str = "at://did:plc:owner/social.colibri.community/c1";
    const MESSAGE: &str = "at://did:plc:alice/social.colibri.message/msg-1";

    fn db_with_no_message_record() -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<record_data::Model>::new()])
            .into_connection()
    }

    fn owner_authz() -> &'static LoadAuthzFn {
        &|_, _, _| {
            Box::pin(async {
                Ok(ActorAuthz {
                    is_owner: true,
                    member: None,
                    roles: vec![],
                })
            })
        }
    }

    #[tokio::test]
    async fn suppress_writes_the_url_list_onto_the_action() {
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

        let result = suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            vec![String::from("https://a"), String::from("https://b")],
            String::from("token"),
            db,
            local_write_deps("did:plc:owner"),
            owner_authz(),
            &write_record,
        )
        .await
        .unwrap();

        assert_eq!(result.message, MESSAGE);
        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(written.action, "suppressEmbeds");
        assert_eq!(written.subject.uri.as_deref(), Some(MESSAGE));
        assert_eq!(
            written.embeds.as_deref(),
            Some(&[String::from("https://a"), String::from("https://b")][..])
        );
    }

    #[tokio::test]
    async fn unsuppress_records_the_reversing_action() {
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
                    rkey: String::from("mod-2"),
                    data: serde_json::json!({}),
                    indexed_at: String::from(""),
                })
            })
        };

        suppress_embeds_with(
            relay_ctx(),
            ACTION_UNSUPPRESS_EMBEDS,
            "social.colibri.community.unsuppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            vec![String::from("https://a")],
            String::from("token"),
            db,
            local_write_deps("did:plc:owner"),
            owner_authz(),
            &write_record,
        )
        .await
        .unwrap();

        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(written.action, "unsuppressEmbeds");
        assert_eq!(
            written.embeds.as_deref(),
            Some(&[String::from("https://a")][..])
        );
    }

    #[tokio::test]
    async fn duplicate_and_blank_urls_are_normalized_away() {
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
                    rkey: String::from("mod-3"),
                    data: serde_json::json!({}),
                    indexed_at: String::from(""),
                })
            })
        };

        suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            vec![
                String::from("https://a"),
                String::from("  "),
                String::from("https://a"),
                String::from("https://b"),
            ],
            String::from("token"),
            db,
            local_write_deps("did:plc:owner"),
            owner_authz(),
            &write_record,
        )
        .await
        .unwrap();

        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(
            written.embeds.as_deref(),
            Some(&[String::from("https://a"), String::from("https://b")][..])
        );
    }

    #[tokio::test]
    async fn an_empty_url_list_is_rejected_before_any_write() {
        let db = db_with_no_message_record();
        let result = suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            vec![String::from("   ")],
            String::from("token"),
            db,
            write_deps_never_authenticating(),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _, _| Box::pin(async { panic!("should not write") }),
        )
        .await;

        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn an_oversized_url_list_is_rejected() {
        let db = db_with_no_message_record();
        let embeds: Vec<String> = (0..40)
            .map(|i| format!("https://example.com/{i}"))
            .collect();

        let result = suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            embeds,
            String::from("token"),
            db,
            write_deps_never_authenticating(),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _, _| Box::pin(async { panic!("should not write") }),
        )
        .await;

        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn a_malformed_message_uri_is_rejected() {
        let db = mock_db();
        let result = suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from("not-a-uri"),
            vec![String::from("https://a")],
            String::from("token"),
            db,
            write_deps_never_authenticating(),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _, _| Box::pin(async { panic!("should not write") }),
        )
        .await;

        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn a_caller_without_message_hide_is_refused() {
        let db = db_with_no_message_record();
        let result = suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            vec![String::from("https://a")],
            String::from("token"),
            db,
            local_write_deps("did:plc:rando"),
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

        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn a_channel_override_denying_message_hide_is_honoured() {
        use crate::lib::test_fixtures::{member, role_with_override};

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![record_data::Model {
                id: 1,
                did: String::from("did:plc:alice"),
                nsid: String::from(MESSAGE_NSID),
                rkey: String::from("msg-1"),
                data: serde_json::json!({ "channel": "chan-a" }),
                indexed_at: String::new(),
            }]])
            .into_connection();

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

        let result = suppress_embeds_with(
            relay_ctx(),
            ACTION_SUPPRESS_EMBEDS,
            "social.colibri.community.suppressMessageEmbeds",
            String::from(COMMUNITY),
            String::from(MESSAGE),
            vec![String::from("https://a")],
            String::from("token"),
            db,
            local_write_deps("did:plc:mod"),
            &move |_, _, _| {
                let authz = authz.clone();
                Box::pin(async move { Ok(authz) })
            },
            &|_, _, _| Box::pin(async { panic!("should not write: override denies this") }),
        )
        .await;

        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }
}
