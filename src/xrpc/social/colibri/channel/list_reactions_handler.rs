use std::collections::HashSet;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::moderation::{currently_banned_dids, is_user_banned};
use crate::lib::reactions::{ReactionSummary, list_reactions_for_message};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

const MESSAGE_NSID: &str = "social.colibri.message";
const CHANNEL_NSID: &str = "social.colibri.channel";

#[derive(Serialize, Debug)]
pub struct ReactionList {
    pub reactions: Vec<ReactionSummary>,
}

#[derive(Deserialize)]
struct StoredMessageChannel {
    channel: String,
}

type AssembleReactionsFn = dyn Fn(DatabaseConnection, AtUri) -> BoxFuture<'static, Result<Vec<ReactionSummary>, DbErr>>
    + Send
    + Sync;

async fn list_reactions_with(
    message_uri: String,
    db: DatabaseConnection,
    assemble_fn: &AssembleReactionsFn,
) -> Result<Json<ReactionList>, ErrorResponse> {
    let parsed = AtUri::parse(&message_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid message AT-URI."),
        }),
    })?;

    let reactions = assemble_fn(db, parsed).await?;
    Ok(Json(ReactionList { reactions }))
}

/// Production assembler: resolves the message's community, suppresses output
/// entirely if the message author is banned there, otherwise loads the
/// reactions and strips reactor DIDs that are currently banned.
pub async fn assemble_reactions(
    db: &DatabaseConnection,
    message: &AtUri,
) -> Result<Vec<ReactionSummary>, DbErr> {
    let Some(community_did) = resolve_community_did(db, message).await? else {
        // No matching message/channel record yet (e.g., not backfilled);
        // fall back to unfiltered reactions.
        return list_reactions_for_message(db, message).await;
    };

    if is_user_banned(db, &community_did, &message.authority).await? {
        return Ok(Vec::new());
    }

    let banned: HashSet<String> = currently_banned_dids(db, &community_did)
        .await?
        .into_iter()
        .collect();
    let reactions = list_reactions_for_message(db, message).await?;
    Ok(strip_banned_reactors(reactions, &banned))
}

/// Looks up the message record, then the channel record, to determine which
/// community the message belongs to. Returns `None` if either record is
/// missing or has an unexpected shape.
async fn resolve_community_did(
    db: &DatabaseConnection,
    message: &AtUri,
) -> Result<Option<String>, DbErr> {
    let message_record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&message.authority))
        .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .filter(record_data::Column::Rkey.eq(&message.rkey))
        .one(db)
        .await?;
    let Some(channel_rkey) = message_record
        .and_then(|r| serde_json::from_value::<StoredMessageChannel>(r.data).ok())
        .map(|m| m.channel)
    else {
        return Ok(None);
    };

    let channel_record = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(CHANNEL_NSID))
        .filter(record_data::Column::Rkey.eq(&channel_rkey))
        .one(db)
        .await?;
    Ok(channel_record.map(|r| r.did))
}

fn strip_banned_reactors(
    reactions: Vec<ReactionSummary>,
    banned: &HashSet<String>,
) -> Vec<ReactionSummary> {
    if banned.is_empty() {
        return reactions;
    }
    reactions
        .into_iter()
        .filter_map(|mut s| {
            s.reactor_dids.retain(|d| !banned.contains(d));
            if s.reactor_dids.is_empty() {
                None
            } else {
                s.count = s.reactor_dids.len() as u32;
                Some(s)
            }
        })
        .collect()
}

fn assemble_reactions_boxed(
    db: DatabaseConnection,
    message: AtUri,
) -> BoxFuture<'static, Result<Vec<ReactionSummary>, DbErr>> {
    Box::pin(async move { assemble_reactions(&db, &message).await })
}

#[get("/xrpc/social.colibri.channel.listReactions?<message>")]
/// Lists all reactions targeting a specific message, grouped by emoji.
pub async fn list_reactions(
    message: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReactionList>, ErrorResponse> {
    list_reactions_with(
        message.to_string(),
        db.inner().clone(),
        &assemble_reactions_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_reactions_from_assemble_fn() {
        let db = mock_db();
        let result = list_reactions_with(
            String::from("at://did:plc:author/social.colibri.message/msg-1"),
            db,
            &|_, message| {
                assert_eq!(message.rkey, "msg-1");
                Box::pin(async {
                    Ok(vec![ReactionSummary {
                        emoji: String::from("🦜"),
                        count: 2,
                        reactor_dids: vec![
                            String::from("did:plc:alice"),
                            String::from("did:plc:bob"),
                        ],
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.reactions.len(), 1);
        assert_eq!(result.reactions[0].emoji, "🦜");
        assert_eq!(result.reactions[0].count, 2);
    }

    #[tokio::test]
    async fn rejects_invalid_message_uri() {
        let db = mock_db();
        let result = list_reactions_with(String::from("invalid"), db, &|_, _| {
            Box::pin(async { panic!("should not call when uri is invalid") })
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn propagates_db_error_as_upstream_error() {
        let db = mock_db();
        let result = list_reactions_with(
            String::from("at://did:plc:author/social.colibri.message/msg-1"),
            db,
            &|_, _| Box::pin(async { Err(DbErr::Custom(String::from("boom"))) }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "UpstreamError"
        );
    }

    #[test]
    fn strip_banned_reactors_drops_banned_dids_and_empty_groups() {
        let banned: HashSet<String> = ["did:plc:alice".to_string()].into_iter().collect();
        let reactions = vec![
            ReactionSummary {
                emoji: String::from("🦜"),
                count: 2,
                reactor_dids: vec![String::from("did:plc:alice"), String::from("did:plc:bob")],
            },
            ReactionSummary {
                emoji: String::from("🔥"),
                count: 1,
                reactor_dids: vec![String::from("did:plc:alice")],
            },
        ];

        let filtered = strip_banned_reactors(reactions, &banned);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].emoji, "🦜");
        assert_eq!(filtered[0].count, 1);
        assert_eq!(filtered[0].reactor_dids, vec![String::from("did:plc:bob")]);
    }

    #[test]
    fn strip_banned_reactors_is_noop_when_set_empty() {
        let banned: HashSet<String> = HashSet::new();
        let reactions = vec![ReactionSummary {
            emoji: String::from("🦜"),
            count: 1,
            reactor_dids: vec![String::from("did:plc:alice")],
        }];
        let filtered = strip_banned_reactors(reactions.clone(), &banned);
        assert_eq!(filtered, reactions);
    }
}
