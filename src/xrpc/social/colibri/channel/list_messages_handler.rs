use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::reactions::{ReactionSummary, group_reactions_for_messages};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

const MESSAGE_NSID: &str = "social.colibri.message";
const CHANNEL_NSID: &str = "social.colibri.channel";
const COMMUNITY_NSID: &str = "social.colibri.community";
const DEFAULT_LIMIT: u64 = 100;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub uri: String,
    pub text: String,
    #[serde(default)]
    pub facets: Vec<Value>,
    pub channel: String,
    pub community: String,
    pub author: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Value>,
    pub reactions: Vec<ReactionSummary>,
}

#[derive(Serialize, Debug)]
pub struct MessageList {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub messages: Vec<Message>,
}

#[derive(Deserialize)]
struct StoredMessage {
    text: String,
    #[serde(default)]
    facets: Option<Vec<Value>>,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    attachments: Option<Vec<Value>>,
}

#[derive(Deserialize)]
struct StoredChannel {
    community: String,
}

/// Fetches the channel record so the response can include a fully-qualified
/// community AT-URI. Returns `None` if no matching channel record exists.
pub async fn fetch_channel_record(
    db: &DatabaseConnection,
    authority: &str,
    channel_rkey: &str,
) -> Result<Option<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(authority))
        .filter(record_data::Column::Nsid.eq(CHANNEL_NSID))
        .filter(record_data::Column::Rkey.eq(channel_rkey))
        .one(db)
        .await
}

/// Fetches a page of message records targeting the given channel rkey,
/// ordered by rkey descending. The optional cursor filters out rkeys at or
/// past the cursor, matching the listRecords convention.
pub async fn fetch_message_page(
    db: &DatabaseConnection,
    channel_rkey: &str,
    limit: u64,
    cursor: Option<&str>,
) -> Result<Vec<record_data::Model>, DbErr> {
    let mut condition = Condition::all()
        .add(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .add(Expr::cust_with_values(
            r#""record_data"."data"->>'channel' = $1"#,
            vec![sea_orm::Value::from(channel_rkey.to_string())],
        ));
    if let Some(c) = cursor {
        condition = condition.add(record_data::Column::Rkey.lt(c));
    }

    record_data::Entity::find()
        .filter(condition)
        .order_by_desc(record_data::Column::Rkey)
        .limit(limit)
        .all(db)
        .await
}

/// Resolves parent message rkeys to fully-qualified AT-URIs. Looks up the
/// authors of each rkey within the same channel.
pub async fn fetch_parent_uris(
    db: &DatabaseConnection,
    channel_rkey: &str,
    parent_rkeys: &[String],
) -> Result<HashMap<String, String>, DbErr> {
    if parent_rkeys.is_empty() {
        return Ok(HashMap::new());
    }
    let parents = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .filter(record_data::Column::Rkey.is_in(parent_rkeys.to_vec()))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'channel' = $1"#,
            vec![sea_orm::Value::from(channel_rkey.to_string())],
        ))
        .all(db)
        .await?;

    Ok(parents
        .into_iter()
        .map(|p| {
            (
                p.rkey.clone(),
                format!("at://{}/{}/{}", p.did, p.nsid, p.rkey),
            )
        })
        .collect())
}

fn build_message(
    record: &record_data::Model,
    channel_uri: &str,
    community_uri: &str,
    parent_uri: Option<String>,
    reactions: Vec<ReactionSummary>,
) -> Option<Message> {
    let stored = serde_json::from_value::<StoredMessage>(record.data.clone()).ok()?;
    Some(Message {
        uri: format!("at://{}/{}/{}", record.did, record.nsid, record.rkey),
        text: stored.text,
        facets: stored.facets.unwrap_or_default(),
        channel: channel_uri.to_string(),
        community: community_uri.to_string(),
        author: record.did.clone(),
        parent: parent_uri,
        attachments: stored.attachments.unwrap_or_default(),
        reactions,
    })
}

pub struct MessagePage {
    pub records: Vec<record_data::Model>,
    pub community_uri: String,
    pub parents: HashMap<String, String>,
    pub reactions: HashMap<String, Vec<ReactionSummary>>,
}

pub async fn assemble_message_page(
    db: &DatabaseConnection,
    channel: &AtUri,
    limit: u64,
    cursor: Option<&str>,
) -> Result<MessagePage, DbErr> {
    let channel_record = fetch_channel_record(db, &channel.authority, &channel.rkey).await?;
    let community_uri = channel_record
        .and_then(|r| serde_json::from_value::<StoredChannel>(r.data).ok())
        .map(|c| {
            format!(
                "at://{}/{}/{}",
                channel.authority, COMMUNITY_NSID, c.community
            )
        })
        .unwrap_or_default();

    let records = fetch_message_page(db, &channel.rkey, limit, cursor).await?;

    let message_rkeys: Vec<String> = records.iter().map(|r| r.rkey.clone()).collect();
    let reactions = group_reactions_for_messages(db, &message_rkeys).await?;

    let parent_rkeys: Vec<String> = records
        .iter()
        .filter_map(|r| {
            serde_json::from_value::<StoredMessage>(r.data.clone())
                .ok()
                .and_then(|m| m.parent)
        })
        .collect();
    let parents = fetch_parent_uris(db, &channel.rkey, &parent_rkeys).await?;

    Ok(MessagePage {
        records,
        community_uri,
        parents,
        reactions,
    })
}

type AssemblePageFn = dyn Fn(
        DatabaseConnection,
        AtUri,
        u64,
        Option<String>,
    ) -> BoxFuture<'static, Result<MessagePage, DbErr>>
    + Send
    + Sync;

async fn list_messages_with(
    channel_uri: String,
    limit: Option<u64>,
    cursor: Option<String>,
    db: DatabaseConnection,
    assemble_fn: &AssemblePageFn,
) -> Result<Json<MessageList>, ErrorResponse> {
    let channel = AtUri::parse(&channel_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid channel AT-URI."),
        }),
    })?;

    let effective_limit = limit.unwrap_or(DEFAULT_LIMIT);
    let page = assemble_fn(db, channel, effective_limit, cursor).await?;

    let next_cursor = if (page.records.len() as u64) == effective_limit {
        page.records.last().map(|r| r.rkey.clone())
    } else {
        None
    };

    let messages = page
        .records
        .iter()
        .filter_map(|record| {
            let stored = serde_json::from_value::<StoredMessage>(record.data.clone()).ok()?;
            let parent_uri = stored
                .parent
                .as_ref()
                .and_then(|rkey| page.parents.get(rkey).cloned());
            let reactions = page
                .reactions
                .get(&record.rkey)
                .cloned()
                .unwrap_or_default();
            build_message(
                record,
                &channel_uri,
                &page.community_uri,
                parent_uri,
                reactions,
            )
        })
        .collect();

    Ok(Json(MessageList {
        cursor: next_cursor,
        messages,
    }))
}

fn assemble_message_page_boxed(
    db: DatabaseConnection,
    channel: AtUri,
    limit: u64,
    cursor: Option<String>,
) -> BoxFuture<'static, Result<MessagePage, DbErr>> {
    Box::pin(async move { assemble_message_page(&db, &channel, limit, cursor.as_deref()).await })
}

#[get("/xrpc/social.colibri.channel.listMessages?<channel>&<limit>&<cursor>&<all>")]
/// Returns a paginated list of messages for a channel, newest first.
///
/// The `all` parameter is accepted to match the spec but is reserved for
/// future filtering (e.g. including blocked messages).
pub async fn list_messages(
    channel: &str,
    limit: Option<u64>,
    cursor: Option<&str>,
    all: Option<bool>,
    db: &State<DatabaseConnection>,
) -> Result<Json<MessageList>, ErrorResponse> {
    let _ = all;
    list_messages_with(
        channel.to_string(),
        limit,
        cursor.map(|c| c.to_string()),
        db.inner().clone(),
        &assemble_message_page_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn message_record(
        rkey: &str,
        author: &str,
        text: &str,
        parent: Option<&str>,
    ) -> record_data::Model {
        let mut data = serde_json::json!({
            "text": text,
            "channel": "chan-a",
            "createdAt": "2026-05-13T00:00:00Z",
        });
        if let Some(p) = parent {
            data["parent"] = serde_json::Value::String(p.to_string());
        }
        record_data::Model {
            id: 0,
            did: author.to_string(),
            nsid: MESSAGE_NSID.to_string(),
            rkey: rkey.to_string(),
            data,
        }
    }

    #[tokio::test]
    async fn builds_message_list_from_page() {
        let db = mock_db();

        let result = list_messages_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            Some(2),
            None,
            db,
            &|_, _, _, _| {
                Box::pin(async {
                    Ok(MessagePage {
                        records: vec![
                            message_record("msg-2", "did:plc:bob", "hi", Some("msg-1")),
                            message_record("msg-1", "did:plc:alice", "hello", None),
                        ],
                        community_uri: String::from(
                            "at://did:plc:owner/social.colibri.community/c1",
                        ),
                        parents: HashMap::from([(
                            String::from("msg-1"),
                            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
                        )]),
                        reactions: HashMap::from([(
                            String::from("msg-1"),
                            vec![ReactionSummary {
                                emoji: String::from("🦜"),
                                count: 1,
                                reactor_dids: vec![String::from("did:plc:carol")],
                            }],
                        )]),
                    })
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.messages.len(), 2);
        assert_eq!(
            result.messages[0].uri,
            "at://did:plc:bob/social.colibri.message/msg-2"
        );
        assert_eq!(
            result.messages[0].community,
            "at://did:plc:owner/social.colibri.community/c1"
        );
        assert_eq!(
            result.messages[0].channel,
            "at://did:plc:owner/social.colibri.channel/chan-a"
        );
        assert_eq!(
            result.messages[0].parent.as_deref(),
            Some("at://did:plc:alice/social.colibri.message/msg-1")
        );
        assert!(result.messages[0].reactions.is_empty());
        assert_eq!(result.messages[1].reactions.len(), 1);
        assert_eq!(result.messages[1].reactions[0].emoji, "🦜");

        assert_eq!(result.cursor.as_deref(), Some("msg-1"));
    }

    #[tokio::test]
    async fn omits_cursor_when_page_is_not_full() {
        let db = mock_db();
        let result = list_messages_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            Some(10),
            None,
            db,
            &|_, _, _, _| {
                Box::pin(async {
                    Ok(MessagePage {
                        records: vec![message_record("msg-1", "did:plc:alice", "hello", None)],
                        community_uri: String::from(
                            "at://did:plc:owner/social.colibri.community/c1",
                        ),
                        parents: HashMap::new(),
                        reactions: HashMap::new(),
                    })
                })
            },
        )
        .await
        .unwrap();

        assert!(result.cursor.is_none());
    }

    #[tokio::test]
    async fn rejects_invalid_channel_uri() {
        let db = mock_db();
        let result =
            list_messages_with(String::from("not-a-uri"), None, None, db, &|_, _, _, _| {
                Box::pin(async { panic!("should not assemble when uri is invalid") })
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
