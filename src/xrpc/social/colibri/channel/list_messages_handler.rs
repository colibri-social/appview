use std::collections::{HashMap, HashSet};

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
use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::ColibriActorData;
use crate::lib::moderation::currently_banned_dids;
use crate::lib::reactions::{ReactionSummary, group_reactions_for_messages};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::{record_data, repos, user_states};
use crate::xrpc::social::colibri::actor::get_data_handler::{ActorData, ActorStatus};

const MESSAGE_NSID: &str = "social.colibri.message";
const CHANNEL_NSID: &str = "social.colibri.channel";
const COMMUNITY_NSID: &str = "social.colibri.community";
const DEFAULT_LIMIT: u64 = 100;

/// A file attached to a message.
#[derive(Serialize, Deserialize, Debug)]
pub struct Attachment {
    pub blob: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Full profile, status, and handle for a message author.
#[derive(Serialize, Deserialize, Debug)]
pub struct MessageAuthor {
    pub did: String,
    pub handle: String,
    pub data: ActorData,
}

/// A parent message embedded one level deep inside a [`Message`]. Identical
/// shape to `Message` except it has no `parent` field, preventing recursion.
#[derive(Serialize, Deserialize, Debug)]
pub struct ParentMessage {
    pub uri: String,
    pub text: String,
    #[serde(default)]
    pub facets: Vec<Value>,
    pub channel: String,
    pub community: String,
    pub author: MessageAuthor,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    pub reactions: Vec<ReactionSummary>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub edited: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub uri: String,
    pub text: String,
    #[serde(default)]
    pub facets: Vec<Value>,
    pub channel: String,
    pub community: String,
    pub author: MessageAuthor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentMessage>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    pub reactions: Vec<ReactionSummary>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub edited: bool,
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
    attachments: Option<Vec<Attachment>>,
    #[serde(rename = "createdAt")]
    created_at: Option<String>,
    #[serde(default)]
    edited: bool,
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

/// Fetches the full records for parent messages within the same channel,
/// keyed by rkey. Used to embed one level of parent data in the response.
pub async fn fetch_parent_records(
    db: &DatabaseConnection,
    channel_rkey: &str,
    parent_rkeys: &[String],
) -> Result<HashMap<String, record_data::Model>, DbErr> {
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

    Ok(parents.into_iter().map(|p| (p.rkey.clone(), p)).collect())
}

fn build_author(
    did: String,
    handle: Option<String>,
    profile: Option<ActorProfile>,
    actor_data: Option<ColibriActorData>,
    state: Option<String>,
) -> MessageAuthor {
    let handle = handle.unwrap_or_else(|| did.clone());
    let display_name = profile
        .as_ref()
        .and_then(|p| p.display_name.clone())
        .unwrap_or_else(|| handle.clone());
    let (avatar, banner, description) = match profile {
        Some(p) => (p.avatar, p.banner, p.description),
        None => (None, None, None),
    };
    let status = actor_data
        .map(|d| ActorStatus {
            text: d.status,
            emoji: d.emoji,
        })
        .unwrap_or(ActorStatus {
            text: String::new(),
            emoji: None,
        });
    let online_state = state.unwrap_or_else(|| String::from("offline"));
    MessageAuthor {
        did,
        handle,
        data: ActorData {
            display_name,
            avatar,
            banner,
            description,
            online_state,
            status,
        },
    }
}

fn build_message(
    record: &record_data::Model,
    channel_uri: &str,
    community_uri: &str,
    author: MessageAuthor,
    parent: Option<ParentMessage>,
    reactions: Vec<ReactionSummary>,
) -> Option<Message> {
    let stored = serde_json::from_value::<StoredMessage>(record.data.clone()).ok()?;
    Some(Message {
        uri: format!("at://{}/{}/{}", record.did, record.nsid, record.rkey),
        text: stored.text,
        facets: stored.facets.unwrap_or_default(),
        channel: channel_uri.to_string(),
        community: community_uri.to_string(),
        author,
        parent,
        attachments: stored.attachments.unwrap_or_default(),
        reactions,
        created_at: stored
            .created_at
            .unwrap_or_else(|| record.indexed_at.clone()),
        edited: stored.edited,
    })
}

pub struct MessagePage {
    pub records: Vec<record_data::Model>,
    pub community_uri: String,
    /// Full parent message records keyed by rkey. Reactions for these are
    /// included in the top-level `reactions` map so a single fetch covers both.
    pub parent_records: HashMap<String, record_data::Model>,
    pub reactions: HashMap<String, Vec<ReactionSummary>>,
    pub author_profiles: HashMap<String, ActorProfile>,
    pub author_actor_data: HashMap<String, ColibriActorData>,
    pub author_states: HashMap<String, String>,
    pub author_handles: HashMap<String, String>,
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

    let banned: HashSet<String> = currently_banned_dids(db, &channel.authority)
        .await?
        .into_iter()
        .collect();

    let raw_records = fetch_message_page(db, &channel.rkey, limit, cursor).await?;
    let records: Vec<record_data::Model> = raw_records
        .into_iter()
        .filter(|r| !banned.contains(&r.did))
        .collect();

    let parent_rkeys: Vec<String> = records
        .iter()
        .filter_map(|r| {
            serde_json::from_value::<StoredMessage>(r.data.clone())
                .ok()
                .and_then(|m| m.parent)
        })
        .collect();
    let parent_records: HashMap<String, record_data::Model> =
        fetch_parent_records(db, &channel.rkey, &parent_rkeys)
            .await?
            .into_iter()
            .filter(|(_, r)| !banned.contains(&r.did))
            .collect();

    // Fetch reactions for page messages and their parents in one round-trip.
    let all_rkeys: Vec<String> = records
        .iter()
        .map(|r| r.rkey.clone())
        .chain(parent_records.keys().cloned())
        .collect();
    let reactions = strip_banned_reactors(
        group_reactions_for_messages(db, &all_rkeys).await?,
        &banned,
    );

    // Collect unique author DIDs from page messages and their parents.
    let mut author_dids: Vec<String> = records
        .iter()
        .map(|r| r.did.clone())
        .chain(parent_records.values().map(|r| r.did.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    author_dids.sort();

    let (author_profiles, author_actor_data, author_states, author_handles) =
        if author_dids.is_empty() {
            (HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new())
        } else {
            let self_records = record_data::Entity::find()
                .filter(record_data::Column::Did.is_in(author_dids.clone()))
                .filter(record_data::Column::Rkey.eq("self"))
                .filter(
                    Condition::any()
                        .add(record_data::Column::Nsid.eq("app.bsky.actor.profile"))
                        .add(record_data::Column::Nsid.eq("social.colibri.actor.data")),
                )
                .all(db)
                .await?;

            let mut profiles: HashMap<String, ActorProfile> = HashMap::new();
            let mut actor_data: HashMap<String, ColibriActorData> = HashMap::new();
            for rec in self_records {
                if rec.nsid == "app.bsky.actor.profile" {
                    if let Ok(p) = serde_json::from_value::<ActorProfile>(rec.data) {
                        profiles.insert(rec.did, p);
                    }
                } else if rec.nsid == "social.colibri.actor.data" {
                    if let Ok(d) = serde_json::from_value::<ColibriActorData>(rec.data) {
                        actor_data.insert(rec.did, d);
                    }
                }
            }

            let states: HashMap<String, String> = user_states::Entity::find()
                .filter(user_states::Column::Did.is_in(author_dids.clone()))
                .all(db)
                .await?
                .into_iter()
                .map(|row| (row.did, row.state))
                .collect();

            let handles: HashMap<String, String> = repos::Entity::find()
                .filter(repos::Column::Did.is_in(author_dids))
                .all(db)
                .await?
                .into_iter()
                .filter_map(|row| row.handle.map(|h| (row.did, h)))
                .collect();

            (profiles, actor_data, states, handles)
        };

    Ok(MessagePage {
        records,
        community_uri,
        parent_records,
        reactions,
        author_profiles,
        author_actor_data,
        author_states,
        author_handles,
    })
}

/// Removes banned reactors from every [`ReactionSummary`] in the map, dropping
/// groups that become empty and recomputing `count`.
fn strip_banned_reactors(
    grouped: HashMap<String, Vec<ReactionSummary>>,
    banned: &HashSet<String>,
) -> HashMap<String, Vec<ReactionSummary>> {
    if banned.is_empty() {
        return grouped;
    }
    grouped
        .into_iter()
        .map(|(rkey, summaries)| {
            let filtered = summaries
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
                .collect();
            (rkey, filtered)
        })
        .collect()
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
            let parent = stored.parent.as_ref().and_then(|rkey| {
                let pr = page.parent_records.get(rkey)?;
                let ps = serde_json::from_value::<StoredMessage>(pr.data.clone()).ok()?;
                let parent_reactions = page.reactions.get(rkey).cloned().unwrap_or_default();
                let parent_author = build_author(
                    pr.did.clone(),
                    page.author_handles.get(&pr.did).cloned(),
                    page.author_profiles.get(&pr.did).cloned(),
                    page.author_actor_data.get(&pr.did).cloned(),
                    page.author_states.get(&pr.did).cloned(),
                );
                Some(ParentMessage {
                    uri: format!("at://{}/{}/{}", pr.did, pr.nsid, pr.rkey),
                    text: ps.text,
                    facets: ps.facets.unwrap_or_default(),
                    channel: channel_uri.clone(),
                    community: page.community_uri.clone(),
                    author: parent_author,
                    attachments: ps.attachments.unwrap_or_default(),
                    reactions: parent_reactions,
                    created_at: ps
                        .created_at
                        .unwrap_or_else(|| pr.indexed_at.clone()),
                    edited: ps.edited,
                })
            });
            let author = build_author(
                record.did.clone(),
                page.author_handles.get(&record.did).cloned(),
                page.author_profiles.get(&record.did).cloned(),
                page.author_actor_data.get(&record.did).cloned(),
                page.author_states.get(&record.did).cloned(),
            );
            let reactions = page
                .reactions
                .get(&record.rkey)
                .cloned()
                .unwrap_or_default();
            build_message(
                record,
                &channel_uri,
                &page.community_uri,
                author,
                parent,
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
            indexed_at: String::from("2026-05-26T00:00:00.000Z"),
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
                        parent_records: HashMap::from([(
                            String::from("msg-1"),
                            message_record("msg-1", "did:plc:alice", "hello", None),
                        )]),
                        reactions: HashMap::from([(
                            String::from("msg-1"),
                            vec![ReactionSummary {
                                emoji: String::from("🦜"),
                                count: 1,
                                reactor_dids: vec![String::from("did:plc:carol")],
                            }],
                        )]),
                        author_profiles: HashMap::new(),
                        author_actor_data: HashMap::new(),
                        author_states: HashMap::new(),
                        author_handles: HashMap::from([
                            (String::from("did:plc:bob"), String::from("bob.test")),
                            (String::from("did:plc:alice"), String::from("alice.test")),
                        ]),
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
        let parent = result.messages[0].parent.as_ref().unwrap();
        assert_eq!(
            parent.uri,
            "at://did:plc:alice/social.colibri.message/msg-1"
        );
        assert_eq!(parent.text, "hello");
        assert_eq!(parent.author.did, "did:plc:alice");
        assert_eq!(parent.author.handle, "alice.test");
        assert_eq!(parent.reactions.len(), 1);
        assert_eq!(parent.reactions[0].emoji, "🦜");
        assert_eq!(result.messages[0].author.did, "did:plc:bob");
        assert_eq!(result.messages[0].author.handle, "bob.test");
        assert!(result.messages[0].reactions.is_empty());
        assert_eq!(result.messages[0].created_at, "2026-05-13T00:00:00Z");
        assert_eq!(result.messages[1].reactions.len(), 1);
        assert_eq!(result.messages[1].reactions[0].emoji, "🦜");
        let parent = result.messages[0].parent.as_ref().unwrap();
        assert_eq!(parent.created_at, "2026-05-13T00:00:00Z");

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
                        parent_records: HashMap::new(),
                        reactions: HashMap::new(),
                        author_profiles: HashMap::new(),
                        author_actor_data: HashMap::new(),
                        author_states: HashMap::new(),
                        author_handles: HashMap::new(),
                    })
                })
            },
        )
        .await
        .unwrap();

        assert!(result.cursor.is_none());
    }

    #[test]
    fn strip_banned_reactors_drops_banned_dids_per_message() {
        let banned: HashSet<String> = ["did:plc:alice".to_string()].into_iter().collect();
        let mut grouped: HashMap<String, Vec<ReactionSummary>> = HashMap::new();
        grouped.insert(
            String::from("msg-1"),
            vec![
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
            ],
        );

        let filtered = strip_banned_reactors(grouped, &banned);
        let summaries = filtered.get("msg-1").unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].emoji, "🦜");
        assert_eq!(summaries[0].count, 1);
        assert_eq!(summaries[0].reactor_dids, vec![String::from("did:plc:bob")]);
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
