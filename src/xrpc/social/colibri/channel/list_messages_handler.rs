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
use crate::lib::colibri::{ColibriActorData, ColibriActorProfile, resolve_effective_profile};
use crate::lib::moderation::community_moderation_state;
use crate::lib::reactions::{ReactionSummary, group_reactions_for_messages};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::{record_data, repos, user_states};
use crate::xrpc::social::colibri::actor::get_data_handler::{
    ActorData, ActorStatus, actor_data_from_effective,
};

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
    /// REMOVABLE MIGRATION SCAFFOLDING: set on a channel cloned from a legacy
    /// community. When present, messages that targeted the legacy channel are
    /// surfaced here too (see `channel_match_condition`).
    #[serde(rename = "migratedFrom", default)]
    migrated_from: Option<String>,
}

/// Builds the "this message targets this channel" predicate. A message stores
/// its channel as either the full channel AT-URI (new) or a bare rkey (legacy),
/// so both are matched. REMOVABLE MIGRATION SCAFFOLDING: when the channel was
/// cloned from a legacy channel (`legacy` = its `(uri, rkey)`), old messages
/// that still point at the legacy channel are matched as well, so history
/// surfaces in the migrated channel without rewriting any PDS records.
fn channel_match_condition(
    channel_uri: &str,
    channel_rkey: &str,
    legacy: Option<(&str, &str)>,
) -> Condition {
    let mut targets = vec![channel_uri.to_string(), channel_rkey.to_string()];
    if let Some((uri, rkey)) = legacy {
        targets.push(uri.to_string());
        targets.push(rkey.to_string());
    }
    let mut any = Condition::any();
    for target in targets {
        any = any.add(Expr::cust_with_values(
            r#""record_data"."data"->>'channel' = $1"#,
            vec![sea_orm::Value::from(target)],
        ));
    }
    any
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

/// Fetches a page of message records targeting the given channel, ordered by
/// rkey descending. Matches messages that store either the full channel AT-URI
/// (new format) or just the rkey (legacy format) in their `channel` field.
/// The optional cursor filters out rkeys at or past the cursor, matching the
/// listRecords convention.
pub async fn fetch_message_page(
    db: &DatabaseConnection,
    channel_uri: &str,
    channel_rkey: &str,
    legacy_channel: Option<(&str, &str)>,
    limit: u64,
    cursor: Option<&str>,
) -> Result<Vec<record_data::Model>, DbErr> {
    let mut condition = Condition::all()
        .add(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .add(channel_match_condition(
            channel_uri,
            channel_rkey,
            legacy_channel,
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

/// Fetches full records for parent messages identified by full AT-URIs.
/// Returns the map keyed by canonical AT-URI `"at://{did}/{nsid}/{rkey}"`.
pub async fn fetch_parent_records(
    db: &DatabaseConnection,
    parent_uris: &[String],
) -> Result<HashMap<String, record_data::Model>, DbErr> {
    if parent_uris.is_empty() {
        return Ok(HashMap::new());
    }

    let mut pair_condition = Condition::any();
    for uri in parent_uris {
        let Some(parsed) = AtUri::parse(uri) else {
            continue;
        };
        pair_condition = pair_condition.add(
            Condition::all()
                .add(record_data::Column::Did.eq(parsed.authority))
                .add(record_data::Column::Rkey.eq(parsed.rkey)),
        );
    }

    let records = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .filter(pair_condition)
        .all(db)
        .await?;

    Ok(records
        .into_iter()
        .map(|r| (format!("at://{}/{}/{}", r.did, r.nsid, r.rkey), r))
        .collect())
}

/// Fetches full records for legacy parent messages identified by bare rkeys
/// within a channel. Returns the map keyed by bare rkey.
/// Matches messages storing either the full channel AT-URI or bare rkey.
async fn fetch_legacy_parent_records(
    db: &DatabaseConnection,
    channel_uri: &str,
    channel_rkey: &str,
    legacy_channel: Option<(&str, &str)>,
    parent_rkeys: &[String],
) -> Result<HashMap<String, record_data::Model>, DbErr> {
    if parent_rkeys.is_empty() {
        return Ok(HashMap::new());
    }
    let parents = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .filter(record_data::Column::Rkey.is_in(parent_rkeys.to_vec()))
        .filter(channel_match_condition(
            channel_uri,
            channel_rkey,
            legacy_channel,
        ))
        .all(db)
        .await?;

    Ok(parents.into_iter().map(|p| (p.rkey.clone(), p)).collect())
}

/// Canonical AT-URI `"at://{did}/{nsid}/{rkey}"` for a stored record.
fn record_uri(record: &record_data::Model) -> String {
    format!("at://{}/{}/{}", record.did, record.nsid, record.rkey)
}

fn build_author(
    did: String,
    handle: Option<String>,
    profile: Option<ActorProfile>,
    colibri_profile: Option<ColibriActorProfile>,
    actor_data: Option<ColibriActorData>,
    state: Option<String>,
) -> MessageAuthor {
    let handle = handle.unwrap_or_else(|| did.clone());

    // `is_bot` always comes from the Bluesky profile; the served profile fields
    // come from the effective profile so a non-synced Colibri author shows their
    // Colibri identity here, matching `getData` and the profile popover.
    let is_bot = profile.as_ref().is_some_and(ActorProfile::is_bot);
    let effective = resolve_effective_profile(colibri_profile.as_ref(), profile.as_ref());

    let status = actor_data
        .map(|d| ActorStatus {
            text: d.status.unwrap_or(String::from("")),
            emoji: d.emoji,
        })
        .unwrap_or(ActorStatus {
            text: String::new(),
            emoji: None,
        });
    let online_state = state.unwrap_or_else(|| String::from("offline"));
    MessageAuthor {
        did,
        data: actor_data_from_effective(effective, is_bot, &handle, online_state, status),
        handle,
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
    /// Keyed by canonical AT-URI for new-format parents, or by bare rkey for
    /// legacy parents. Both are valid lookup keys via `stored.parent`.
    pub parent_records: HashMap<String, record_data::Model>,
    pub reactions: HashMap<String, Vec<ReactionSummary>>,
    pub author_profiles: HashMap<String, ActorProfile>,
    /// Raw `social.colibri.actor.profile` records, keyed by author DID. Resolved
    /// against `author_profiles` per the `syncBluesky` rule when building authors.
    pub author_colibri_profiles: HashMap<String, ColibriActorProfile>,
    pub author_actor_data: HashMap<String, ColibriActorData>,
    pub author_states: HashMap<String, String>,
    pub author_handles: HashMap<String, String>,
}

pub async fn assemble_message_page(
    db: &DatabaseConnection,
    channel: &AtUri,
    limit: u64,
    cursor: Option<&str>,
    include_hidden: bool,
) -> Result<MessagePage, DbErr> {
    let stored_channel = fetch_channel_record(db, &channel.authority, &channel.rkey)
        .await?
        .and_then(|r| serde_json::from_value::<StoredChannel>(r.data).ok());
    let community_uri = stored_channel
        .as_ref()
        .map(|c| {
            format!(
                "at://{}/{}/{}",
                channel.authority, COMMUNITY_NSID, c.community
            )
        })
        .unwrap_or_default();

    // REMOVABLE MIGRATION SCAFFOLDING: if this channel was cloned from a legacy
    // channel, resolve the legacy channel's (uri, rkey) so old messages that
    // still target it are folded into this channel's history.
    let legacy_channel = stored_channel
        .as_ref()
        .and_then(|c| c.migrated_from.as_deref())
        .and_then(|uri| AtUri::parse(uri).map(|p| (uri.to_string(), p.rkey)));
    let legacy_channel_ref = legacy_channel
        .as_ref()
        .map(|(uri, rkey)| (uri.as_str(), rkey.as_str()));

    // Single scan of the moderation log yields both banned authors and hidden
    // message URIs. Banned authors are always filtered; hidden messages are
    // filtered unless the caller explicitly asks to include them.
    let moderation = community_moderation_state(db, &channel.authority).await?;
    let banned = &moderation.banned_dids;
    let hidden = &moderation.hidden_message_uris;
    let is_hidden = |r: &record_data::Model| !include_hidden && hidden.contains(&record_uri(r));

    let channel_uri = format!(
        "at://{}/{}/{}",
        channel.authority, channel.collection, channel.rkey
    );
    let raw_records = fetch_message_page(
        db,
        &channel_uri,
        &channel.rkey,
        legacy_channel_ref,
        limit,
        cursor,
    )
    .await?;
    let records: Vec<record_data::Model> = raw_records
        .into_iter()
        .filter(|r| !banned.contains(&r.did) && !is_hidden(r))
        .collect();

    // Split parent values: full AT-URIs (new) vs bare rkeys (legacy).
    let mut parent_uris: Vec<String> = Vec::new();
    let mut legacy_parent_rkeys: Vec<String> = Vec::new();
    for r in &records {
        if let Ok(m) = serde_json::from_value::<StoredMessage>(r.data.clone())
            && let Some(p) = m.parent
        {
            if p.starts_with("at://") {
                parent_uris.push(p);
            } else {
                legacy_parent_rkeys.push(p);
            }
        }
    }

    let mut parent_records: HashMap<String, record_data::Model> =
        fetch_parent_records(db, &parent_uris)
            .await?
            .into_iter()
            .filter(|(_, r)| !banned.contains(&r.did) && !is_hidden(r))
            .collect();

    let legacy_records = fetch_legacy_parent_records(
        db,
        &channel_uri,
        &channel.rkey,
        legacy_channel_ref,
        &legacy_parent_rkeys,
    )
    .await?
    .into_iter()
    .filter(|(_, r)| !banned.contains(&r.did) && !is_hidden(r));
    parent_records.extend(legacy_records);

    // Fetch reactions for page messages and their parents in one round-trip.
    let all_rkeys: Vec<String> = records
        .iter()
        .map(|r| r.rkey.clone())
        .chain(parent_records.values().map(|r| r.rkey.clone()))
        .collect();
    let reactions =
        strip_banned_reactors(group_reactions_for_messages(db, &all_rkeys).await?, banned);

    // Collect unique author DIDs from page messages and their parents.
    let mut author_dids: Vec<String> = records
        .iter()
        .map(|r| r.did.clone())
        .chain(parent_records.values().map(|r| r.did.clone()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    author_dids.sort();

    let (
        author_profiles,
        author_colibri_profiles,
        author_actor_data,
        author_states,
        author_handles,
    ) = if author_dids.is_empty() {
        (
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    } else {
        let self_records = record_data::Entity::find()
            .filter(record_data::Column::Did.is_in(author_dids.clone()))
            .filter(record_data::Column::Rkey.eq("self"))
            .filter(
                Condition::any()
                    .add(record_data::Column::Nsid.eq("app.bsky.actor.profile"))
                    .add(record_data::Column::Nsid.eq("social.colibri.actor.profile"))
                    .add(record_data::Column::Nsid.eq("social.colibri.actor.data")),
            )
            .all(db)
            .await?;

        let mut profiles: HashMap<String, ActorProfile> = HashMap::new();
        let mut colibri_profiles: HashMap<String, ColibriActorProfile> = HashMap::new();
        let mut actor_data: HashMap<String, ColibriActorData> = HashMap::new();
        for rec in self_records {
            if rec.nsid == "app.bsky.actor.profile" {
                if let Ok(p) = serde_json::from_value::<ActorProfile>(rec.data) {
                    profiles.insert(rec.did, p);
                }
            } else if rec.nsid == "social.colibri.actor.profile" {
                if let Ok(p) = serde_json::from_value::<ColibriActorProfile>(rec.data) {
                    colibri_profiles.insert(rec.did, p);
                }
            } else if rec.nsid == "social.colibri.actor.data"
                && let Ok(d) = serde_json::from_value::<ColibriActorData>(rec.data)
            {
                actor_data.insert(rec.did, d);
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

        (profiles, colibri_profiles, actor_data, states, handles)
    };

    Ok(MessagePage {
        records,
        community_uri,
        parent_records,
        reactions,
        author_profiles,
        author_colibri_profiles,
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
        bool,
    ) -> BoxFuture<'static, Result<MessagePage, DbErr>>
    + Send
    + Sync;

async fn list_messages_with(
    channel_uri: String,
    limit: Option<u64>,
    cursor: Option<String>,
    include_hidden: bool,
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
    let page = assemble_fn(db, channel, effective_limit, cursor, include_hidden).await?;
    Ok(Json(message_list_from_page(
        page,
        &channel_uri,
        effective_limit,
    )))
}

pub fn message_list_from_page(
    page: MessagePage,
    channel_uri: &str,
    effective_limit: u64,
) -> MessageList {
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
            let parent = stored.parent.as_ref().and_then(|parent_uri| {
                let pr = page.parent_records.get(parent_uri)?;
                let ps = serde_json::from_value::<StoredMessage>(pr.data.clone()).ok()?;
                let parent_reactions = page.reactions.get(&pr.rkey).cloned().unwrap_or_default();
                let parent_author = build_author(
                    pr.did.clone(),
                    page.author_handles.get(&pr.did).cloned(),
                    page.author_profiles.get(&pr.did).cloned(),
                    page.author_colibri_profiles.get(&pr.did).cloned(),
                    page.author_actor_data.get(&pr.did).cloned(),
                    page.author_states.get(&pr.did).cloned(),
                );
                Some(ParentMessage {
                    uri: format!("at://{}/{}/{}", pr.did, pr.nsid, pr.rkey),
                    text: ps.text,
                    facets: ps.facets.unwrap_or_default(),
                    channel: channel_uri.to_string(),
                    community: page.community_uri.clone(),
                    author: parent_author,
                    attachments: ps.attachments.unwrap_or_default(),
                    reactions: parent_reactions,
                    created_at: ps.created_at.unwrap_or_else(|| pr.indexed_at.clone()),
                    edited: ps.edited,
                })
            });
            let author = build_author(
                record.did.clone(),
                page.author_handles.get(&record.did).cloned(),
                page.author_profiles.get(&record.did).cloned(),
                page.author_colibri_profiles.get(&record.did).cloned(),
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
                channel_uri,
                &page.community_uri,
                author,
                parent,
                reactions,
            )
        })
        .collect();

    MessageList {
        cursor: next_cursor,
        messages,
    }
}

pub async fn build_message_list(
    db: &DatabaseConnection,
    channel_uri: &str,
    limit: Option<u64>,
    cursor: Option<&str>,
    include_hidden: bool,
) -> Result<MessageList, ErrorResponse> {
    let channel = AtUri::parse(channel_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid channel AT-URI."),
        }),
    })?;
    let effective_limit = limit.unwrap_or(DEFAULT_LIMIT);
    let page = assemble_message_page(db, &channel, effective_limit, cursor, include_hidden).await?;
    Ok(message_list_from_page(page, channel_uri, effective_limit))
}

fn assemble_message_page_boxed(
    db: DatabaseConnection,
    channel: AtUri,
    limit: u64,
    cursor: Option<String>,
    include_hidden: bool,
) -> BoxFuture<'static, Result<MessagePage, DbErr>> {
    Box::pin(async move {
        assemble_message_page(&db, &channel, limit, cursor.as_deref(), include_hidden).await
    })
}

#[get("/xrpc/social.colibri.channel.listMessages?<channel>&<limit>&<cursor>&<all>")]
/// Returns a paginated list of messages for a channel, newest first.
///
/// Messages hidden by a `hideMessage` moderation action are filtered out by
/// default. Passing `all=true` includes them (e.g. for moderator views).
pub async fn list_messages(
    channel: &str,
    limit: Option<u64>,
    cursor: Option<&str>,
    all: Option<bool>,
    db: &State<DatabaseConnection>,
) -> Result<Json<MessageList>, ErrorResponse> {
    list_messages_with(
        channel.to_string(),
        limit,
        cursor.map(|c| c.to_string()),
        all.unwrap_or(false),
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
            false,
            db,
            &|_, _, _, _, _| {
                Box::pin(async {
                    Ok(MessagePage {
                        records: vec![
                            message_record(
                                "msg-2",
                                "did:plc:bob",
                                "hi",
                                Some("at://did:plc:alice/social.colibri.message/msg-1"),
                            ),
                            message_record("msg-1", "did:plc:alice", "hello", None),
                        ],
                        community_uri: String::from(
                            "at://did:plc:owner/social.colibri.community/c1",
                        ),
                        parent_records: HashMap::from([(
                            String::from("at://did:plc:alice/social.colibri.message/msg-1"),
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
                        author_colibri_profiles: HashMap::new(),
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
            false,
            db,
            &|_, _, _, _, _| {
                Box::pin(async {
                    Ok(MessagePage {
                        records: vec![message_record("msg-1", "did:plc:alice", "hello", None)],
                        community_uri: String::from(
                            "at://did:plc:owner/social.colibri.community/c1",
                        ),
                        parent_records: HashMap::new(),
                        reactions: HashMap::new(),
                        author_profiles: HashMap::new(),
                        author_colibri_profiles: HashMap::new(),
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

    fn bsky_profile(display_name: &str) -> ActorProfile {
        serde_json::from_value(serde_json::json!({ "displayName": display_name })).unwrap()
    }

    // These three mirror the `get_data_handler` cases so message-author chips
    // resolve the same effective profile as the popover.

    #[test]
    fn build_author_uses_non_synced_colibri_profile() {
        let colibri = ColibriActorProfile {
            display_name: Some(String::from("Colibri Bob")),
            sync_bluesky: false,
            theme: Some(crate::lib::colibri::ColibriProfileTheme {
                accent_color: Some(String::from("#ff0000")),
                ..Default::default()
            }),
            ..Default::default()
        };
        let author = build_author(
            String::from("did:plc:bob"),
            Some(String::from("bob.test")),
            Some(bsky_profile("Bsky Bob")),
            Some(colibri),
            None,
            None,
        );
        assert_eq!(author.data.display_name, "Colibri Bob");
        assert!(!author.data.sync_bluesky);
        assert_eq!(
            author.data.theme.unwrap().accent_color.as_deref(),
            Some("#ff0000")
        );
    }

    #[test]
    fn build_author_synced_uses_bsky_fields_but_colibri_theme() {
        let colibri = ColibriActorProfile {
            sync_bluesky: true,
            theme: Some(crate::lib::colibri::ColibriProfileTheme {
                banner_color: Some(String::from("#00ff00")),
                ..Default::default()
            }),
            ..Default::default()
        };
        let author = build_author(
            String::from("did:plc:bob"),
            Some(String::from("bob.test")),
            Some(bsky_profile("Bsky Bob")),
            Some(colibri),
            None,
            None,
        );
        assert_eq!(author.data.display_name, "Bsky Bob");
        assert!(author.data.sync_bluesky);
        assert_eq!(
            author.data.theme.unwrap().banner_color.as_deref(),
            Some("#00ff00")
        );
    }

    #[test]
    fn build_author_un_onboarded_falls_back_to_bsky() {
        let author = build_author(
            String::from("did:plc:bob"),
            Some(String::from("bob.test")),
            Some(bsky_profile("Bsky Bob")),
            None,
            None,
            None,
        );
        assert_eq!(author.data.display_name, "Bsky Bob");
        assert!(!author.data.sync_bluesky);
        assert!(author.data.theme.is_none());
    }

    #[tokio::test]
    async fn rejects_invalid_channel_uri() {
        let db = mock_db();
        let result = list_messages_with(
            String::from("not-a-uri"),
            None,
            None,
            false,
            db,
            &|_, _, _, _, _| Box::pin(async { panic!("should not assemble when uri is invalid") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
