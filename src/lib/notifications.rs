use std::collections::{HashMap, HashSet};

use sea_orm::{
    ActiveValue, ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, sea_query,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriMessage;
use crate::lib::time::current_iso8601_utc;
use crate::models::{notifications, record_data};

pub const KIND_MENTION: &str = "mention";
pub const KIND_REPLY: &str = "reply";
pub const FACET_MENTION_TYPE: &str = "social.colibri.richtext.facet#mention";

/// Bundle carried over the WS-broadcast channel so each subscriber can render
/// a notification without having to fetch the message body separately.
#[derive(Debug, Clone)]
pub struct IndexedNotification {
    pub row: notifications::Model,
    pub message: NotificationMessage,
}

/// Inline message body rendered alongside a notification so clients can show
/// the contents without a follow-up fetch. Mirrors the subset of `ColibriMessage`
/// useful for notification previews.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NotificationMessage {
    pub text: String,
    #[serde(default)]
    pub facets: Vec<Value>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited: Option<bool>,
}

impl NotificationMessage {
    pub fn from_colibri_message(message: ColibriMessage) -> Self {
        Self {
            text: message.text,
            facets: message.facets.unwrap_or_default(),
            created_at: message.created_at,
            parent: message.parent,
            attachments: message.attachments.unwrap_or_default(),
            edited: message.edited,
        }
    }
}

/// One notification awaiting delivery, in the shape we send to clients.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NotificationView {
    pub id: i64,
    #[serde(rename = "recipientDid")]
    pub recipient_did: String,
    pub kind: String,
    #[serde(rename = "messageUri")]
    pub message_uri: String,
    #[serde(rename = "authorDid")]
    pub author_did: String,
    #[serde(rename = "channelUri")]
    pub channel_uri: String,
    #[serde(rename = "indexedAt")]
    pub indexed_at: String,
    #[serde(rename = "seenAt", skip_serializing_if = "Option::is_none")]
    pub seen_at: Option<String>,
    /// Inline message body. `None` if the underlying message has been deleted
    /// from the AppView cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<NotificationMessage>,
}

impl NotificationView {
    pub fn from_row(row: notifications::Model, message: Option<NotificationMessage>) -> Self {
        Self {
            id: row.id,
            recipient_did: row.recipient_did,
            kind: row.kind,
            message_uri: row.message_uri,
            author_did: row.author_did,
            channel_uri: row.channel_uri,
            indexed_at: row.indexed_at,
            seen_at: row.seen_at,
            message,
        }
    }
}

/// Extracts every mentioned DID from the facets of a message. Returns DIDs in
/// the order they first appear, deduplicated.
pub fn extract_mentioned_dids(facets: &[Value]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for facet in facets {
        let Some(features) = facet.get("features").and_then(|f| f.as_array()) else {
            continue;
        };
        for feature in features {
            let Some(kind) = feature.get("$type").and_then(|t| t.as_str()) else {
                continue;
            };
            if kind != FACET_MENTION_TYPE {
                continue;
            }
            let Some(did) = feature.get("did").and_then(|d| d.as_str()) else {
                continue;
            };
            if seen.insert(did.to_string()) {
                out.push(did.to_string());
            }
        }
    }
    out
}

/// Looks up the author DID of a parent message inside the same channel.
/// Returns `None` if the parent is not in the cache.
///
/// `channel` may be a full AT-URI (new messages) or a bare rkey (legacy).
/// Both formats are matched so reply notifications work across the transition.
pub async fn fetch_parent_author(
    db: &DatabaseConnection,
    channel: &str,
    parent_rkey: &str,
) -> Result<Option<String>, DbErr> {
    let channel_rkey = AtUri::parse(channel)
        .map(|u| u.rkey)
        .unwrap_or_else(|| channel.to_string());

    let record = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.message"))
        .filter(record_data::Column::Rkey.eq(parent_rkey))
        .filter(sea_orm::prelude::Expr::cust_with_values(
            r#"("record_data"."data"->>'channel' = $1 OR "record_data"."data"->>'channel' = $2)"#,
            vec![
                sea_orm::Value::from(channel.to_string()),
                sea_orm::Value::from(channel_rkey),
            ],
        ))
        .one(db)
        .await?;

    Ok(record.map(|r| r.did))
}

/// Inserts notification rows for a freshly indexed message. Mentions of the
/// author themself are skipped, as are duplicate insertions (the table has a
/// unique index on `(recipient_did, message_uri, kind)`).
pub async fn index_message_notifications(
    db: &DatabaseConnection,
    author_did: &str,
    message_uri: &str,
    message: &ColibriMessage,
) -> Result<Vec<IndexedNotification>, DbErr> {
    let mut recipients: Vec<(String, &'static str)> = Vec::new();
    let mut seen: HashSet<(String, &'static str)> = HashSet::new();

    if let Some(facets) = message.facets.as_ref() {
        for did in extract_mentioned_dids(facets) {
            if did == author_did {
                continue;
            }
            let key = (did.clone(), KIND_MENTION);
            if seen.insert(key.clone()) {
                recipients.push(key);
            }
        }
    }

    if let Some(parent_rkey) = message.parent.as_ref()
        && let Some(parent_author) = fetch_parent_author(db, &message.channel, parent_rkey).await?
        && parent_author != author_did
    {
        let key = (parent_author, KIND_REPLY);
        if seen.insert(key.clone()) {
            recipients.push(key);
        }
    }

    if recipients.is_empty() {
        return Ok(Vec::new());
    }

    let now = current_iso8601_utc();
    let mut inserted = Vec::new();
    for (recipient_did, kind) in recipients {
        let row = notifications::ActiveModel {
            recipient_did: ActiveValue::Set(recipient_did.clone()),
            kind: ActiveValue::Set(kind.to_string()),
            message_uri: ActiveValue::Set(message_uri.to_string()),
            author_did: ActiveValue::Set(author_did.to_string()),
            channel_uri: ActiveValue::Set(message.channel.clone()),
            indexed_at: ActiveValue::Set(now.clone()),
            seen_at: ActiveValue::Set(None),
            ..Default::default()
        };

        let result = notifications::Entity::insert(row)
            .on_conflict(
                sea_query::OnConflict::columns([
                    notifications::Column::RecipientDid,
                    notifications::Column::MessageUri,
                    notifications::Column::Kind,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec(db)
            .await;

        // Conflict (= already indexed) is a no-op; surface other failures.
        match result {
            Ok(_) => {}
            Err(DbErr::RecordNotInserted) => continue,
            Err(e) => return Err(e),
        }

        if let Some(row) = notifications::Entity::find()
            .filter(notifications::Column::RecipientDid.eq(&recipient_did))
            .filter(notifications::Column::MessageUri.eq(message_uri))
            .filter(notifications::Column::Kind.eq(kind))
            .one(db)
            .await?
        {
            inserted.push(IndexedNotification {
                row,
                message: NotificationMessage::from_colibri_message(message.clone()),
            });
        }
    }
    Ok(inserted)
}

/// Lists notifications for a recipient, newest first. The optional cursor is
/// the stringified id of the oldest notification on the previous page; rows
/// with `id < cursor` are returned.
pub async fn list_notifications(
    db: &DatabaseConnection,
    recipient_did: &str,
    limit: u64,
    cursor: Option<&str>,
) -> Result<Vec<notifications::Model>, DbErr> {
    let mut condition = Condition::all()
        .add(notifications::Column::RecipientDid.eq(recipient_did))
        .add(sea_orm::prelude::Expr::cust(
            r#""notifications"."channel_uri" LIKE 'at://%'"#,
        ));

    if let Some(c) = cursor {
        let parsed: i64 = c
            .parse()
            .map_err(|_| DbErr::Custom(format!("invalid cursor: {c}")))?;
        condition = condition.add(notifications::Column::Id.lt(parsed));
    }

    notifications::Entity::find()
        .filter(condition)
        .order_by_desc(notifications::Column::Id)
        .limit(limit)
        .all(db)
        .await
}

/// Fetches the cached `social.colibri.message` records referenced by the given
/// AT-URIs and returns them keyed by URI. URIs that don't resolve to a
/// matching record (deleted, never indexed, malformed URI) are simply absent
/// from the result.
pub async fn hydrate_messages(
    db: &DatabaseConnection,
    message_uris: &[String],
) -> Result<HashMap<String, NotificationMessage>, DbErr> {
    if message_uris.is_empty() {
        return Ok(HashMap::new());
    }

    let mut by_uri: HashMap<String, (String, String)> = HashMap::new();
    let mut condition = Condition::any();
    for uri in message_uris {
        let Some(parsed) = AtUri::parse(uri) else {
            continue;
        };
        if parsed.collection != "social.colibri.message" {
            continue;
        }
        let did = parsed.authority.clone();
        let rkey = parsed.rkey.clone();
        by_uri.insert(uri.clone(), (did.clone(), rkey.clone()));
        condition = condition.add(
            Condition::all()
                .add(record_data::Column::Did.eq(did))
                .add(record_data::Column::Rkey.eq(rkey)),
        );
    }

    if by_uri.is_empty() {
        return Ok(HashMap::new());
    }

    let records = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.message"))
        .filter(condition)
        .all(db)
        .await?;

    let mut by_key: HashMap<(String, String), NotificationMessage> = HashMap::new();
    for record in records {
        if let Ok(message) = serde_json::from_value::<ColibriMessage>(record.data) {
            by_key.insert(
                (record.did, record.rkey),
                NotificationMessage::from_colibri_message(message),
            );
        }
    }

    let mut out = HashMap::new();
    for (uri, key) in by_uri {
        if let Some(msg) = by_key.remove(&key) {
            out.insert(uri, msg);
        }
    }
    Ok(out)
}

/// Counts unseen notifications for a recipient.
pub async fn unseen_count(db: &DatabaseConnection, recipient_did: &str) -> Result<u64, DbErr> {
    notifications::Entity::find()
        .filter(notifications::Column::RecipientDid.eq(recipient_did))
        .filter(notifications::Column::SeenAt.is_null())
        .filter(sea_orm::prelude::Expr::cust(
            r#""notifications"."channel_uri" LIKE 'at://%'"#,
        ))
        .count(db)
        .await
}

/// Marks every unseen notification for the recipient with `indexed_at <= now`
/// as seen at `seen_at`. Returns the number of rows updated.
pub async fn mark_seen_up_to(
    db: &DatabaseConnection,
    recipient_did: &str,
    seen_at: &str,
    cutoff_indexed_at: &str,
) -> Result<u64, DbErr> {
    let res = notifications::Entity::update_many()
        .col_expr(
            notifications::Column::SeenAt,
            sea_query::Expr::value(seen_at),
        )
        .filter(notifications::Column::RecipientDid.eq(recipient_did))
        .filter(notifications::Column::SeenAt.is_null())
        .filter(notifications::Column::IndexedAt.lte(cutoff_indexed_at))
        .exec(db)
        .await?;
    Ok(res.rows_affected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message_with_facets(facets: Value, parent: Option<&str>) -> ColibriMessage {
        ColibriMessage {
            r#type: "social.colibri.message".to_string(),
            text: "hello".to_string(),
            facets: Some(facets.as_array().cloned().unwrap_or_default()),
            created_at: "2026-05-14T00:00:00Z".to_string(),
            channel: "chan-a".to_string(),
            edited: None,
            parent: parent.map(String::from),
            attachments: None,
        }
    }

    #[test]
    fn extract_mentioned_dids_pulls_did_from_mention_features() {
        let facets = json!([
            {
                "index": {"byteStart": 0, "byteEnd": 5},
                "features": [
                    {"$type": "social.colibri.richtext.facet#mention", "did": "did:plc:alice"}
                ]
            },
            {
                "index": {"byteStart": 10, "byteEnd": 13},
                "features": [
                    {"$type": "social.colibri.richtext.facet#bold"}
                ]
            },
            {
                "index": {"byteStart": 20, "byteEnd": 25},
                "features": [
                    {"$type": "social.colibri.richtext.facet#mention", "did": "did:plc:bob"},
                    {"$type": "social.colibri.richtext.facet#mention", "did": "did:plc:alice"}
                ]
            }
        ]);
        let dids = extract_mentioned_dids(facets.as_array().unwrap());
        assert_eq!(
            dids,
            vec![String::from("did:plc:alice"), String::from("did:plc:bob")]
        );
    }

    #[test]
    fn extract_mentioned_dids_ignores_non_mention_features_and_missing_dids() {
        let facets = json!([
            {
                "features": [
                    {"$type": "social.colibri.richtext.facet#link", "uri": "https://example"}
                ]
            },
            {
                "features": [
                    {"$type": "social.colibri.richtext.facet#mention"}
                ]
            }
        ]);
        assert!(extract_mentioned_dids(facets.as_array().unwrap()).is_empty());
    }

    #[test]
    fn message_with_facets_is_constructible() {
        // Just guarantees that ColibriMessage stays compatible with this helper.
        let msg = message_with_facets(json!([]), Some("parent"));
        assert_eq!(msg.channel, "chan-a");
        assert_eq!(msg.parent.as_deref(), Some("parent"));
    }
}
