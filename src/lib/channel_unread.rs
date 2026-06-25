//! Per-channel unread computation for the Discord-style unread model.
//!
//! For each channel in a community we report two independent signals to the
//! client:
//!
//! - `has_unread_messages` (white dot): the channel's newest message is newer
//!   than the user's last-read cursor. When the user has no cursor yet, we fall
//!   back to their **join time** — messages posted before they joined never
//!   count as unread (otherwise a fresh join would light up every channel with
//!   history). With neither a cursor nor a known join boundary, any message
//!   counts as unread.
//! - `unread_ping_count` (red badge): how many notifications (mentions/replies)
//!   for the user in this channel are still unseen.
//!
//! Message and read-cursor records are keyed by TID rkeys, which sort
//! lexicographically by creation time, so an rkey comparison is a time
//! comparison. The read cursor stores the AT-URI of the last-read message; its
//! rkey is what we compare against the channel's newest message rkey. The join
//! boundary is instead compared against the newest message's `indexed_at`: both
//! are AppView ingestion stamps minted by `current_iso8601_utc()` in one fixed
//! `YYYY-MM-DDTHH:MM:SS.mmmZ` form, so a plain string comparison is a
//! chronological one.

use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::notifications;
use crate::models::record_data;

const CHANNEL_NSID: &str = "social.colibri.channel";
const MESSAGE_NSID: &str = "social.colibri.message";
const READ_NSID: &str = "social.colibri.channel.read";
const MEMBER_NSID: &str = "social.colibri.member";
const COMMUNITY_NSID: &str = "social.colibri.community";

/// Unread state for a single channel, in the shape the client consumes.
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct ChannelUnreadStatus {
    #[serde(rename = "channelUri")]
    pub channel_uri: String,
    #[serde(rename = "hasUnreadMessages")]
    pub has_unread_messages: bool,
    #[serde(rename = "unreadPingCount")]
    pub unread_ping_count: u64,
}

#[derive(Deserialize)]
struct StoredCursor {
    cursor: String,
}

/// The newest message in a channel as `(rkey, indexed_at)`, or `None` if the
/// channel has no messages. Matches messages that store either the full channel
/// AT-URI (new format) or the bare channel rkey (legacy format), mirroring
/// `list_messages_handler::fetch_message_page`.
async fn latest_message(
    db: &DatabaseConnection,
    channel_uri: &str,
    channel_rkey: &str,
) -> Result<Option<(String, String)>, DbErr> {
    let record = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
        .filter(Expr::cust_with_values(
            r#"("record_data"."data"->>'channel' = $1 OR "record_data"."data"->>'channel' = $2)"#,
            vec![
                sea_orm::Value::from(channel_uri.to_string()),
                sea_orm::Value::from(channel_rkey.to_string()),
            ],
        ))
        .order_by_desc(record_data::Column::Rkey)
        .limit(1)
        .one(db)
        .await?;
    Ok(record.map(|r| (r.rkey, r.indexed_at)))
}

/// The caller's "join boundary" for a community: the AppView-ingestion
/// timestamp (`indexed_at`) at or before which messages should not count as
/// unread for a user with no read cursor. It is the caller's
/// `social.colibri.member` record's `indexed_at`, or — for the community owner,
/// who has no member record — the community record's `indexed_at`. `None` when
/// neither is cached (treated as "no boundary": any message counts as unread).
async fn join_boundary_indexed_at(
    db: &DatabaseConnection,
    caller_did: &str,
    community: &AtUri,
) -> Result<Option<String>, DbErr> {
    let member = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(caller_did.to_string())],
        ))
        .one(db)
        .await?;
    if let Some(record) = member {
        return Ok(Some(record.indexed_at));
    }

    // Owner (or otherwise no member record): fall back to the community record.
    let community_record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(COMMUNITY_NSID))
        .filter(record_data::Column::Rkey.eq("self"))
        .one(db)
        .await?;
    Ok(community_record.map(|r| r.indexed_at))
}

/// Decides whether a channel has unread messages for a user, given the newest
/// message `(rkey, indexed_at)`, the user's read-cursor rkey (if any), and the
/// user's join boundary `indexed_at` (if known).
///
/// - No messages → not unread.
/// - Has a cursor → newest rkey strictly after the cursor rkey (the join
///   boundary is irrelevant: you can't have read before joining).
/// - No cursor → unread only if the newest message was indexed after the join
///   boundary. With no boundary either, any message counts as unread.
fn channel_has_unread(
    latest: Option<(&str, &str)>,
    cursor_rkey: Option<&str>,
    join_boundary: Option<&str>,
) -> bool {
    match latest {
        None => false,
        Some((rkey, indexed_at)) => match cursor_rkey {
            Some(cursor) => rkey > cursor,
            None => match join_boundary {
                Some(boundary) => indexed_at > boundary,
                None => true,
            },
        },
    }
}

/// rkey of the last-read message for a user in a channel, from their latest
/// read-cursor record. `None` when no cursor exists or its stored message URI
/// is malformed (treated as "nothing read yet").
async fn read_cursor_rkey(
    db: &DatabaseConnection,
    did: &str,
    channel_uri: &str,
) -> Result<Option<String>, DbErr> {
    let record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(did))
        .filter(record_data::Column::Nsid.eq(READ_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'channel' = $1"#,
            vec![sea_orm::Value::from(channel_uri.to_string())],
        ))
        .order_by_desc(record_data::Column::Rkey)
        .limit(1)
        .one(db)
        .await?;

    let Some(record) = record else {
        return Ok(None);
    };
    let Ok(stored) = serde_json::from_value::<StoredCursor>(record.data) else {
        return Ok(None);
    };
    Ok(AtUri::parse(&stored.cursor).map(|u| u.rkey))
}

/// Computes unread status for every channel in a community for one user.
pub async fn community_channel_unread_status(
    db: &DatabaseConnection,
    caller_did: &str,
    community: &AtUri,
) -> Result<Vec<ChannelUnreadStatus>, DbErr> {
    let channel_records = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(CHANNEL_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(community.rkey.clone())],
        ))
        .all(db)
        .await?;

    // The join boundary is the same for every channel in the community, so
    // resolve it once up front.
    let join_boundary = join_boundary_indexed_at(db, caller_did, community).await?;

    let mut out = Vec::with_capacity(channel_records.len());
    for channel in channel_records {
        let channel_uri = format!("at://{}/{}/{}", channel.did, channel.nsid, channel.rkey);

        let latest = latest_message(db, &channel_uri, &channel.rkey).await?;
        let cursor_rkey = read_cursor_rkey(db, caller_did, &channel_uri).await?;
        let has_unread_messages = channel_has_unread(
            latest.as_ref().map(|(r, i)| (r.as_str(), i.as_str())),
            cursor_rkey.as_deref(),
            join_boundary.as_deref(),
        );

        let unread_ping_count =
            notifications::unseen_count_for_channel(db, caller_did, &channel_uri).await?;

        out.push(ChannelUnreadStatus {
            channel_uri,
            has_unread_messages,
            unread_ping_count,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_messages_is_never_unread() {
        assert!(!channel_has_unread(None, None, None));
        assert!(!channel_has_unread(None, Some("aaa"), Some("2026-01-01T00:00:00.000Z")));
    }

    #[test]
    fn with_cursor_compares_rkeys_and_ignores_join_boundary() {
        // Newest message after the cursor → unread.
        assert!(channel_has_unread(
            Some(("bbb", "2020-01-01T00:00:00.000Z")),
            Some("aaa"),
            // A future join boundary must not suppress this — cursor wins.
            Some("2999-01-01T00:00:00.000Z"),
        ));
        // Newest message at/before the cursor → read.
        assert!(!channel_has_unread(
            Some(("aaa", "2026-01-01T00:00:00.000Z")),
            Some("aaa"),
            None,
        ));
        assert!(!channel_has_unread(
            Some(("aaa", "2026-01-01T00:00:00.000Z")),
            Some("bbb"),
            None,
        ));
    }

    #[test]
    fn without_cursor_uses_join_boundary() {
        // Message indexed after the user joined → unread.
        assert!(channel_has_unread(
            Some(("bbb", "2026-06-25T12:00:00.000Z")),
            None,
            Some("2026-06-25T11:00:00.000Z"),
        ));
        // Message indexed before the user joined → not unread (the join case).
        assert!(!channel_has_unread(
            Some(("bbb", "2026-06-25T10:00:00.000Z")),
            None,
            Some("2026-06-25T11:00:00.000Z"),
        ));
        // Exactly at the boundary → not unread (strictly-after semantics).
        assert!(!channel_has_unread(
            Some(("bbb", "2026-06-25T11:00:00.000Z")),
            None,
            Some("2026-06-25T11:00:00.000Z"),
        ));
    }

    #[test]
    fn without_cursor_or_boundary_any_message_is_unread() {
        assert!(channel_has_unread(
            Some(("bbb", "2026-06-25T10:00:00.000Z")),
            None,
            None,
        ));
    }
}
