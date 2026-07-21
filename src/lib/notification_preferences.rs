//! Per-user, on-protocol notification level.
//!
//! Stored as a `social.colibri.actor.notificationPreference` record on the
//! user's own repo (deterministic rkey, like `social.colibri.actor.mute`),
//! and read back through the generic `record_data` firehose mirror — same
//! shape as `list_mutes_handler::fetch_mutes`. Absence of a record means the
//! default level, `"all"`.

use std::collections::{HashMap, HashSet};

use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::Deserialize;

use crate::models::record_data;

pub const NOTIFICATION_PREFERENCE_NSID: &str = "social.colibri.actor.notificationPreference";
pub const NOTIFICATION_PREFERENCE_RKEY: &str = "self";

pub const LEVEL_ALL: &str = "all";
pub const LEVEL_MENTIONS_AND_REPLIES: &str = "mentionsAndReplies";

#[derive(Deserialize)]
struct StoredPreference {
    level: String,
}

fn level_from_record(record: &record_data::Model) -> Option<String> {
    let stored: StoredPreference = serde_json::from_value(record.data.clone()).ok()?;
    if stored.level == LEVEL_MENTIONS_AND_REPLIES {
        Some(stored.level)
    } else {
        // Any unrecognized/legacy value falls back to the default rather than
        // silently narrowing what a client that predates a future level would see.
        None
    }
}

/// The recipient's notification level, defaulting to [`LEVEL_ALL`] when no
/// record exists or it doesn't parse.
pub async fn level_for(db: &DatabaseConnection, did: &str) -> Result<String, DbErr> {
    let record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(did))
        .filter(record_data::Column::Nsid.eq(NOTIFICATION_PREFERENCE_NSID))
        .filter(record_data::Column::Rkey.eq(NOTIFICATION_PREFERENCE_RKEY))
        .one(db)
        .await?;

    Ok(record
        .as_ref()
        .and_then(level_from_record)
        .unwrap_or_else(|| LEVEL_ALL.to_string()))
}

/// Given a set of candidate DIDs, returns the subset that has explicitly
/// opted down to mentions/replies-only. Everyone else defaults to `"all"`.
/// Batched into a single `IN` query so expanding "notify every member" for a
/// message doesn't cost one round-trip per member.
pub async fn mentions_and_replies_only_dids(
    db: &DatabaseConnection,
    dids: &[String],
) -> Result<HashSet<String>, DbErr> {
    if dids.is_empty() {
        return Ok(HashSet::new());
    }

    let records = record_data::Entity::find()
        .filter(record_data::Column::Did.is_in(dids.to_vec()))
        .filter(record_data::Column::Nsid.eq(NOTIFICATION_PREFERENCE_NSID))
        .filter(record_data::Column::Rkey.eq(NOTIFICATION_PREFERENCE_RKEY))
        .all(db)
        .await?;

    let mut by_did: HashMap<String, String> = HashMap::new();
    for record in &records {
        if let Some(level) = level_from_record(record) {
            by_did.insert(record.did.clone(), level);
        }
    }

    Ok(by_did
        .into_iter()
        .filter(|(_, level)| level == LEVEL_MENTIONS_AND_REPLIES)
        .map(|(did, _)| did)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use serde_json::json;

    fn pref_record(did: &str, level: &str) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: did.to_string(),
            nsid: NOTIFICATION_PREFERENCE_NSID.to_string(),
            rkey: NOTIFICATION_PREFERENCE_RKEY.to_string(),
            data: json!({ "level": level }),
            indexed_at: String::new(),
        }
    }

    #[tokio::test]
    async fn level_for_defaults_to_all_when_no_record() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<record_data::Model>::new()])
            .into_connection();
        assert_eq!(level_for(&db, "did:plc:me").await.unwrap(), LEVEL_ALL);
    }

    #[tokio::test]
    async fn level_for_reads_explicit_mentions_and_replies() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![pref_record("did:plc:me", LEVEL_MENTIONS_AND_REPLIES)]])
            .into_connection();
        assert_eq!(
            level_for(&db, "did:plc:me").await.unwrap(),
            LEVEL_MENTIONS_AND_REPLIES
        );
    }

    #[tokio::test]
    async fn level_for_falls_back_to_all_on_unrecognized_value() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![pref_record("did:plc:me", "somethingElse")]])
            .into_connection();
        assert_eq!(level_for(&db, "did:plc:me").await.unwrap(), LEVEL_ALL);
    }

    #[tokio::test]
    async fn mentions_and_replies_only_dids_returns_empty_for_empty_input() {
        let db = crate::lib::test_fixtures::mock_db();
        let out = mentions_and_replies_only_dids(&db, &[]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn mentions_and_replies_only_dids_filters_to_opted_down_members() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                pref_record("did:plc:alice", LEVEL_MENTIONS_AND_REPLIES),
                pref_record("did:plc:bob", LEVEL_ALL),
            ]])
            .into_connection();
        let out = mentions_and_replies_only_dids(
            &db,
            &[
                String::from("did:plc:alice"),
                String::from("did:plc:bob"),
                String::from("did:plc:carol"),
            ],
        )
        .await
        .unwrap();
        assert_eq!(out.len(), 1);
        assert!(out.contains("did:plc:alice"));
    }
}
