//! Whether an actor may participate in a community at all: the admission
//! check, distinct from [`crate::lib::channel_authz`] (*where inside* a
//! community an admitted member may post) and [`crate::lib::permissions`]
//! (admin actions). Participation requires the community-side
//! `social.colibri.member` record the AppView writes on admission, so its
//! absence means never admitted, kicked, banned, or left.
//!
//! Messages and reactions live on the author's own repo, so the write can never
//! be prevented; this gate decides whether the AppView indexes, notifies and
//! broadcasts them. Inconclusive lookups fail open, matching the
//! channel-restriction gate in `tap::process_event`.

use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, prelude::Expr};

use crate::lib::event_scope::CommunityResolver;
use crate::lib::moderation;
use crate::lib::time::iso8601_ago;
use crate::models::record_data;

const MEMBERSHIP_NSID: &str = "social.colibri.membership";

/// How long a freshly indexed `social.colibri.membership` keeps a not-yet
/// admitted actor's records in the retry path instead of dropping them. Covers
/// the window between the join intent landing and auto-admit writing the
/// community-side member record.
pub const ADMISSION_GRACE_SECS: u64 = 120;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Participation {
    /// Index, notify and broadcast as normal.
    Allow,
    /// Admission looks imminent: leave the event unacked so tap redelivers it
    /// once the member record exists.
    Defer,
    /// Not admitted: ack the event so tap stops redelivering, but index
    /// nothing and tell nobody.
    Reject,
}

/// Resolves participation for `actor_did` in the community hosted at
/// `community_did`.
pub async fn may_participate(
    db: &DatabaseConnection,
    resolver: &CommunityResolver,
    community_did: &str,
    actor_did: &str,
) -> Participation {
    if actor_did == community_did {
        return Participation::Allow;
    }

    match resolver.is_native_community(db, community_did).await {
        Some(true) => {}
        Some(false) | None => return Participation::Allow,
    }

    admission_for(db, community_did, actor_did).await
}

/// The member-record half of [`may_participate`], split out so it can be
/// exercised without a resolver.
pub async fn admission_for(
    db: &DatabaseConnection,
    community_did: &str,
    actor_did: &str,
) -> Participation {
    match moderation::find_member_rkey(db, community_did, actor_did).await {
        Ok(Some(_)) => return Participation::Allow,
        Ok(None) => {}
        Err(e) => {
            log::warn!(
                "member lookup failed for {actor_did} in {community_did}: {e}; allowing through"
            );
            return Participation::Allow;
        }
    }

    match has_fresh_intent(db, community_did, actor_did).await {
        Ok(true) => Participation::Defer,
        Ok(false) => Participation::Reject,
        Err(e) => {
            log::warn!(
                "join-intent lookup failed for {actor_did} in {community_did}: {e}; allowing through"
            );
            Participation::Allow
        }
    }
}

/// Whether `actor_did` holds a `social.colibri.membership` for this community
/// that was indexed within [`ADMISSION_GRACE_SECS`].
async fn has_fresh_intent(
    db: &DatabaseConnection,
    community_did: &str,
    actor_did: &str,
) -> Result<bool, DbErr> {
    let community_uri = format!("at://{community_did}/social.colibri.community/self");
    let row = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(actor_did))
        .filter(record_data::Column::Nsid.eq(MEMBERSHIP_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(community_uri)],
        ))
        .one(db)
        .await?;

    let cutoff = iso8601_ago(ADMISSION_GRACE_SECS);
    Ok(row.is_some_and(|row| row.indexed_at >= cutoff))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use crate::lib::time::current_iso8601_utc;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    const COMMUNITY: &str = "did:plc:community";
    const ACTOR: &str = "did:plc:alice";

    fn row(nsid: &str, data: serde_json::Value, indexed_at: String) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from(COMMUNITY),
            nsid: nsid.to_string(),
            rkey: String::from("r1"),
            data,
            indexed_at,
        }
    }

    fn member_row() -> record_data::Model {
        row(
            "social.colibri.member",
            serde_json::json!({ "subject": ACTOR, "joinedAt": "2026-07-01T00:00:00.000Z" }),
            current_iso8601_utc(),
        )
    }

    fn intent_row(indexed_at: String) -> record_data::Model {
        row(
            MEMBERSHIP_NSID,
            serde_json::json!({
                "community": format!("at://{COMMUNITY}/social.colibri.community/self"),
            }),
            indexed_at,
        )
    }

    fn db_with(results: Vec<Vec<record_data::Model>>) -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results(results)
            .into_connection()
    }

    #[tokio::test]
    async fn community_account_always_allowed() {
        let resolver = CommunityResolver::new();
        assert_eq!(
            may_participate(&mock_db(), &resolver, COMMUNITY, COMMUNITY).await,
            Participation::Allow
        );
    }

    #[tokio::test]
    async fn legacy_community_is_not_gated() {
        let resolver = CommunityResolver::new();
        resolver.seed_native_community(COMMUNITY, false);
        assert_eq!(
            may_participate(&mock_db(), &resolver, COMMUNITY, ACTOR).await,
            Participation::Allow
        );
    }

    #[tokio::test]
    async fn admitted_member_is_allowed() {
        let resolver = CommunityResolver::new();
        resolver.seed_native_community(COMMUNITY, true);
        let db = db_with(vec![vec![member_row()]]);
        assert_eq!(
            may_participate(&db, &resolver, COMMUNITY, ACTOR).await,
            Participation::Allow
        );
    }

    #[tokio::test]
    async fn fresh_intent_without_member_record_defers() {
        let db = db_with(vec![vec![], vec![intent_row(current_iso8601_utc())]]);
        assert_eq!(
            admission_for(&db, COMMUNITY, ACTOR).await,
            Participation::Defer
        );
    }

    #[tokio::test]
    async fn stale_intent_without_member_record_is_rejected() {
        let db = db_with(vec![
            vec![],
            vec![intent_row(String::from("2026-07-01T00:00:00.000Z"))],
        ]);
        assert_eq!(
            admission_for(&db, COMMUNITY, ACTOR).await,
            Participation::Reject
        );
    }

    #[tokio::test]
    async fn no_member_and_no_intent_is_rejected() {
        let db = db_with(vec![vec![], vec![]]);
        assert_eq!(
            admission_for(&db, COMMUNITY, ACTOR).await,
            Participation::Reject
        );
    }
}
