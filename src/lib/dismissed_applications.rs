//! AppView-only (off-protocol) storage for dismissed join applications.
//!
//! Dismissal is a purely local moderation convenience — it has no on-protocol
//! representation and isn't broadcast over the firehose. A dismissed
//! application is still a pending `social.colibri.membership` as far as the
//! protocol is concerned; dismissal just hides it from the active queue in
//! `listApplications` until a moderator restores it (`undismissApplication`)
//! or it resolves normally (`approveMembership`, which clears the dismissal
//! row too so a re-application after a kick doesn't inherit a stale one).

use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use std::collections::HashSet;

use crate::lib::time::current_iso8601_utc;
use crate::models::dismissed_applications;

/// Returns the set of applicant DIDs currently dismissed for `community_did`.
pub async fn dismissed_dids(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<HashSet<String>, DbErr> {
    let rows = dismissed_applications::Entity::find()
        .filter(dismissed_applications::Column::CommunityDid.eq(community_did))
        .all(db)
        .await?;
    Ok(rows.into_iter().map(|r| r.applicant_did).collect())
}

/// Marks `applicant_did` as dismissed for `community_did`. Idempotent — a
/// second dismissal of an already-dismissed applicant is a no-op.
pub async fn dismiss(
    db: &DatabaseConnection,
    community_did: &str,
    applicant_did: &str,
) -> Result<(), DbErr> {
    let existing = dismissed_applications::Entity::find()
        .filter(dismissed_applications::Column::CommunityDid.eq(community_did))
        .filter(dismissed_applications::Column::ApplicantDid.eq(applicant_did))
        .one(db)
        .await?;
    if existing.is_some() {
        return Ok(());
    }

    let active = dismissed_applications::ActiveModel {
        community_did: sea_orm::ActiveValue::Set(community_did.to_string()),
        applicant_did: sea_orm::ActiveValue::Set(applicant_did.to_string()),
        dismissed_at: sea_orm::ActiveValue::Set(current_iso8601_utc()),
        ..Default::default()
    };
    dismissed_applications::Entity::insert(active)
        .exec(db)
        .await?;
    Ok(())
}

/// Removes a dismissal, restoring `applicant_did` to the active queue.
/// Idempotent — undismissing an applicant who isn't dismissed is a no-op.
pub async fn undismiss(
    db: &DatabaseConnection,
    community_did: &str,
    applicant_did: &str,
) -> Result<(), DbErr> {
    dismissed_applications::Entity::delete_many()
        .filter(dismissed_applications::Column::CommunityDid.eq(community_did))
        .filter(dismissed_applications::Column::ApplicantDid.eq(applicant_did))
        .exec(db)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn dismissed_dids_collects_rows_for_community() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                dismissed_applications::Model {
                    id: 1,
                    community_did: String::from("did:plc:community"),
                    applicant_did: String::from("did:plc:alice"),
                    dismissed_at: String::from("2026-01-01T00:00:00Z"),
                },
                dismissed_applications::Model {
                    id: 2,
                    community_did: String::from("did:plc:community"),
                    applicant_did: String::from("did:plc:bob"),
                    dismissed_at: String::from("2026-01-02T00:00:00Z"),
                },
            ]])
            .into_connection();

        let dids = dismissed_dids(&db, "did:plc:community").await.unwrap();
        assert_eq!(dids.len(), 2);
        assert!(dids.contains("did:plc:alice"));
        assert!(dids.contains("did:plc:bob"));
    }

    #[tokio::test]
    async fn dismiss_is_idempotent_when_already_dismissed() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![dismissed_applications::Model {
                id: 1,
                community_did: String::from("did:plc:community"),
                applicant_did: String::from("did:plc:alice"),
                dismissed_at: String::from("2026-01-01T00:00:00Z"),
            }]])
            .into_connection();

        // Only the lookup query is mocked — if `dismiss` tried to insert
        // despite the existing row, the missing insert-result mock would
        // panic, so reaching `Ok(())` proves the idempotent skip happened.
        dismiss(&db, "did:plc:community", "did:plc:alice")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn undismiss_is_a_no_op_without_panicking() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([sea_orm::MockExecResult {
                last_insert_id: 0,
                rows_affected: 0,
            }])
            .into_connection();
        undismiss(&db, "did:plc:community", "did:plc:alice")
            .await
            .unwrap();
    }
}
