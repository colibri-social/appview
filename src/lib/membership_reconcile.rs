//! Reconciles join intents against admissions.
//!
//! Auto-admit runs once, off a single tap event (see
//! `tap::process_membership_create`). Anything that stops that one attempt from
//! writing the community-side `social.colibri.member` record leaves the user
//! half-joined for good: their `social.colibri.membership` says they joined,
//! every membership-derived read says they did not.
//!
//! This sweep closes that gap. It walks the indexed join intents, keeps the ones
//! for open native communities with no member record and no active ban, and
//! admits them through the same idempotent [`moderation::write_member_record`]
//! the live path uses.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rocket::tokio::time::{MissedTickBehavior, interval};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriMembership;
use crate::lib::community_credentials::skip_community_write;
use crate::lib::community_record::fetch_community_record;
use crate::lib::moderation;
use crate::models::record_data;

const MEMBERSHIP_NSID: &str = "social.colibri.membership";
const COMMUNITY_SELF_RKEY: &str = "self";

/// Default sweep cadence. Long enough to be free even with a large index, short
/// enough that a transient auto-admit failure heals within one coffee break.
const DEFAULT_RECONCILE_SECS: u64 = 900;

/// Sweep interval from `MEMBERSHIP_RECONCILE_SECS`. `0` disables the periodic
/// sweep, which the spawn site in `main.rs` handles by not spawning it.
pub fn reconcile_interval_secs() -> u64 {
    std::env::var("MEMBERSHIP_RECONCILE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RECONCILE_SECS)
}

/// One join intent worth checking: the user, the community they mean to be in,
/// and the intent record backing it (kept for `member.fromMembership`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub user_did: String,
    pub community_did: String,
    pub membership_uri: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileSummary {
    pub examined: usize,
    pub admitted: usize,
    pub already_admitted: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Extracts the admissible candidates from raw `social.colibri.membership`
/// rows: drops malformed payloads and URIs, drops legacy communities (rkey
/// other than `self`, which the AppView holds no credentials for), and keeps
/// one entry per (user, community).
pub fn candidates_from_rows(rows: Vec<record_data::Model>) -> Vec<Candidate> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut candidates = Vec::new();

    for row in rows {
        let Ok(membership) = serde_json::from_value::<ColibriMembership>(row.data) else {
            continue;
        };
        let Some(community) = AtUri::parse(&membership.community) else {
            continue;
        };
        if community.rkey != COMMUNITY_SELF_RKEY {
            continue;
        }
        if !seen.insert((row.did.clone(), community.authority.clone())) {
            continue;
        }
        candidates.push(Candidate {
            membership_uri: format!("at://{}/{}/{}", row.did, row.nsid, row.rkey),
            user_did: row.did,
            community_did: community.authority,
        });
    }

    candidates
}

/// Runs one sweep and returns what it did.
pub async fn reconcile_once(db: &DatabaseConnection) -> Result<ReconcileSummary, DbErr> {
    let rows = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MEMBERSHIP_NSID))
        .all(db)
        .await?;

    let candidates = candidates_from_rows(rows);
    let mut summary = ReconcileSummary {
        examined: candidates.len(),
        ..Default::default()
    };

    let mut open_community: HashMap<String, bool> = HashMap::new();
    let mut banned_by_community: HashMap<String, Vec<String>> = HashMap::new();

    for candidate in candidates {
        let community_did = candidate.community_did.clone();

        if !open_community.contains_key(&community_did) {
            let open = match fetch_community_record(db, &community_did, COMMUNITY_SELF_RKEY).await {
                Ok(Some(community)) => !community.requires_approval_to_join,
                Ok(None) => false,
                Err(e) => {
                    log::warn!("reconcile: community fetch failed for {community_did}: {e}");
                    false
                }
            };
            open_community.insert(community_did.clone(), open);
        }
        if open_community.get(&community_did) != Some(&true) {
            summary.skipped += 1;
            continue;
        }

        if !banned_by_community.contains_key(&community_did) {
            let banned = moderation::currently_banned_dids(db, &community_did)
                .await
                .unwrap_or_default();
            banned_by_community.insert(community_did.clone(), banned);
        }
        if banned_by_community
            .get(&community_did)
            .is_some_and(|banned| banned.contains(&candidate.user_did))
        {
            summary.skipped += 1;
            continue;
        }

        match moderation::find_member_rkey(db, &community_did, &candidate.user_did).await {
            Ok(Some(_)) => {
                summary.already_admitted += 1;
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!(
                    "reconcile: member lookup failed for {} in {community_did}: {e}",
                    candidate.user_did
                );
                summary.failed += 1;
                continue;
            }
        }

        match moderation::write_member_record(
            db,
            &community_did,
            &candidate.user_did,
            vec![],
            Some(candidate.membership_uri.clone()),
        )
        .await
        {
            Ok(Some(_)) => {
                summary.admitted += 1;
                log::info!(
                    "reconcile: admitted {} to open community {community_did}",
                    candidate.user_did
                );
            }
            Ok(None) => summary.already_admitted += 1,
            Err(e) => {
                summary.failed += 1;
                if !skip_community_write(&e) {
                    log::warn!(
                        "reconcile: admission failed for {} in {community_did}: {e}",
                        candidate.user_did
                    );
                }
            }
        }
    }

    Ok(summary)
}

/// Sweeps once at startup and then on every tick. Silent unless it changed
/// something or hit a failure, so a healthy deployment stays quiet.
pub async fn run_reconciler(db: DatabaseConnection) {
    let window = Duration::from_secs(std::cmp::max(reconcile_interval_secs(), 1));
    let mut ticker = interval(window);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        match reconcile_once(&db).await {
            Ok(summary) => {
                if summary.admitted > 0 || summary.failed > 0 {
                    log::info!(
                        "membership reconcile: {} examined, {} admitted, {} already admitted, {} \
                         skipped, {} failed",
                        summary.examined,
                        summary.admitted,
                        summary.already_admitted,
                        summary.skipped,
                        summary.failed
                    );
                } else {
                    log::debug!(
                        "membership reconcile: {} examined, nothing to admit",
                        summary.examined
                    );
                }
            }
            Err(e) => log::error!("membership reconcile failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(did: &str, rkey: &str, community: &str) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from(did),
            nsid: String::from(MEMBERSHIP_NSID),
            rkey: String::from(rkey),
            data: serde_json::json!({
                "$type": MEMBERSHIP_NSID,
                "community": community,
                "createdAt": "2026-07-29T12:37:32.074Z",
            }),
            indexed_at: String::from("2026-07-29T12:37:33.000Z"),
        }
    }

    #[test]
    fn keeps_native_community_intents() {
        let candidates = candidates_from_rows(vec![row(
            "did:plc:alice",
            "m1",
            "at://did:plc:community/social.colibri.community/self",
        )]);
        assert_eq!(
            candidates,
            vec![Candidate {
                user_did: String::from("did:plc:alice"),
                community_did: String::from("did:plc:community"),
                membership_uri: String::from("at://did:plc:alice/social.colibri.membership/m1"),
            }]
        );
    }

    #[test]
    fn drops_legacy_and_malformed_intents() {
        let candidates = candidates_from_rows(vec![
            row(
                "did:plc:alice",
                "m1",
                "at://did:plc:owner/social.colibri.community/3mhyddoabof2r",
            ),
            row("did:plc:bob", "m2", "not-a-uri"),
        ]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn keeps_one_entry_per_user_and_community() {
        let candidates = candidates_from_rows(vec![
            row(
                "did:plc:alice",
                "m1",
                "at://did:plc:community/social.colibri.community/self",
            ),
            row(
                "did:plc:alice",
                "m2",
                "at://did:plc:community/social.colibri.community/self",
            ),
            row(
                "did:plc:alice",
                "m3",
                "at://did:plc:other/social.colibri.community/self",
            ),
        ]);
        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0].membership_uri.split('/').next_back(),
            Some("m1")
        );
        assert_eq!(candidates[1].community_did, String::from("did:plc:other"));
    }
}
