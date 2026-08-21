use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, prelude::Expr};
use serde_json::Value;

use crate::lib::colibri::ColibriMember;
use crate::lib::community_credentials::skip_community_write;
use crate::lib::community_write;
use crate::lib::moderation::MEMBER_NSID;
use crate::models::record_data;

pub fn subject_of(row: &record_data::Model) -> Option<String> {
    row.data
        .get("subject")
        .and_then(Value::as_str)
        .map(String::from)
}

fn role_count(row: &record_data::Model) -> usize {
    row.data
        .get("roles")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default()
}

fn compare(a: &record_data::Model, b: &record_data::Model) -> Ordering {
    role_count(a)
        .cmp(&role_count(b))
        .then_with(|| b.rkey.cmp(&a.rkey))
}

pub fn authoritative(rows: &[record_data::Model]) -> Option<&record_data::Model> {
    rows.iter().max_by(|a, b| compare(a, b))
}

pub fn one_per_subject(rows: Vec<record_data::Model>) -> Vec<record_data::Model> {
    let mut winners: HashMap<String, record_data::Model> = HashMap::new();

    for row in rows {
        let Some(subject) = subject_of(&row) else {
            continue;
        };
        match winners.get(&subject) {
            Some(current) if compare(&row, current) != Ordering::Greater => {}
            _ => {
                winners.insert(subject, row);
            }
        }
    }

    let mut result: Vec<record_data::Model> = winners.into_values().collect();
    result.sort_by(|a, b| a.rkey.cmp(&b.rkey));
    result
}

pub async fn rows_for_subject(
    db: &DatabaseConnection,
    community_did: &str,
    subject_did: &str,
) -> Result<Vec<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_did))
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(subject_did.to_string())],
        ))
        .all(db)
        .await
}

pub async fn find_authoritative(
    db: &DatabaseConnection,
    community_did: &str,
    subject_did: &str,
) -> Result<Option<record_data::Model>, DbErr> {
    let rows = rows_for_subject(db, community_did, subject_did).await?;
    Ok(authoritative(&rows).cloned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateSet {
    pub subject: String,
    pub keep_rkey: String,
    pub keep_data: Value,
    pub merged: Value,
    pub drop_rkeys: Vec<String>,
}

fn merge_members(keep: &ColibriMember, losers: &[ColibriMember]) -> ColibriMember {
    let mut merged = keep.clone();

    for loser in losers {
        for role in &loser.roles {
            if !merged.roles.contains(role) {
                merged.roles.push(role.clone());
            }
        }
        if loser.joined_at < merged.joined_at {
            merged.joined_at = loser.joined_at.clone();
        }
        if merged.nickname.is_none() {
            merged.nickname = loser.nickname.clone();
        }
        if merged.from_membership.is_none() {
            merged.from_membership = loser.from_membership.clone();
        }
    }

    merged
}

pub fn duplicate_sets(rows: &[record_data::Model]) -> Vec<DuplicateSet> {
    let mut grouped: HashMap<String, Vec<&record_data::Model>> = HashMap::new();
    for row in rows {
        let Some(subject) = subject_of(row) else {
            continue;
        };
        grouped.entry(subject).or_default().push(row);
    }

    let mut sets: Vec<DuplicateSet> = grouped
        .into_iter()
        .filter(|(_, group)| group.len() > 1)
        .filter_map(|(subject, mut group)| {
            group.sort_by(|a, b| a.rkey.cmp(&b.rkey));
            let keep = group.iter().copied().max_by(|a, b| compare(a, b))?;

            let keep_member = serde_json::from_value::<ColibriMember>(keep.data.clone()).ok()?;

            let mut losers: Vec<ColibriMember> = Vec::new();
            let mut drop_rkeys: Vec<String> = Vec::new();
            for row in group.iter().filter(|row| row.rkey != keep.rkey) {
                losers.push(serde_json::from_value::<ColibriMember>(row.data.clone()).ok()?);
                drop_rkeys.push(row.rkey.clone());
            }

            let merged = serde_json::to_value(merge_members(&keep_member, &losers)).ok()?;

            Some(DuplicateSet {
                subject,
                keep_rkey: keep.rkey.clone(),
                keep_data: keep.data.clone(),
                merged,
                drop_rkeys,
            })
        })
        .collect();

    sets.sort_by(|a, b| a.subject.cmp(&b.subject));
    sets
}

fn inflight() -> &'static Mutex<HashSet<String>> {
    static INFLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    INFLIGHT.get_or_init(|| Mutex::new(HashSet::new()))
}

pub fn spawn_dedupe(db: &DatabaseConnection, community_did: &str, rows: &[record_data::Model]) {
    for set in duplicate_sets(rows) {
        let guard = format!("{community_did}/{}", set.subject);
        if !inflight().lock().unwrap().insert(guard.clone()) {
            continue;
        }

        let db = db.clone();
        let community_did = community_did.to_string();
        rocket::tokio::spawn(async move {
            dedupe_one(&db, &community_did, &set).await;
            inflight().lock().unwrap().remove(&guard);
        });
    }
}

async fn dedupe_one(db: &DatabaseConnection, community_did: &str, set: &DuplicateSet) {
    if set.merged != set.keep_data
        && let Err(e) = community_write::put_record(
            db,
            community_did,
            MEMBER_NSID,
            &set.keep_rkey,
            set.merged.clone(),
        )
        .await
    {
        if !skip_community_write(&e) {
            log::warn!(
                "member dedupe for {} in {community_did} could not merge into {}: {e}",
                set.subject,
                set.keep_rkey
            );
        }
        return;
    }

    for rkey in &set.drop_rkeys {
        match community_write::delete_record(db, community_did, MEMBER_NSID, rkey).await {
            Ok(()) => log::info!(
                "member dedupe: dropped duplicate {community_did}/{rkey} for {}, keeping {}",
                set.subject,
                set.keep_rkey
            ),
            Err(e) => {
                if !skip_community_write(&e) {
                    log::warn!(
                        "member dedupe for {} in {community_did} could not drop {rkey}: {e}",
                        set.subject
                    );
                }
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(rkey: &str, subject: &str, roles: Vec<&str>) -> record_data::Model {
        record_data::Model {
            id: 0,
            did: String::from("did:plc:community"),
            nsid: MEMBER_NSID.to_string(),
            rkey: rkey.to_string(),
            data: serde_json::json!({
                "$type": MEMBER_NSID,
                "subject": subject,
                "roles": roles,
                "joinedAt": "2026-08-13T22:17:45.045Z",
            }),
            indexed_at: String::from("2026-08-13T22:17:45.045Z"),
        }
    }

    #[test]
    fn the_role_bearing_record_wins_over_a_later_empty_duplicate() {
        let rows = vec![
            row(
                "3msyodfzdhfvw",
                "did:plc:owner",
                vec!["owner-role", "admin"],
            ),
            row("3mtm3gkst5u2k", "did:plc:owner", vec![]),
        ];

        assert_eq!(authoritative(&rows).unwrap().rkey, "3msyodfzdhfvw");
    }

    #[test]
    fn the_role_bearing_record_wins_even_when_it_is_the_newer_one() {
        let rows = vec![
            row("3mt2gd36ibs2n", "did:plc:member", vec![]),
            row("3mt2gd2eja22n", "did:plc:member", vec!["player"]),
        ];

        assert_eq!(authoritative(&rows).unwrap().rkey, "3mt2gd2eja22n");
    }

    #[test]
    fn equally_ranked_duplicates_resolve_to_the_oldest_rkey() {
        let rows = vec![
            row("3mtm3fzng442k", "did:plc:member", vec![]),
            row("3mt7n4vbobc2k", "did:plc:member", vec![]),
        ];

        assert_eq!(authoritative(&rows).unwrap().rkey, "3mt7n4vbobc2k");
    }

    #[test]
    fn one_per_subject_keeps_a_single_winner_for_each_member() {
        let rows = vec![
            row("3msyodfzdhfvw", "did:plc:owner", vec!["owner-role"]),
            row("3mtm3gkst5u2k", "did:plc:owner", vec![]),
            row("3mtae547e2s2k", "did:plc:member", vec!["player"]),
        ];

        let kept = one_per_subject(rows);
        let rkeys: Vec<&str> = kept.iter().map(|r| r.rkey.as_str()).collect();

        assert_eq!(rkeys, vec!["3msyodfzdhfvw", "3mtae547e2s2k"]);
    }

    #[test]
    fn a_member_with_one_record_is_not_a_duplicate_set() {
        let rows = vec![row("3mtae547e2s2k", "did:plc:member", vec!["player"])];

        assert!(duplicate_sets(&rows).is_empty());
    }

    #[test]
    fn a_duplicate_set_keeps_the_roles_and_drops_the_rest() {
        let rows = vec![
            row(
                "3msyodfzdhfvw",
                "did:plc:owner",
                vec!["owner-role", "admin"],
            ),
            row("3mtm3gkst5u2k", "did:plc:owner", vec![]),
        ];

        let sets = duplicate_sets(&rows);

        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].subject, "did:plc:owner");
        assert_eq!(sets[0].keep_rkey, "3msyodfzdhfvw");
        assert_eq!(sets[0].drop_rkeys, vec![String::from("3mtm3gkst5u2k")]);
    }

    #[test]
    fn an_unreadable_duplicate_is_never_dropped() {
        let keep = row("3msyodfzdhfvw", "did:plc:owner", vec!["owner-role"]);
        let mut unreadable = row("3mtm3gkst5u2k", "did:plc:owner", vec![]);
        unreadable.data = serde_json::json!({ "subject": "did:plc:owner" });

        assert!(duplicate_sets(&[keep, unreadable]).is_empty());
    }

    #[test]
    fn merging_carries_over_roles_and_audit_data_the_survivor_lacks() {
        let mut keep = row("3msyodfzdhfvw", "did:plc:owner", vec!["owner-role"]);
        keep.data["joinedAt"] = Value::from("2026-08-13T22:17:45.045Z");

        let mut loser = row("3mtm3gkst5u2k", "did:plc:owner", vec!["admin"]);
        loser.data["joinedAt"] = Value::from("2026-08-11T10:00:00.000Z");
        loser.data["fromMembership"] =
            Value::from("at://did:plc:owner/social.colibri.membership/3mta5l3u7dshk");
        loser.data["nickname"] = Value::from("atpcraft");

        let sets = duplicate_sets(&[keep, loser]);
        let merged = &sets[0].merged;

        assert_eq!(
            merged["roles"],
            serde_json::json!(["owner-role", "admin"]),
            "the survivor absorbs roles only the duplicate carried"
        );
        assert_eq!(merged["joinedAt"], "2026-08-11T10:00:00.000Z");
        assert_eq!(
            merged["fromMembership"],
            "at://did:plc:owner/social.colibri.membership/3mta5l3u7dshk"
        );
        assert_eq!(merged["nickname"], "atpcraft");
    }

    #[test]
    fn a_survivor_that_already_holds_everything_needs_no_merge_write() {
        let keep = row("3msyodfzdhfvw", "did:plc:owner", vec!["owner-role"]);
        let loser = row("3mtm3gkst5u2k", "did:plc:owner", vec![]);

        let sets = duplicate_sets(&[keep, loser]);

        assert_eq!(sets[0].merged, sets[0].keep_data);
    }
}
