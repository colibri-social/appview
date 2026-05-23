use std::collections::HashMap;

use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::models::record_data;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReactionSummary {
    pub emoji: String,
    pub count: u32,
    #[serde(rename = "reactorDIDs")]
    pub reactor_dids: Vec<String>,
}

#[derive(Deserialize)]
struct StoredReaction {
    emoji: String,
    parent: String,
}

/// Loads every reaction targeting the provided message rkeys and returns them
/// keyed by the target message rkey, with reactions of the same emoji folded
/// into a single summary entry.
pub async fn group_reactions_for_messages(
    db: &DatabaseConnection,
    message_rkeys: &[String],
) -> Result<HashMap<String, Vec<ReactionSummary>>, DbErr> {
    if message_rkeys.is_empty() {
        return Ok(HashMap::new());
    }

    let values: Vec<sea_orm::Value> = message_rkeys
        .iter()
        .map(|k| sea_orm::Value::from(k.clone()))
        .collect();

    let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${i}")).collect();
    let in_clause = format!(
        r#""record_data"."data"->>'parent' IN ({})"#,
        placeholders.join(", ")
    );

    let records = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.reaction"))
        .filter(Expr::cust_with_values(in_clause, values))
        .all(db)
        .await?;

    Ok(group_reaction_records(records))
}

/// Loads every reaction targeting a single message rkey.
pub async fn list_reactions_for_message(
    db: &DatabaseConnection,
    message_rkey: &str,
) -> Result<Vec<ReactionSummary>, DbErr> {
    let records = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.reaction"))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'parent' = $1"#,
            vec![sea_orm::Value::from(message_rkey.to_string())],
        ))
        .all(db)
        .await?;

    let mut grouped = group_reaction_records(records);
    Ok(grouped.remove(message_rkey).unwrap_or_default())
}

fn group_reaction_records(
    records: Vec<record_data::Model>,
) -> HashMap<String, Vec<ReactionSummary>> {
    // (target_message_rkey, emoji) -> Vec<reactor_did>
    let mut buckets: HashMap<(String, String), Vec<String>> = HashMap::new();

    for record in records {
        let Ok(stored) = serde_json::from_value::<StoredReaction>(record.data) else {
            continue;
        };
        buckets
            .entry((stored.parent, stored.emoji))
            .or_default()
            .push(record.did);
    }

    let mut grouped: HashMap<String, Vec<ReactionSummary>> = HashMap::new();
    for ((target, emoji), mut dids) in buckets {
        dids.sort();
        dids.dedup();
        grouped.entry(target).or_default().push(ReactionSummary {
            emoji,
            count: dids.len() as u32,
            reactor_dids: dids,
        });
    }

    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reaction(did: &str, rkey: &str, target: &str, emoji: &str) -> record_data::Model {
        record_data::Model {
            id: 0,
            did: did.to_string(),
            nsid: String::from("social.colibri.reaction"),
            rkey: rkey.to_string(),
            data: serde_json::json!({
                "emoji": emoji,
                "parent": target,
            }),
        }
    }

    #[test]
    fn groups_reactions_by_message_and_emoji() {
        let records = vec![
            reaction("did:plc:alice", "r1", "msg-1", "🦜"),
            reaction("did:plc:bob", "r2", "msg-1", "🦜"),
            reaction("did:plc:alice", "r3", "msg-1", "🔥"),
            reaction("did:plc:carol", "r4", "msg-2", "🔥"),
        ];
        let grouped = group_reaction_records(records);

        let msg1 = grouped.get("msg-1").unwrap();
        let parrot = msg1.iter().find(|r| r.emoji == "🦜").unwrap();
        assert_eq!(parrot.count, 2);
        assert_eq!(
            parrot.reactor_dids,
            vec![String::from("did:plc:alice"), String::from("did:plc:bob")]
        );

        let fire = msg1.iter().find(|r| r.emoji == "🔥").unwrap();
        assert_eq!(fire.count, 1);

        let msg2 = grouped.get("msg-2").unwrap();
        assert_eq!(msg2.len(), 1);
        assert_eq!(msg2[0].emoji, "🔥");
        assert_eq!(msg2[0].reactor_dids, vec![String::from("did:plc:carol")]);
    }

    #[test]
    fn deduplicates_reactor_dids_for_same_emoji() {
        let records = vec![
            reaction("did:plc:alice", "r1", "msg-1", "🦜"),
            reaction("did:plc:alice", "r2", "msg-1", "🦜"),
        ];
        let grouped = group_reaction_records(records);
        let parrot = &grouped.get("msg-1").unwrap()[0];
        assert_eq!(parrot.count, 1);
        assert_eq!(parrot.reactor_dids, vec![String::from("did:plc:alice")]);
    }

    #[test]
    fn skips_records_with_invalid_payload() {
        let records = vec![record_data::Model {
            id: 0,
            did: String::from("did:plc:alice"),
            nsid: String::from("social.colibri.reaction"),
            rkey: String::from("r1"),
            data: serde_json::json!({ "irrelevant": true }),
        }];
        let grouped = group_reaction_records(records);
        assert!(grouped.is_empty());
    }
}
