use std::collections::HashSet;

use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder};

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriModeration, ColibriModerationSubject};
use crate::models::record_data;

pub const MODERATION_NSID: &str = "social.colibri.moderation";

pub const ACTION_BAN: &str = "ban";
pub const ACTION_UNBAN: &str = "unban";
pub const ACTION_HIDE_MESSAGE: &str = "hideMessage";
pub const ACTION_UNHIDE_MESSAGE: &str = "unhideMessage";
pub const ACTION_KICK: &str = "kick";

/// Generates a TID-like rkey (13 base32-sortable chars) based on the current
/// system time. Sufficient for cursor ordering; the AppView is the only writer.
pub fn generate_tid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    // Pack microseconds into 13 base32 chars (65 bits, we drop the high bit).
    let mut out = [0u8; 13];
    let mut value = micros & ((1u64 << 53) - 1);
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(value & 31) as usize];
        value >>= 5;
    }
    String::from_utf8(out.to_vec()).expect("ASCII characters")
}

/// Writes a `social.colibri.moderation` record onto the community repo.
pub async fn write_moderation_record(
    db: &DatabaseConnection,
    community: &AtUri,
    record: &ColibriModeration,
) -> Result<record_data::Model, DbErr> {
    let rkey = generate_tid();
    let data = serde_json::to_value(record).map_err(|e| DbErr::Custom(e.to_string()))?;

    let active = record_data::ActiveModel {
        did: sea_orm::ActiveValue::Set(community.authority.clone()),
        nsid: sea_orm::ActiveValue::Set(MODERATION_NSID.to_string()),
        rkey: sea_orm::ActiveValue::Set(rkey.clone()),
        data: sea_orm::ActiveValue::Set(data),
        ..Default::default()
    };

    record_data::Entity::insert(active).exec(db).await?;

    let written = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(MODERATION_NSID))
        .filter(record_data::Column::Rkey.eq(&rkey))
        .one(db)
        .await?
        .ok_or_else(|| DbErr::Custom(format!("inserted moderation record missing: {rkey}")))?;

    Ok(written)
}

/// Computes the set of DIDs currently banned in a community by replaying every
/// `ban`/`unban` moderation event in createdAt order.
pub async fn currently_banned_dids(
    db: &DatabaseConnection,
    community: &AtUri,
) -> Result<Vec<String>, DbErr> {
    let records = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(MODERATION_NSID))
        .order_by_asc(record_data::Column::Rkey)
        .all(db)
        .await?;

    let mut banned: HashSet<String> = HashSet::new();
    for record in records {
        let Ok(mod_record) = serde_json::from_value::<ColibriModeration>(record.data) else {
            continue;
        };
        let Some(did) = mod_record.subject.did else {
            continue;
        };
        match mod_record.action.as_str() {
            ACTION_BAN => {
                banned.insert(did);
            }
            ACTION_UNBAN => {
                banned.remove(&did);
            }
            _ => {}
        }
    }
    let mut result: Vec<String> = banned.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Returns true if the given user is currently banned in the community.
pub async fn is_user_banned(
    db: &DatabaseConnection,
    community: &AtUri,
    did: &str,
) -> Result<bool, DbErr> {
    let banned = currently_banned_dids(db, community).await?;
    Ok(banned.iter().any(|b| b == did))
}

/// Convenience constructor for building a moderation record payload.
pub fn moderation_record(
    action: &str,
    subject: ColibriModerationSubject,
    created_by: String,
    created_at: String,
    reason: Option<String>,
) -> ColibriModeration {
    ColibriModeration {
        record_type: Some(MODERATION_NSID.to_string()),
        action: action.to_string(),
        subject,
        reason,
        created_by,
        created_at,
    }
}

/// Computes the latest moderation action targeting a given (subject) DID. Used
/// for tests and finer-grained checks beyond the boolean `is_user_banned`.
#[cfg(test)]
pub fn latest_action_for_did(records: &[ColibriModeration], did: &str) -> Option<String> {
    records
        .iter()
        .filter(|r| {
            r.subject
                .did
                .as_ref()
                .map(|d| d == did)
                .unwrap_or(false)
        })
        .map(|r| r.action.clone())
        .next_back()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(action: &str, did: &str) -> ColibriModeration {
        ColibriModeration {
            record_type: Some(MODERATION_NSID.to_string()),
            action: action.to_string(),
            subject: ColibriModerationSubject {
                did: Some(did.to_string()),
                uri: None,
            },
            reason: None,
            created_by: String::from("did:plc:owner"),
            created_at: String::from("2026-05-13T00:00:00Z"),
        }
    }

    #[test]
    fn ban_then_unban_results_in_not_banned() {
        let log = vec![record("ban", "did:plc:alice"), record("unban", "did:plc:alice")];
        assert_eq!(
            latest_action_for_did(&log, "did:plc:alice"),
            Some(String::from("unban"))
        );
    }

    #[test]
    fn moderation_record_helper_sets_type() {
        let rec = moderation_record(
            ACTION_BAN,
            ColibriModerationSubject {
                did: Some(String::from("did:plc:alice")),
                uri: None,
            },
            String::from("did:plc:owner"),
            String::from("2026-05-13T00:00:00Z"),
            Some(String::from("spam")),
        );
        assert_eq!(rec.action, "ban");
        assert_eq!(rec.created_by, "did:plc:owner");
        assert_eq!(rec.record_type.as_deref(), Some(MODERATION_NSID));
        assert_eq!(rec.reason.as_deref(), Some("spam"));
    }

    #[test]
    fn generate_tid_yields_13_char_lowercase_ascii() {
        let t = generate_tid();
        assert_eq!(t.len(), 13);
        assert!(t.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
