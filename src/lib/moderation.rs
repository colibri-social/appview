use std::collections::HashSet;

use futures::future::BoxFuture;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder, prelude::Expr,
};

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriMember, ColibriModeration, ColibriModerationSubject};
use crate::lib::community_credentials::{self, CredentialsError};
use crate::lib::crypto;
use crate::lib::pds_client::{self, PdsError};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data;

pub const MEMBER_NSID: &str = "social.colibri.member";

/// Trait-object alias for the moderation write seam.
///
/// Handlers take `&WriteRecordFn` so production wires
/// [`write_moderation_boxed`] (which talks to the community's PDS) while
/// tests inject a closure that captures the would-be record for assertion.
pub type WriteRecordFn = dyn Fn(
        DatabaseConnection,
        AtUri,
        ColibriModeration,
    ) -> BoxFuture<'static, Result<record_data::Model, DbErr>>
    + Send
    + Sync;

/// Production [`WriteRecordFn`] — forwards to [`write_moderation_record`].
pub fn write_moderation_boxed(
    db: DatabaseConnection,
    community: AtUri,
    record: ColibriModeration,
) -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
    Box::pin(async move { write_moderation_record(&db, &community, &record).await })
}

/// Trait-object alias for the member-revocation seam — used to delete a
/// `social.colibri.member` record from the community's PDS when a member is
/// banned, kicked, or leaves on their own. Returns `true` if a record was
/// actually deleted, `false` if no member record was found for the subject.
pub type RevokeMemberFn = dyn Fn(
        DatabaseConnection,
        /* community_did */ String,
        /* subject_did */ String,
    ) -> BoxFuture<'static, Result<bool, DbErr>>
    + Send
    + Sync;

/// Production [`RevokeMemberFn`] — forwards to [`revoke_community_member`].
pub fn revoke_member_boxed(
    db: DatabaseConnection,
    community_did: String,
    subject_did: String,
) -> BoxFuture<'static, Result<bool, DbErr>> {
    Box::pin(async move { revoke_community_member(&db, &community_did, &subject_did).await })
}

pub const MODERATION_NSID: &str = "social.colibri.moderation";

pub const ACTION_BAN: &str = "ban";
pub const ACTION_UNBAN: &str = "unban";
pub const ACTION_HIDE_MESSAGE: &str = "hideMessage";
/// Placeholder for an `unhideMessage` endpoint that isn't wired up yet.
#[allow(dead_code)]
pub const ACTION_UNHIDE_MESSAGE: &str = "unhideMessage";
pub const ACTION_KICK: &str = "kick";
/// System-issued audit entry recorded when a currently-banned user writes a
/// `social.colibri.membership` targeting this community. The AppView refuses
/// to write the matching `member` record; this entry exists so moderators can
/// see the attempt in the audit feed. `created_by` is the community DID
/// (the action is automated, not moderator-driven).
pub const ACTION_BLOCKED_JOIN: &str = "blockedJoin";

/// Generates an atproto TID rkey: 13 base32-sortable chars encoding a 64-bit
/// big-endian integer laid out as `[reserved=0 | micros(53) | clock_id(10)]`.
/// The reserved high bit is always 0; `clock_id` is randomized per call so
/// near-simultaneous calls on the same clock-microsecond don't collide.
pub fn generate_tid() -> String {
    use rand::Rng;
    use std::time::{SystemTime, UNIX_EPOCH};

    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
        & ((1u64 << 53) - 1);
    let clock_id: u64 = rand::thread_rng().gen_range(0..(1u64 << 10));
    let mut value = (micros << 10) | clock_id;
    let mut out = [0u8; 13];
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(value & 31) as usize];
        value >>= 5;
    }
    String::from_utf8(out.to_vec()).expect("ASCII characters")
}

/// Writes a `social.colibri.moderation` record onto the community repo.
///
/// The call goes through the community's PDS using stored credentials (see
/// `community_credentials`). After the PDS write, we optimistically insert
/// into the local `record_data` cache so the issuer's own queries reflect the
/// change immediately — the upstream firehose ingester will eventually
/// re-deliver the same record, and the local cache has a unique
/// `(did, nsid, rkey)` index that makes the duplicate a no-op.
///
/// On local-cache failure we log and return the synthesised row anyway: the
/// firehose path will reconcile.
pub async fn write_moderation_record(
    db: &DatabaseConnection,
    community: &AtUri,
    record: &ColibriModeration,
) -> Result<record_data::Model, DbErr> {
    let data = serde_json::to_value(record).map_err(|e| DbErr::Custom(e.to_string()))?;

    let creds =
        community_credentials::load_credentials(db, crypto::master_key(), &community.authority)
            .await
            .map_err(credentials_error_to_db_err)?
            .ok_or_else(|| {
                DbErr::Custom(format!(
                    "no credentials registered for community {}",
                    community.authority
                ))
            })?;

    let session =
        pds_client::create_session(&creds.pds_endpoint, &creds.identifier, &creds.password)
            .await
            .map_err(pds_error_to_db_err)?;

    let record_ref = pds_client::create_record(
        &creds.pds_endpoint,
        &session.access_jwt,
        &community.authority,
        MODERATION_NSID,
        None,
        &data,
    )
    .await
    .map_err(pds_error_to_db_err)?;

    let parsed = AtUri::parse(&record_ref.uri).ok_or_else(|| {
        DbErr::Custom(format!(
            "pds returned malformed record URI: {}",
            record_ref.uri
        ))
    })?;
    let rkey = parsed.rkey;

    // Best-effort optimistic local insert. Failures here are logged but not
    // fatal; the firehose ingester re-delivers the record asynchronously.
    let active = record_data::ActiveModel {
        did: sea_orm::ActiveValue::Set(community.authority.clone()),
        nsid: sea_orm::ActiveValue::Set(MODERATION_NSID.to_string()),
        rkey: sea_orm::ActiveValue::Set(rkey.clone()),
        data: sea_orm::ActiveValue::Set(data.clone()),
        ..Default::default()
    };
    if let Err(e) = record_data::Entity::insert(active).exec(db).await {
        log::warn!(
            "optimistic local insert failed for moderation {}/{}: {} (firehose will reconcile)",
            community.authority,
            rkey,
            e
        );
    }

    // If the optimistic insert just landed, re-read to return the authoritative
    // row; otherwise synthesise a model with whatever metadata we have so the
    // caller still gets a `Model` back.
    if let Some(row) = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(MODERATION_NSID))
        .filter(record_data::Column::Rkey.eq(&rkey))
        .one(db)
        .await?
    {
        return Ok(row);
    }

    Ok(record_data::Model {
        id: 0,
        did: community.authority.clone(),
        nsid: MODERATION_NSID.to_string(),
        rkey,
        data,
    })
}

fn credentials_error_to_db_err(e: CredentialsError) -> DbErr {
    match e {
        CredentialsError::Db(inner) => inner,
        other => DbErr::Custom(format!("credentials error: {other}")),
    }
}

fn pds_error_to_db_err(e: PdsError) -> DbErr {
    DbErr::Custom(format!("pds write failed: {e}"))
}

/// Computes the set of DIDs currently banned in a community by replaying every
/// `ban`/`unban` moderation event in createdAt order.
pub async fn currently_banned_dids(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<Vec<String>, DbErr> {
    let records = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_did))
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
    community_did: &str,
    did: &str,
) -> Result<bool, DbErr> {
    let banned = currently_banned_dids(db, community_did).await?;
    Ok(banned.iter().any(|b| b == did))
}

/// One-call entry point for issuing a moderation action. Builds the
/// `social.colibri.moderation` payload (filling in `createdAt` with the
/// current UTC clock), then hands it to `write_record_fn` for persistence.
///
/// This is the only function moderation handlers should call. Centralizing
/// the build+write pair here means the on-protocol write path (PDS call,
/// optimistic local insert, error mapping) lives in one place — handlers
/// just describe the action they want issued.
pub async fn issue_action(
    write_record_fn: &WriteRecordFn,
    db: DatabaseConnection,
    community: AtUri,
    action: &str,
    subject: ColibriModerationSubject,
    created_by: String,
    reason: Option<String>,
) -> Result<record_data::Model, DbErr> {
    let record = moderation_record(action, subject, created_by, current_iso8601_utc(), reason);
    write_record_fn(db, community, record).await
}

/// Looks up the rkey of the `social.colibri.member` record for `subject_did`
/// in the given community. Returns `None` if no member record exists yet
/// (e.g. the subject was never admitted).
pub async fn find_member_rkey(
    db: &DatabaseConnection,
    community_did: &str,
    subject_did: &str,
) -> Result<Option<String>, DbErr> {
    let row = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_did))
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(subject_did.to_string())],
        ))
        .one(db)
        .await?;
    Ok(row.map(|r| r.rkey))
}

/// Deletes the `social.colibri.member` record for `subject_did` from the
/// community's PDS. Used on ban, kick, and self-leave to revoke the
/// community-side role record after the moderation/state event has been
/// recorded. Returns `Ok(true)` if a record was actually deleted,
/// `Ok(false)` if no member record was found (already gone or never existed).
///
/// The local `record_data` row is not modified here — the firehose ingester
/// will reconcile via the upstream member-delete event. (For now, the tap
/// ingester also short-circuits delete-action processing for `member` and
/// `membership` NSIDs to actually drop the row, partially working around the
/// generic-delete bug in `ack_tap_msg`.)
pub async fn revoke_community_member(
    db: &DatabaseConnection,
    community_did: &str,
    subject_did: &str,
) -> Result<bool, DbErr> {
    let Some(member_rkey) = find_member_rkey(db, community_did, subject_did).await? else {
        return Ok(false);
    };

    let creds = community_credentials::load_credentials(db, crypto::master_key(), community_did)
        .await
        .map_err(credentials_error_to_db_err)?
        .ok_or_else(|| {
            DbErr::Custom(format!(
                "no credentials registered for community {community_did}"
            ))
        })?;

    let session =
        pds_client::create_session(&creds.pds_endpoint, &creds.identifier, &creds.password)
            .await
            .map_err(pds_error_to_db_err)?;

    pds_client::delete_record(
        &creds.pds_endpoint,
        &session.access_jwt,
        community_did,
        MEMBER_NSID,
        &member_rkey,
    )
    .await
    .map_err(pds_error_to_db_err)?;

    Ok(true)
}

/// Writes a `social.colibri.member` record on the community's PDS, admitting
/// `subject_did` to the community. Mirror of [`revoke_community_member`].
///
/// Idempotent: if a member record already exists for `subject_did`, this
/// returns `Ok(None)` without writing. Callers that need to know whether a
/// new record was actually minted should branch on the return value.
///
/// The write goes through stored community credentials (see
/// [`community_credentials`]); on success the new row is optimistically
/// inserted into the local `record_data` cache so the issuer's own reads
/// reflect the change immediately — the firehose ingester will redeliver
/// the same record, and the local cache's unique `(did, nsid, rkey)` index
/// makes that a no-op.
pub async fn write_member_record(
    db: &DatabaseConnection,
    community_did: &str,
    subject_did: &str,
    role_rkeys: Vec<String>,
    from_membership: Option<String>,
) -> Result<Option<record_data::Model>, DbErr> {
    if find_member_rkey(db, community_did, subject_did).await?.is_some() {
        return Ok(None);
    }

    let record = ColibriMember {
        record_type: Some(MEMBER_NSID.to_string()),
        subject: subject_did.to_string(),
        roles: role_rkeys,
        joined_at: current_iso8601_utc(),
        nickname: None,
        from_membership,
    };
    let data = serde_json::to_value(&record).map_err(|e| DbErr::Custom(e.to_string()))?;

    let creds = community_credentials::load_credentials(db, crypto::master_key(), community_did)
        .await
        .map_err(credentials_error_to_db_err)?
        .ok_or_else(|| {
            DbErr::Custom(format!(
                "no credentials registered for community {community_did}"
            ))
        })?;

    let session =
        pds_client::create_session(&creds.pds_endpoint, &creds.identifier, &creds.password)
            .await
            .map_err(pds_error_to_db_err)?;

    let record_ref = pds_client::create_record(
        &creds.pds_endpoint,
        &session.access_jwt,
        community_did,
        MEMBER_NSID,
        None,
        &data,
    )
    .await
    .map_err(pds_error_to_db_err)?;

    let parsed = AtUri::parse(&record_ref.uri).ok_or_else(|| {
        DbErr::Custom(format!(
            "pds returned malformed record URI: {}",
            record_ref.uri
        ))
    })?;
    let rkey = parsed.rkey;

    let active = record_data::ActiveModel {
        did: sea_orm::ActiveValue::Set(community_did.to_string()),
        nsid: sea_orm::ActiveValue::Set(MEMBER_NSID.to_string()),
        rkey: sea_orm::ActiveValue::Set(rkey.clone()),
        data: sea_orm::ActiveValue::Set(data.clone()),
        ..Default::default()
    };
    if let Err(e) = record_data::Entity::insert(active).exec(db).await {
        log::warn!(
            "optimistic local insert failed for member {community_did}/{rkey}: {e} \
             (firehose will reconcile)"
        );
    }

    if let Some(row) = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_did))
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(record_data::Column::Rkey.eq(&rkey))
        .one(db)
        .await?
    {
        return Ok(Some(row));
    }

    Ok(Some(record_data::Model {
        id: 0,
        did: community_did.to_string(),
        nsid: MEMBER_NSID.to_string(),
        rkey,
        data,
    }))
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
        .filter(|r| r.subject.did.as_ref().map(|d| d == did).unwrap_or(false))
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
        let log = vec![
            record("ban", "did:plc:alice"),
            record("unban", "did:plc:alice"),
        ];
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

    /// Regression: TIDs used to put the microseconds in the low bits of the
    /// 64-bit value instead of bits 62..10, so parsers decoded them as
    /// ~January 1970. This test decodes a freshly minted TID with the atproto
    /// layout (`micros = value >> 10`) and confirms the timestamp matches the
    /// current wall clock within a couple of seconds.
    #[test]
    fn generate_tid_encodes_current_microseconds_in_atproto_layout() {
        use std::time::{SystemTime, UNIX_EPOCH};

        const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let tid = generate_tid();
        let mut value: u64 = 0;
        for c in tid.bytes() {
            let idx = ALPHABET
                .iter()
                .position(|&a| a == c)
                .expect("TID must use the base32-sortable alphabet") as u64;
            value = (value << 5) | idx;
        }
        // Reserved high bit must be zero.
        assert_eq!(value >> 63, 0, "TID reserved high bit must be 0");
        let decoded_micros = value >> 10;

        // Tolerate a couple of seconds of skew between the two wall-clock
        // reads + any system jitter.
        let skew = decoded_micros.abs_diff(now_micros);
        assert!(
            skew < 2_000_000,
            "TID timestamp {decoded_micros} differs from now {now_micros} by {skew} µs"
        );
    }
}
