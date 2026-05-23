//! Read-side helper for fetching a `social.colibri.community` record by
//! (DID, rkey), with a PDS fallback when the local `record_data` cache hasn't
//! ingested it yet.
//!
//! Used by the firehose-driven join path: when a `social.colibri.membership`
//! event arrives for a brand-new community we haven't yet seen, we still need
//! to read `requiresApprovalToJoin` to decide whether to auto-write a
//! `social.colibri.member` record on the community side. Without the
//! fallback, joiners would race the firehose backfill.

use sea_orm::{ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde_json::Value;

use crate::lib::colibri::ColibriCommunity;
use crate::lib::pds_client::{self, PdsError};
use crate::models::record_data;
use crate::xrpc::com::atproto::identity::resolve_did;

const COMMUNITY_NSID: &str = "social.colibri.community";

#[derive(Debug, thiserror::Error)]
pub enum CommunityRecordError {
    #[error(transparent)]
    Db(#[from] DbErr),
    #[error("failed to resolve community DID {did}: {message}")]
    ResolveDid { did: String, message: String },
    #[error("community DID {did} has no atproto_pds service entry")]
    NoPdsService { did: String },
    #[error("pds get_record failed: {0}")]
    Pds(#[from] PdsError),
    #[error("failed to parse community record: {0}")]
    Parse(String),
}

/// Looks up the `social.colibri.community` record at `(community_did, rkey)`.
/// Checks the local `record_data` cache first, and on a miss falls back to a
/// direct `com.atproto.repo.getRecord` call against the community's PDS.
///
/// On a successful PDS-fallback fetch, the record is optimistically inserted
/// into `record_data` so subsequent reads hit the cache. Failures in that
/// best-effort insert are logged and ignored — the firehose will reconcile.
///
/// Returns `Ok(None)` only when the PDS explicitly says the record doesn't
/// exist (404 / `RecordNotFound`). Network / parsing failures propagate as
/// `Err` so callers can decide whether to retry or treat the join as
/// transiently unprocessable.
pub async fn fetch_community_record(
    db: &DatabaseConnection,
    community_did: &str,
    community_rkey: &str,
) -> Result<Option<ColibriCommunity>, CommunityRecordError> {
    if let Some(row) = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_did))
        .filter(record_data::Column::Nsid.eq(COMMUNITY_NSID))
        .filter(record_data::Column::Rkey.eq(community_rkey))
        .one(db)
        .await?
    {
        let parsed = serde_json::from_value::<ColibriCommunity>(row.data)
            .map_err(|e| CommunityRecordError::Parse(e.to_string()))?;
        return Ok(Some(parsed));
    }

    let did_doc = resolve_did(community_did).await.map_err(|e| {
        CommunityRecordError::ResolveDid {
            did: community_did.to_string(),
            message: e.body.into_inner().message,
        }
    })?;
    let pds_endpoint = did_doc
        .service
        .iter()
        .find(|s| s.service_type == "AtprotoPersonalDataServer")
        .map(|s| s.service_endpoint.clone())
        .ok_or_else(|| CommunityRecordError::NoPdsService {
            did: community_did.to_string(),
        })?;

    let value: Value =
        match pds_client::get_record(&pds_endpoint, community_did, COMMUNITY_NSID, community_rkey)
            .await?
        {
            Some(v) => v,
            None => return Ok(None),
        };

    let parsed = serde_json::from_value::<ColibriCommunity>(value.clone())
        .map_err(|e| CommunityRecordError::Parse(e.to_string()))?;

    let active = record_data::ActiveModel {
        did: ActiveValue::Set(community_did.to_string()),
        nsid: ActiveValue::Set(COMMUNITY_NSID.to_string()),
        rkey: ActiveValue::Set(community_rkey.to_string()),
        data: ActiveValue::Set(value),
        ..Default::default()
    };
    if let Err(e) = record_data::Entity::insert(active).exec(db).await {
        log::warn!(
            "optimistic cache insert failed for community {community_did}/{community_rkey}: {e} \
             (firehose will reconcile)"
        );
    }

    Ok(Some(parsed))
}
