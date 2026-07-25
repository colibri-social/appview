//! Read-side helper for fetching a `social.colibri.community` record by
//! (DID, rkey), with a PDS fallback when the local `record_data` cache hasn't
//! ingested it yet.
//!
//! Used by the firehose-driven join path: when a `social.colibri.membership`
//! event arrives for a brand-new community we haven't yet seen, we still need
//! to read `requiresApprovalToJoin` to decide whether to auto-write a
//! `social.colibri.member` record on the community side. Without the
//! fallback, joiners would race the firehose backfill.

use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde_json::Value;

use crate::lib::colibri::ColibriCommunity;
use crate::lib::pds_client::{self, PdsError};
use crate::lib::repo_endpoint::{self, EndpointError, RepoEndpoint};
use crate::models::record_data;

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

impl From<EndpointError> for CommunityRecordError {
    fn from(err: EndpointError) -> Self {
        match err {
            EndpointError::Db(e) => Self::Db(e),
            EndpointError::ResolveDid { did, message } => Self::ResolveDid { did, message },
            EndpointError::NoPdsService { did } => Self::NoPdsService { did },
        }
    }
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

    let endpoint = repo_endpoint::resolve(db, community_did).await?;

    let fetched = match &endpoint {
        RepoEndpoint::Trusted(_) => {
            pds_client::get_record_trusted(
                endpoint.as_str(),
                community_did,
                COMMUNITY_NSID,
                community_rkey,
            )
            .await?
        }
        RepoEndpoint::Untrusted(_) => {
            pds_client::get_record(
                endpoint.as_str(),
                community_did,
                COMMUNITY_NSID,
                community_rkey,
            )
            .await?
        }
    };
    let value: Value = match fetched {
        Some(v) => v,
        None => return Ok(None),
    };

    let parsed = serde_json::from_value::<ColibriCommunity>(value.clone())
        .map_err(|e| CommunityRecordError::Parse(e.to_string()))?;

    crate::lib::community_write::cache_upsert(
        db,
        community_did,
        COMMUNITY_NSID,
        community_rkey,
        value,
    )
    .await;

    Ok(Some(parsed))
}
