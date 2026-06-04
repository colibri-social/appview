//! Shared helpers for writing records to a community's PDS repo.
//!
//! Every community-management write follows the same three-step pattern:
//! load stored credentials → create a PDS session → write the record.
//! The helpers here centralise that boilerplate so individual handlers only
//! describe *what* to write, not *how* to authenticate.
//!
//! All writes are accompanied by an optimistic local-cache update so the
//! issuer's own reads reflect the change immediately. The tap firehose
//! ingester will re-deliver the same record; the local cache's unique
//! `(did, nsid, rkey)` index makes that a no-op.

use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::community_credentials;
use crate::lib::crypto;
use crate::lib::pds_client::{self, PdsError};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;
use rocket::serde::json::Json;

// ---- Error helpers ---------------------------------------------------------

pub fn creds_err_to_db(e: community_credentials::CredentialsError) -> DbErr {
    DbErr::Custom(format!("credentials error: {e}"))
}

pub fn pds_err_to_db(e: PdsError) -> DbErr {
    DbErr::Custom(format!("pds write failed: {e}"))
}

pub fn not_found_error(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("NotFound"),
            message: message.into(),
        }),
    }
}

pub fn invalid_request(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: message.into(),
        }),
    }
}

// ---- Session helper --------------------------------------------------------

/// Loads stored credentials for `community_did`, creates a PDS session, and
/// returns `(pds_endpoint, access_jwt)` ready for immediate PDS calls.
pub async fn community_session(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<(String, String), DbErr> {
    let creds = community_credentials::load_credentials(db, crypto::master_key(), community_did)
        .await
        .map_err(creds_err_to_db)?
        .ok_or_else(|| {
            DbErr::Custom(format!(
                "no credentials registered for community {community_did}"
            ))
        })?;

    let session =
        pds_client::create_session(&creds.pds_endpoint, &creds.identifier, &creds.password)
            .await
            .map_err(pds_err_to_db)?;

    Ok((creds.pds_endpoint, session.access_jwt))
}

// ---- Record write helpers --------------------------------------------------

/// Creates a new record on `community_did`'s PDS. If `rkey` is `None` the PDS
/// generates a TID. Returns the rkey of the newly minted record.
pub async fn create_record(
    db: &DatabaseConnection,
    community_did: &str,
    nsid: &str,
    rkey: Option<&str>,
    data: Value,
) -> Result<String, DbErr> {
    let (endpoint, jwt) = community_session(db, community_did).await?;

    let record_ref =
        pds_client::create_record(&endpoint, &jwt, community_did, nsid, rkey, &data)
            .await
            .map_err(pds_err_to_db)?;

    let final_rkey = AtUri::parse(&record_ref.uri)
        .map(|u| u.rkey)
        .unwrap_or_else(|| rkey.unwrap_or("").to_string());

    cache_upsert(db, community_did, nsid, &final_rkey, data).await;
    Ok(final_rkey)
}

/// Overwrites an existing record on the community's PDS via `putRecord`.
/// Updates the local cache optimistically.
pub async fn put_record(
    db: &DatabaseConnection,
    community_did: &str,
    nsid: &str,
    rkey: &str,
    data: Value,
) -> Result<(), DbErr> {
    let (endpoint, jwt) = community_session(db, community_did).await?;

    pds_client::put_record(&endpoint, &jwt, community_did, nsid, rkey, &data)
        .await
        .map_err(pds_err_to_db)?;

    cache_upsert(db, community_did, nsid, rkey, data).await;
    Ok(())
}

/// Deletes a record from the community's PDS. Removes the local cache row.
pub async fn delete_record(
    db: &DatabaseConnection,
    community_did: &str,
    nsid: &str,
    rkey: &str,
) -> Result<(), DbErr> {
    let (endpoint, jwt) = community_session(db, community_did).await?;

    pds_client::delete_record(&endpoint, &jwt, community_did, nsid, rkey)
        .await
        .map_err(pds_err_to_db)?;

    cache_delete(db, community_did, nsid, rkey).await;
    Ok(())
}

// ---- Cache helpers ---------------------------------------------------------

/// Returns the cached `data` blob for `(did, nsid, rkey)`, or `None`.
pub async fn read_cached(
    db: &DatabaseConnection,
    did: &str,
    nsid: &str,
    rkey: &str,
) -> Result<Option<Value>, DbErr> {
    let row = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(did))
        .filter(record_data::Column::Nsid.eq(nsid))
        .filter(record_data::Column::Rkey.eq(rkey))
        .one(db)
        .await?;
    Ok(row.map(|r| r.data))
}

/// Upserts a row in the local `record_data` cache. Failures are logged but
/// not fatal — the firehose ingester reconciles asynchronously.
pub async fn cache_upsert(
    db: &DatabaseConnection,
    did: &str,
    nsid: &str,
    rkey: &str,
    data: Value,
) {
    let active = record_data::ActiveModel {
        did: ActiveValue::Set(did.to_string()),
        nsid: ActiveValue::Set(nsid.to_string()),
        rkey: ActiveValue::Set(rkey.to_string()),
        data: ActiveValue::Set(data),
        ..Default::default()
    };

    if let Err(e) = record_data::Entity::insert(active)
        .on_conflict(
            sea_query::OnConflict::columns([
                record_data::Column::Did,
                record_data::Column::Nsid,
                record_data::Column::Rkey,
            ])
            .update_column(record_data::Column::Data)
            .to_owned(),
        )
        .exec(db)
        .await
    {
        log::warn!(
            "optimistic cache upsert failed for {did}/{nsid}/{rkey}: {e} (firehose will reconcile)"
        );
    }
}

/// Removes a row from the local `record_data` cache. Failures are logged.
pub async fn cache_delete(db: &DatabaseConnection, did: &str, nsid: &str, rkey: &str) {
    if let Err(e) = record_data::Entity::delete_many()
        .filter(record_data::Column::Did.eq(did))
        .filter(record_data::Column::Nsid.eq(nsid))
        .filter(record_data::Column::Rkey.eq(rkey))
        .exec(db)
        .await
    {
        log::warn!("cache delete failed for {did}/{nsid}/{rkey}: {e}");
    }
}
