//! `social.colibri.community.migrate` — REMOVABLE MIGRATION SCAFFOLDING.
//!
//! Migrates a legacy (pre-rework) community into a fresh community on a new
//! DID, using the same managed-vs-BYO provisioning as `community.create`:
//!
//! - reads the legacy community's structure (categories + channels) from the
//!   local `record_data` index,
//! - provisions a target repo (managed = mint a DID; BYO = supplied creds),
//! - clones the structure onto it, stamping each new channel with
//!   `migratedFrom` (its legacy channel AT-URI) and the community with
//!   `migratedFrom` (the legacy community AT-URI),
//! - imports every legacy member (anyone with a `social.colibri.membership`
//!   record pointing at the legacy community), bypassing the approval queue.
//!
//! The legacy community record itself is stamped with `migratedTo` by the
//! *client* afterwards — the AppView holds no credentials for the owner's
//! personal DID where the legacy community lives.
//!
//! The `kind` discriminator lets this endpoint host future migrations; today
//! only `"legacy-community"` is handled.

use std::collections::{HashMap, HashSet};

use futures::future::BoxFuture;
use rocket::data::{Data, ToByteUnit};
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::Serialize;
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{
    ColibriActorData, ColibriCategory, ColibriChannel, ColibriCommunity, ColibriMember, ColibriRole,
};
use crate::lib::community_credentials::{self, SOURCE_APPVIEW_MANAGED, SOURCE_BYO};
use crate::lib::crypto;
use crate::lib::moderation::generate_tid;
use crate::lib::pds_client::{self, PdsError, RecordRef};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data;

const COMMUNITY_NSID: &str = "social.colibri.community";
const CATEGORY_NSID: &str = "social.colibri.category";
const CHANNEL_NSID: &str = "social.colibri.channel";
const MEMBERSHIP_NSID: &str = "social.colibri.membership";
const COMMUNITY_RKEY: &str = "self";
const SUPPORTED_KIND: &str = "legacy-community";

const ALLOWED_PICTURE_MIME_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif"];
const MAX_PICTURE_MEBIBYTES: i64 = 10;

// ---- Wire types ---------------------------------------------------------

#[derive(Serialize, Debug, PartialEq)]
pub struct ChannelMapping {
    pub old: String,
    pub new: String,
}

#[derive(Serialize, Debug)]
pub struct MigrateResponse {
    pub did: String,
    pub community: String,
    #[serde(rename = "channelMap")]
    pub channel_map: Vec<ChannelMapping>,
}

#[derive(Debug, Clone)]
pub struct MigrateInput {
    pub kind: String,
    pub source: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub requires_approval_to_join: Option<bool>,
    pub picture: Option<Vec<u8>>,
    pub mime_type: Option<String>,
}

// ---- Intermediate types -------------------------------------------------

/// The legacy community's structure, read out of the local index.
#[derive(Debug, Clone)]
pub struct OldStructure {
    pub community: ColibriCommunity,
    /// `(legacy_rkey, record)` pairs.
    pub categories: Vec<(String, ColibriCategory)>,
    pub channels: Vec<(String, ColibriChannel)>,
}

/// A legacy member: their DID plus the membership declaration we admitted them
/// from (kept for the `member.fromMembership` audit trail).
#[derive(Debug, Clone)]
pub struct LegacyMember {
    pub did: String,
    pub membership_uri: String,
}

/// A repo we may write the migrated community onto.
#[derive(Debug, Clone)]
pub struct ProvisionedTarget {
    pub did: String,
    pub pds_endpoint: String,
    pub access_jwt: String,
}

/// The cloned structure, ready to write. Rkeys are freshly generated.
#[derive(Debug)]
pub struct PlannedStructure {
    pub community: ColibriCommunity,
    /// `(new_rkey, record)` pairs.
    pub categories: Vec<(String, ColibriCategory)>,
    pub channels: Vec<(String, ColibriChannel)>,
    pub channel_map: Vec<ChannelMapping>,
}

// ---- Dependency seams (mirrors create_handler's testing approach) -------

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type ProvisionFn =
    dyn Fn() -> BoxFuture<'static, Result<ProvisionedTarget, ErrorResponse>> + Send + Sync;
type FetchStructureFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Option<OldStructure>, ErrorResponse>> + Send + Sync;
type FetchMembersFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Vec<LegacyMember>, ErrorResponse>> + Send + Sync;
type CreateRecordFn = dyn Fn(
        String,
        String,
        String,
        String,
        Option<String>,
        Value,
    ) -> BoxFuture<'static, Result<RecordRef, PdsError>>
    + Send
    + Sync;
type UploadBlobFn = dyn Fn(String, String, Vec<u8>, String) -> BoxFuture<'static, Result<Value, PdsError>>
    + Send
    + Sync;
type RegisterDidsFn = dyn Fn(Vec<String>) -> BoxFuture<'static, ()> + Send + Sync;

// ---- Pure structure planner (unit-tested) -------------------------------

/// Clones a legacy community's structure into records for the new repo,
/// generating fresh rkeys and rebuilding all cross-references against them.
///
/// - each new channel gets `migratedFrom` = its legacy channel AT-URI, which
///   the read-time remap keys off to surface old messages;
/// - the new community gets `migratedFrom` = the legacy community AT-URI;
/// - legacy per-channel role/member allow-lists are dropped (their rkeys refer
///   to roles that don't exist in the new repo — legacy communities had none).
pub fn plan_structure(
    new_did: &str,
    source_uri: &str,
    source_authority: &str,
    old: &OldStructure,
    input: &MigrateInput,
) -> PlannedStructure {
    // Map every legacy rkey to a freshly generated one up front so all
    // cross-references can be rewritten deterministically below.
    let cat_rkey_map: HashMap<String, String> = old
        .categories
        .iter()
        .map(|(old_rkey, _)| (old_rkey.clone(), generate_tid()))
        .collect();
    let chan_rkey_map: HashMap<String, String> = old
        .channels
        .iter()
        .map(|(old_rkey, _)| (old_rkey.clone(), generate_tid()))
        .collect();

    let channel_map = old
        .channels
        .iter()
        .filter_map(|(old_rkey, _)| {
            let new_rkey = chan_rkey_map.get(old_rkey)?;
            Some(ChannelMapping {
                old: format!("at://{source_authority}/{CHANNEL_NSID}/{old_rkey}"),
                new: format!("at://{new_did}/{CHANNEL_NSID}/{new_rkey}"),
            })
        })
        .collect();

    let channels = old
        .channels
        .iter()
        .filter_map(|(old_rkey, chan)| {
            let new_rkey = chan_rkey_map.get(old_rkey)?.clone();
            // Skip channels whose category didn't survive (shouldn't happen).
            let new_category = cat_rkey_map.get(&chan.category)?.clone();
            let record = ColibriChannel {
                r#type: String::from(CHANNEL_NSID),
                name: chan.name.clone(),
                description: chan.description.clone(),
                channel_type: chan.channel_type.clone(),
                category: new_category,
                community: COMMUNITY_RKEY.to_string(),
                owner_only: chan.owner_only,
                allowed_roles: vec![],
                allowed_members: vec![],
                migrated_from: Some(format!("at://{source_authority}/{CHANNEL_NSID}/{old_rkey}")),
            };
            Some((new_rkey, record))
        })
        .collect::<Vec<_>>();

    let categories = old
        .categories
        .iter()
        .filter_map(|(old_rkey, cat)| {
            let new_rkey = cat_rkey_map.get(old_rkey)?.clone();
            let channel_order = cat
                .channel_order
                .iter()
                .filter_map(|c| chan_rkey_map.get(c).cloned())
                .collect();
            let record = ColibriCategory {
                r#type: String::from(CATEGORY_NSID),
                name: cat.name.clone(),
                channel_order,
                community: COMMUNITY_RKEY.to_string(),
            };
            Some((new_rkey, record))
        })
        .collect::<Vec<_>>();

    // Category order: the legacy order remapped, with any categories missing
    // from it appended so none are lost.
    let mut category_order: Vec<String> = old
        .community
        .category_order
        .iter()
        .filter_map(|c| cat_rkey_map.get(c).cloned())
        .collect();
    for (new_rkey, _) in &categories {
        if !category_order.contains(new_rkey) {
            category_order.push(new_rkey.clone());
        }
    }

    let community = ColibriCommunity {
        r#type: String::from(COMMUNITY_NSID),
        name: input
            .name
            .clone()
            .unwrap_or_else(|| old.community.name.clone()),
        description: input
            .description
            .clone()
            .unwrap_or_else(|| old.community.description.clone()),
        category_order,
        requires_approval_to_join: input
            .requires_approval_to_join
            .unwrap_or(old.community.requires_approval_to_join),
        picture: None,
        migrated_to: None,
        migrated_from: Some(source_uri.to_string()),
        appview: Some(crate::lib::service_auth::appview_did()),
    };

    PlannedStructure {
        community,
        categories,
        channels,
        channel_map,
    }
}

// ---- Orchestrator (testable via seams) ----------------------------------

#[allow(clippy::too_many_arguments)]
async fn migrate_with(
    auth: String,
    input: MigrateInput,
    verify_auth_fn: &VerifyAuthFn,
    provision_fn: &ProvisionFn,
    fetch_structure_fn: &FetchStructureFn,
    fetch_members_fn: &FetchMembersFn,
    create_record_fn: &CreateRecordFn,
    upload_blob_fn: &UploadBlobFn,
    register_dids_fn: &RegisterDidsFn,
) -> Result<Json<MigrateResponse>, ErrorResponse> {
    let mig_id = generate_tid();

    if input.kind != SUPPORTED_KIND {
        return Err(invalid_request(format!(
            "unsupported migration kind `{}`. Supported: {SUPPORTED_KIND}.",
            input.kind
        )));
    }

    let caller_did = verify_auth_fn(auth, String::from("social.colibri.community.migrate"))
        .await
        .map_err(auth_error)?;

    let source = AtUri::parse(&input.source)
        .ok_or_else(|| invalid_request(String::from("invalid source AT-URI")))?;

    log::info!(
        "[migrate {mig_id}] kind={} source={} caller={caller_did}",
        input.kind,
        input.source
    );

    // Owner-only: the legacy community lives on the owner's own repo, so the
    // caller must be that repo's DID.
    if source.authority != caller_did {
        return Err(forbidden(String::from(
            "only the owner of the legacy community may migrate it",
        )));
    }

    let old = fetch_structure_fn(input.source.clone())
        .await?
        .ok_or_else(|| not_found(String::from("legacy community not found")))?;

    if old.community.migrated_to.is_some() {
        return Err(invalid_request(String::from(
            "this community has already been migrated",
        )));
    }

    log::info!(
        "[migrate {mig_id}] legacy structure: {} categories, {} channels",
        old.categories.len(),
        old.channels.len()
    );

    let target = provision_fn().await?;
    log::info!(
        "[migrate {mig_id}] provisioned target did={} pds={}",
        target.did,
        target.pds_endpoint
    );

    let picture_blob = upload_picture(&input, &target, upload_blob_fn, &mig_id).await?;

    let mut planned = plan_structure(&target.did, &input.source, &source.authority, &old, &input);
    planned.community.picture = picture_blob;

    // 1. Community (singleton at "self").
    let community_ref = write_record(
        create_record_fn,
        &target,
        COMMUNITY_NSID,
        Some(COMMUNITY_RKEY.to_string()),
        &planned.community,
        &mig_id,
        "community",
    )
    .await?;

    // 2. Categories.
    for (rkey, record) in &planned.categories {
        write_record(
            create_record_fn,
            &target,
            CATEGORY_NSID,
            Some(rkey.clone()),
            record,
            &mig_id,
            "category",
        )
        .await?;
    }

    // 3. Channels.
    for (rkey, record) in &planned.channels {
        write_record(
            create_record_fn,
            &target,
            CHANNEL_NSID,
            Some(rkey.clone()),
            record,
            &mig_id,
            "channel",
        )
        .await?;
    }

    // 4. Owner role (protected, full permission catalog).
    let owner_role_rkey = generate_tid();
    let owner_role = ColibriRole {
        record_type: Some(String::from("social.colibri.role")),
        name: String::from("Owner"),
        color: None,
        permissions: Permission::all()
            .iter()
            .map(|p| p.as_str().to_string())
            .collect(),
        position: 100,
        hoisted: Some(true),
        mentionable: Some(false),
        protected: Some(true),
        channel_overrides: vec![],
    };
    write_record(
        create_record_fn,
        &target,
        "social.colibri.role",
        Some(owner_role_rkey.clone()),
        &owner_role,
        &mig_id,
        "role",
    )
    .await?;

    // 5. Owner member (the migrating caller).
    let owner_member = ColibriMember {
        record_type: Some(String::from("social.colibri.member")),
        subject: caller_did.clone(),
        roles: vec![owner_role_rkey],
        joined_at: current_iso8601_utc(),
        nickname: None,
        from_membership: None,
    };
    write_record(
        create_record_fn,
        &target,
        "social.colibri.member",
        Some(generate_tid()),
        &owner_member,
        &mig_id,
        "member",
    )
    .await?;

    // 6. Import every legacy member (bypassing approval). The owner is already
    //    a member above, so skip any self-membership.
    let legacy_members = fetch_members_fn(input.source.clone()).await?;
    let mut imported = 0usize;
    let mut seen: HashSet<String> = HashSet::from([caller_did.clone()]);
    for member in legacy_members {
        if !seen.insert(member.did.clone()) {
            continue;
        }
        let record = ColibriMember {
            record_type: Some(String::from("social.colibri.member")),
            subject: member.did.clone(),
            roles: vec![],
            joined_at: current_iso8601_utc(),
            nickname: None,
            from_membership: Some(member.membership_uri),
        };
        match write_record(
            create_record_fn,
            &target,
            "social.colibri.member",
            Some(generate_tid()),
            &record,
            &mig_id,
            "imported-member",
        )
        .await
        {
            Ok(_) => imported += 1,
            Err(e) => {
                // A single member failing shouldn't abort the whole migration.
                log::error!(
                    "[migrate {mig_id}] failed to import member {}: {}",
                    member.did,
                    e.body.0.message
                );
            }
        }
    }

    // 7. Empty actor.data on the community repo (a stable "this DID is a
    //    Colibri community" signal, matching create).
    let actor_data = ColibriActorData {
        record_type: Some(String::from("social.colibri.actor.data")),
        emoji: Some(String::new()),
        status: Some(String::new()),
        communities: vec![],
    };
    write_record(
        create_record_fn,
        &target,
        "social.colibri.actor.data",
        Some(String::from("self")),
        &actor_data,
        &mig_id,
        "actor.data",
    )
    .await?;

    register_dids_fn(vec![target.did.clone()]).await;

    log::info!(
        "[migrate {mig_id}] complete: did={} categories={} channels={} members_imported={}",
        target.did,
        planned.categories.len(),
        planned.channels.len(),
        imported
    );

    Ok(Json(MigrateResponse {
        did: target.did,
        community: community_ref.uri,
        channel_map: planned.channel_map,
    }))
}

async fn upload_picture(
    input: &MigrateInput,
    target: &ProvisionedTarget,
    upload_blob_fn: &UploadBlobFn,
    mig_id: &str,
) -> Result<Option<Value>, ErrorResponse> {
    match (input.picture.as_deref(), input.mime_type.as_deref()) {
        (Some(bytes), Some(mime)) => {
            if !ALLOWED_PICTURE_MIME_TYPES.contains(&mime) {
                return Err(invalid_request(format!(
                    "Unsupported picture mimeType `{mime}`. Accepted: {}.",
                    ALLOWED_PICTURE_MIME_TYPES.join(", ")
                )));
            }
            let blob = upload_blob_fn(
                target.pds_endpoint.clone(),
                target.access_jwt.clone(),
                bytes.to_vec(),
                mime.to_string(),
            )
            .await
            .map_err(|e| {
                log::error!("[migrate {mig_id}] uploadBlob failed: {e}");
                pds_error(format!("uploadBlob failed: {e}"))
            })?;
            Ok(Some(blob))
        }
        (Some(_), None) => Err(invalid_request(String::from(
            "`mimeType` is required when a picture body is supplied.",
        ))),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_record<T: Serialize>(
    create_record_fn: &CreateRecordFn,
    target: &ProvisionedTarget,
    collection: &'static str,
    rkey: Option<String>,
    record: &T,
    mig_id: &str,
    label: &'static str,
) -> Result<RecordRef, ErrorResponse> {
    let value = serde_json::to_value(record)
        .map_err(|e| internal_error(format!("serialize {label}: {e}")))?;
    create_record_fn(
        target.pds_endpoint.clone(),
        target.access_jwt.clone(),
        target.did.clone(),
        collection.to_string(),
        rkey,
        value,
    )
    .await
    .map_err(|e| {
        log::error!("[migrate {mig_id}] write_record({label}) failed: {e}");
        pds_error(format!("createRecord({label}) failed: {e}"))
    })
}

// ---- Error helpers ------------------------------------------------------

fn auth_error(err: ServiceAuthError) -> ErrorResponse {
    error_response("AuthError", err.to_string())
}
fn pds_error(message: String) -> ErrorResponse {
    error_response("UpstreamError", message)
}
fn internal_error(message: String) -> ErrorResponse {
    error_response("InternalServerError", message)
}
fn invalid_request(message: String) -> ErrorResponse {
    error_response("InvalidRequest", message)
}
fn forbidden(message: String) -> ErrorResponse {
    error_response("Forbidden", message)
}
fn not_found(message: String) -> ErrorResponse {
    error_response("NotFound", message)
}
fn error_response(error: &str, message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from(error),
            message,
        }),
    }
}

// ---- Production dependency implementations ------------------------------

async fn fetch_old_structure_prod(
    db: DatabaseConnection,
    source_uri: String,
) -> Result<Option<OldStructure>, ErrorResponse> {
    let source = AtUri::parse(&source_uri)
        .ok_or_else(|| invalid_request(String::from("invalid source AT-URI")))?;

    let db_err = |e: sea_orm::DbErr| internal_error(format!("db error: {e}"));

    let community_record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&source.authority))
        .filter(record_data::Column::Nsid.eq(COMMUNITY_NSID))
        .filter(record_data::Column::Rkey.eq(&source.rkey))
        .one(&db)
        .await
        .map_err(db_err)?;

    let Some(community_record) = community_record else {
        return Ok(None);
    };
    let community = serde_json::from_value::<ColibriCommunity>(community_record.data)
        .map_err(|e| internal_error(format!("parse legacy community: {e}")))?;

    // Categories/channels reference the community by its (legacy) rkey.
    let by_community = |nsid: &'static str| {
        record_data::Entity::find()
            .filter(record_data::Column::Did.eq(source.authority.clone()))
            .filter(record_data::Column::Nsid.eq(nsid))
            .filter(Expr::cust_with_values(
                r#""record_data"."data"->>'community' = $1"#,
                vec![sea_orm::Value::from(source.rkey.clone())],
            ))
    };

    let categories = by_community(CATEGORY_NSID)
        .all(&db)
        .await
        .map_err(db_err)?
        .into_iter()
        .filter_map(|r| {
            let cat = serde_json::from_value::<ColibriCategory>(r.data).ok()?;
            Some((r.rkey, cat))
        })
        .collect();

    let channels = by_community(CHANNEL_NSID)
        .all(&db)
        .await
        .map_err(db_err)?
        .into_iter()
        .filter_map(|r| {
            let chan = serde_json::from_value::<ColibriChannel>(r.data).ok()?;
            Some((r.rkey, chan))
        })
        .collect();

    Ok(Some(OldStructure {
        community,
        categories,
        channels,
    }))
}

async fn fetch_legacy_members_prod(
    db: DatabaseConnection,
    source_uri: String,
) -> Result<Vec<LegacyMember>, ErrorResponse> {
    let rows = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MEMBERSHIP_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(source_uri)],
        ))
        .all(&db)
        .await
        .map_err(|e| internal_error(format!("db error: {e}")))?;

    let mut seen = HashSet::new();
    Ok(rows
        .into_iter()
        .filter(|r| seen.insert(r.did.clone()))
        .map(|r| LegacyMember {
            membership_uri: format!("at://{}/{}/{}", r.did, r.nsid, r.rkey),
            did: r.did,
        })
        .collect())
}

fn create_record_boxed(
    pds_endpoint: String,
    access_jwt: String,
    repo: String,
    collection: String,
    rkey: Option<String>,
    record: Value,
) -> BoxFuture<'static, Result<RecordRef, PdsError>> {
    Box::pin(async move {
        pds_client::create_record(
            &pds_endpoint,
            &access_jwt,
            &repo,
            &collection,
            rkey.as_deref(),
            &record,
        )
        .await
    })
}

fn upload_blob_boxed(
    pds_endpoint: String,
    access_jwt: String,
    bytes: Vec<u8>,
    mime_type: String,
) -> BoxFuture<'static, Result<Value, PdsError>> {
    Box::pin(
        async move { pds_client::upload_blob(&pds_endpoint, &access_jwt, bytes, &mime_type).await },
    )
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn register_dids_boxed(dids: Vec<String>) -> BoxFuture<'static, ()> {
    Box::pin(async move { crate::lib::tap::register_dids(dids).await })
}

/// Builds the managed-vs-BYO provisioning closure, mirroring `community.create`.
/// Managed mints a fresh DID on the AppView's PDS; BYO opens a session against
/// the supplied credentials and writes onto that DID. Either way credentials
/// are persisted before any bootstrap write so partial failures are
/// recoverable.
fn build_provision_fn(
    db: DatabaseConnection,
    byo: Option<(String, String, String)>,
) -> Result<Box<ProvisionFn>, ErrorResponse> {
    if let Some((pds, identifier, password)) = byo {
        Ok(Box::new(move || {
            let (db, pds, identifier, password) = (
                db.clone(),
                pds.clone(),
                identifier.clone(),
                password.clone(),
            );
            Box::pin(async move {
                let session = pds_client::create_session(&pds, &identifier, &password)
                    .await
                    .map_err(|e| pds_error(format!("createSession failed: {e}")))?;
                community_credentials::upsert_credentials(
                    &db,
                    crypto::master_key(),
                    &session.did,
                    &pds,
                    &identifier,
                    &password,
                    SOURCE_BYO,
                )
                .await
                .map_err(|e| internal_error(format!("failed to persist credentials: {e}")))?;
                Ok(ProvisionedTarget {
                    did: session.did,
                    pds_endpoint: pds,
                    access_jwt: session.access_jwt,
                })
            })
        }))
    } else {
        let pds_endpoint = std::env::var("PDS_LOC")
            .map_err(|_| internal_error(String::from("PDS_LOC env var not set")))?;
        let handle_domain = std::env::var("APPVIEW_HANDLE_DOMAIN")
            .map_err(|_| internal_error(String::from("APPVIEW_HANDLE_DOMAIN env var not set")))?;
        let admin_pass = std::env::var("PDS_ADMIN_PASS")
            .map_err(|_| internal_error(String::from("PDS_ADMIN_PASS env var not set")))?;

        Ok(Box::new(move || {
            let (db, pds_endpoint, handle_domain, admin_pass) = (
                db.clone(),
                pds_endpoint.clone(),
                handle_domain.clone(),
                admin_pass.clone(),
            );
            Box::pin(async move {
                let placeholder = generate_tid();
                let handle = format!("c-{placeholder}.{handle_domain}");
                let email = format!("c-{placeholder}@noreply.{handle_domain}");
                let password = pds_client::generate_strong_password();

                let account = pds_client::create_account(
                    &pds_endpoint,
                    Some(&admin_pass),
                    &handle,
                    &email,
                    &password,
                )
                .await
                .map_err(|e| pds_error(format!("createAccount failed: {e}")))?;

                community_credentials::upsert_credentials(
                    &db,
                    crypto::master_key(),
                    &account.did,
                    &pds_endpoint,
                    &account.handle,
                    &password,
                    SOURCE_APPVIEW_MANAGED,
                )
                .await
                .map_err(|e| internal_error(format!("failed to persist credentials: {e}")))?;

                let session = pds_client::create_session(&pds_endpoint, &account.handle, &password)
                    .await
                    .map_err(|e| pds_error(format!("createSession failed: {e}")))?;

                Ok(ProvisionedTarget {
                    did: account.did,
                    pds_endpoint,
                    access_jwt: session.access_jwt,
                })
            })
        }))
    }
}

#[post(
    "/xrpc/social.colibri.community.migrate?<kind>&<source>&<name>&<description>&<requiresApprovalToJoin>&<auth>&<mimeType>&<pds>&<identifier>&<password>",
    data = "<picture>"
)]
/// Migrates a legacy community into a fresh community (see module docs). The
/// optional replacement picture is sent as the raw request body, exactly like
/// `community.create`.
#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub async fn migrate(
    kind: &str,
    source: &str,
    name: Option<&str>,
    description: Option<&str>,
    requiresApprovalToJoin: Option<bool>,
    auth: &str,
    mimeType: Option<&str>,
    pds: Option<&str>,
    identifier: Option<&str>,
    password: Option<&str>,
    picture: Data<'_>,
    db: &State<DatabaseConnection>,
) -> Result<Json<MigrateResponse>, ErrorResponse> {
    let capped = picture
        .open(MAX_PICTURE_MEBIBYTES.mebibytes())
        .into_bytes()
        .await
        .map_err(|e| invalid_request(format!("Failed to read picture body: {e}")))?;
    if !capped.is_complete() {
        return Err(invalid_request(format!(
            "picture exceeds the maximum allowed size of {MAX_PICTURE_MEBIBYTES} MiB."
        )));
    }
    let bytes = capped.into_inner();
    let picture = if bytes.is_empty() { None } else { Some(bytes) };

    let input = MigrateInput {
        kind: kind.to_string(),
        source: source.to_string(),
        name: name.map(str::to_string),
        description: description.map(str::to_string),
        requires_approval_to_join: requiresApprovalToJoin,
        picture,
        mime_type: mimeType.map(str::to_string),
    };

    let byo = match (pds, identifier, password) {
        (Some(p), Some(i), Some(pw)) if !p.is_empty() && !i.is_empty() && !pw.is_empty() => {
            Some((p.to_string(), i.to_string(), pw.to_string()))
        }
        _ => None,
    };

    let db_conn = db.inner().clone();
    let provision_fn = build_provision_fn(db_conn.clone(), byo)?;

    let db_for_structure = db_conn.clone();
    let fetch_structure_fn =
        move |uri: String| -> BoxFuture<'static, Result<Option<OldStructure>, ErrorResponse>> {
            let db = db_for_structure.clone();
            Box::pin(async move { fetch_old_structure_prod(db, uri).await })
        };

    let db_for_members = db_conn.clone();
    let fetch_members_fn =
        move |uri: String| -> BoxFuture<'static, Result<Vec<LegacyMember>, ErrorResponse>> {
            let db = db_for_members.clone();
            Box::pin(async move { fetch_legacy_members_prod(db, uri).await })
        };

    migrate_with(
        auth.to_string(),
        input,
        &verify_auth_boxed,
        provision_fn.as_ref(),
        &fetch_structure_fn,
        &fetch_members_fn,
        &create_record_boxed,
        &upload_blob_boxed,
        &register_dids_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn legacy_community() -> ColibriCommunity {
        ColibriCommunity {
            r#type: String::from(COMMUNITY_NSID),
            name: String::from("Old Community"),
            description: String::from("legacy"),
            category_order: vec![String::from("cat-old")],
            requires_approval_to_join: true,
            picture: None,
            migrated_to: None,
            migrated_from: None,
            appview: None,
        }
    }

    fn old_structure() -> OldStructure {
        OldStructure {
            community: legacy_community(),
            categories: vec![(
                String::from("cat-old"),
                ColibriCategory {
                    r#type: String::from(CATEGORY_NSID),
                    name: String::from("General"),
                    channel_order: vec![String::from("chan-old")],
                    community: String::from("legacy-rkey"),
                },
            )],
            channels: vec![(
                String::from("chan-old"),
                ColibriChannel {
                    r#type: String::from(CHANNEL_NSID),
                    name: String::from("general"),
                    description: None,
                    channel_type: String::from("social.colibri.channel.text"),
                    category: String::from("cat-old"),
                    community: String::from("legacy-rkey"),
                    owner_only: None,
                    allowed_roles: vec![String::from("stale-role")],
                    allowed_members: vec![],
                    migrated_from: None,
                },
            )],
        }
    }

    fn input() -> MigrateInput {
        MigrateInput {
            kind: String::from(SUPPORTED_KIND),
            source: String::from("at://did:plc:owner/social.colibri.community/legacy-rkey"),
            name: None,
            description: None,
            requires_approval_to_join: None,
            picture: None,
            mime_type: None,
        }
    }

    #[test]
    fn plan_remaps_structure_and_stamps_migrated_from() {
        let plan = plan_structure(
            "did:plc:new",
            &input().source,
            "did:plc:owner",
            &old_structure(),
            &input(),
        );

        // Community carries migratedFrom and copies legacy metadata.
        assert_eq!(plan.community.name, "Old Community");
        assert_eq!(
            plan.community.migrated_from.as_deref(),
            Some("at://did:plc:owner/social.colibri.community/legacy-rkey")
        );
        assert!(plan.community.migrated_to.is_none());
        assert_eq!(plan.community.category_order.len(), 1);

        // One category, one channel, both with fresh rkeys.
        assert_eq!(plan.categories.len(), 1);
        assert_eq!(plan.channels.len(), 1);
        let (new_cat_rkey, cat) = &plan.categories[0];
        let (new_chan_rkey, chan) = &plan.channels[0];
        assert_ne!(new_cat_rkey, "cat-old");
        assert_ne!(new_chan_rkey, "chan-old");

        // Channel references the *new* category rkey and community "self".
        assert_eq!(&chan.category, new_cat_rkey);
        assert_eq!(chan.community, "self");
        assert_eq!(cat.community, "self");
        // Category's channelOrder is remapped to the new channel rkey.
        assert_eq!(cat.channel_order, vec![new_chan_rkey.clone()]);
        // Community's categoryOrder is remapped to the new category rkey.
        assert_eq!(plan.community.category_order, vec![new_cat_rkey.clone()]);

        // Channel stamped with its legacy AT-URI and stale allow-lists dropped.
        assert_eq!(
            chan.migrated_from.as_deref(),
            Some("at://did:plc:owner/social.colibri.channel/chan-old")
        );
        assert!(chan.allowed_roles.is_empty());

        // channelMap maps legacy channel URI -> new channel URI.
        assert_eq!(plan.channel_map.len(), 1);
        assert_eq!(
            plan.channel_map[0].old,
            "at://did:plc:owner/social.colibri.channel/chan-old"
        );
        assert_eq!(
            plan.channel_map[0].new,
            format!("at://did:plc:new/social.colibri.channel/{new_chan_rkey}")
        );
    }

    #[test]
    fn plan_applies_overrides() {
        let mut input = input();
        input.name = Some(String::from("Renamed"));
        input.requires_approval_to_join = Some(false);
        let plan = plan_structure(
            "did:plc:new",
            &input.source,
            "did:plc:owner",
            &old_structure(),
            &input,
        );
        assert_eq!(plan.community.name, "Renamed");
        assert!(!plan.community.requires_approval_to_join);
    }

    fn ok_provision() -> Box<ProvisionFn> {
        Box::new(|| {
            Box::pin(async {
                Ok(ProvisionedTarget {
                    did: String::from("did:plc:new"),
                    pds_endpoint: String::from("https://pds.example"),
                    access_jwt: String::from("jwt"),
                })
            })
        })
    }

    type CapturedRecords = Arc<Mutex<Vec<(String, Value)>>>;

    fn record_capturer(records: CapturedRecords) -> Box<CreateRecordFn> {
        Box::new(
            move |_, _, _, collection: String, rkey: Option<String>, record: Value| {
                let records = records.clone();
                Box::pin(async move {
                    records.lock().unwrap().push((collection.clone(), record));
                    let rkey = rkey.unwrap_or_else(|| String::from("auto"));
                    Ok(RecordRef {
                        uri: format!("at://did:plc:new/{collection}/{rkey}"),
                        cid: String::from("cid"),
                    })
                })
            },
        )
    }

    #[tokio::test]
    async fn rejects_unsupported_kind() {
        let mut input = input();
        input.kind = String::from("something-else");
        let result = migrate_with(
            String::from("token"),
            input,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            ok_provision().as_ref(),
            &|_| Box::pin(async { panic!("should not fetch") }),
            &|_| Box::pin(async { panic!("should not fetch members") }),
            &|_, _, _, _, _, _| Box::pin(async { panic!("should not write") }),
            &|_, _, _, _| Box::pin(async { panic!("should not upload") }),
            &|_| Box::pin(async {}),
        )
        .await;
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn rejects_non_owner_caller() {
        let result = migrate_with(
            String::from("token"),
            input(),
            // Caller is not the source authority (did:plc:owner).
            &|_, _| Box::pin(async { Ok(String::from("did:plc:someone-else")) }),
            ok_provision().as_ref(),
            &|_| Box::pin(async { panic!("should not fetch") }),
            &|_| Box::pin(async { panic!("should not fetch members") }),
            &|_, _, _, _, _, _| Box::pin(async { panic!("should not write") }),
            &|_, _, _, _| Box::pin(async { panic!("should not upload") }),
            &|_| Box::pin(async {}),
        )
        .await;
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn writes_full_migration_and_imports_members() {
        let records: CapturedRecords = Arc::new(Mutex::new(vec![]));
        let create_record = record_capturer(records.clone());
        let registered: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
        let reg = registered.clone();

        let result = migrate_with(
            String::from("token"),
            input(),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            ok_provision().as_ref(),
            &|_| Box::pin(async { Ok(Some(old_structure())) }),
            &|_| {
                Box::pin(async {
                    Ok(vec![
                        LegacyMember {
                            did: String::from("did:plc:alice"),
                            membership_uri: String::from(
                                "at://did:plc:alice/social.colibri.membership/m1",
                            ),
                        },
                        // Duplicate of the owner — must be skipped.
                        LegacyMember {
                            did: String::from("did:plc:owner"),
                            membership_uri: String::from(
                                "at://did:plc:owner/social.colibri.membership/m2",
                            ),
                        },
                    ])
                })
            },
            create_record.as_ref(),
            &|_, _, _, _| Box::pin(async { panic!("no picture in this test") }),
            &move |dids| {
                let reg = reg.clone();
                Box::pin(async move {
                    reg.lock().unwrap().extend(dids);
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:new");
        assert_eq!(
            result.community,
            "at://did:plc:new/social.colibri.community/self"
        );
        assert_eq!(result.channel_map.len(), 1);

        let records = records.lock().unwrap();
        let count = |nsid: &str| records.iter().filter(|(c, _)| c == nsid).count();
        assert_eq!(count(COMMUNITY_NSID), 1);
        assert_eq!(count(CATEGORY_NSID), 1);
        assert_eq!(count(CHANNEL_NSID), 1);
        assert_eq!(count("social.colibri.role"), 1);
        // Owner + alice = 2 members (the duplicate owner membership is skipped).
        assert_eq!(count("social.colibri.member"), 2);
        assert_eq!(count("social.colibri.actor.data"), 1);

        // The new DID was registered with Tap for firehose indexing.
        assert_eq!(
            *registered.lock().unwrap(),
            vec![String::from("did:plc:new")]
        );
    }
}
