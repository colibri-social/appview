//! `social.colibri.community.create` — Variant A registration endpoint.
//!
//! Mints a fresh DID on the AppView's own PDS (`PDS_LOC`), stores the new
//! account's credentials encrypted, and bootstraps five on-protocol records
//! on the new repo so the caller ends up as an owner-equivalent member of a
//! fully populated community:
//!
//! - `social.colibri.community` (the community metadata, pinned at
//!   `rkey: "self"`)
//! - `social.colibri.category` (a default "General" category)
//! - `social.colibri.channel` (a default "general" text channel inside that
//!   category)
//! - `social.colibri.role` (an "Owner" role with every permission and
//!   `protected: true` so role-management endpoints refuse to delete it)
//! - `social.colibri.member` (subject = caller DID, holding the Owner role)
//!
//! Rkeys for the four non-singleton records are pre-generated locally so each
//! record can reference the others before any PDS round-trip.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::colibri::{
    ColibriActorData, ColibriCategory, ColibriChannel, ColibriCommunity, ColibriMember, ColibriRole,
};
use crate::lib::community_credentials::{self, SOURCE_APPVIEW_MANAGED};
use crate::lib::crypto;
use crate::lib::moderation::generate_tid;
use crate::lib::pds_client::{self, CreatedAccount, PdsError, RecordRef};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::lib::time::current_iso8601_utc;

#[derive(Serialize, Debug)]
pub struct CreateCommunityResponse {
    pub did: String,
    pub community: String,
    pub category: String,
    pub channel: String,
    #[serde(rename = "ownerRole")]
    pub owner_role: String,
    pub member: String,
}

const DEFAULT_CATEGORY_NAME: &str = "General";
const DEFAULT_CHANNEL_NAME: &str = "general";
const TEXT_CHANNEL_TYPE: &str = "social.colibri.channel.text";
const COMMUNITY_RKEY: &str = "self";

/// MIME types the community lexicon's `picture` field accepts. Mirrors
/// `accept: ["image/jpeg", "image/png", "image/gif"]` in the lexicon doc;
/// keep in sync.
const ALLOWED_PICTURE_MIME_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif"];

/// Decodes a base64-encoded picture and validates its declared MIME type.
///
/// Rejects with `InvalidRequest` if `mime_type` isn't in
/// [`ALLOWED_PICTURE_MIME_TYPES`], or if neither URL-safe nor standard
/// base64 decoding succeeds against the input. (Query-string base64 most
/// commonly arrives URL-safe; some clients still send the standard
/// alphabet, so we accept both.)
fn decode_picture(picture_b64: &str, mime_type: &str) -> Result<Vec<u8>, ErrorResponse> {
    if !ALLOWED_PICTURE_MIME_TYPES.contains(&mime_type) {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: format!(
                    "Unsupported picture mime_type `{mime_type}`. Accepted: {}.",
                    ALLOWED_PICTURE_MIME_TYPES.join(", ")
                ),
            }),
        });
    }

    if let Ok(bytes) = URL_SAFE_NO_PAD.decode(picture_b64) {
        return Ok(bytes);
    }
    STANDARD.decode(picture_b64).map_err(|e| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: format!("Invalid base64 in `image`: {e}"),
        }),
    })
}

#[derive(Deserialize, Debug)]
pub struct CreateCommunityInput {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "requiresApprovalToJoin", default = "default_true")]
    pub requires_approval_to_join: bool,
    #[serde(default)]
    pub picture: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Admin credentials for the AppView's own PDS. Bundled as a struct so the
/// caller passes them through `create_with` without exploding its parameter
/// list further. Going through the live handler these come from `PDS_ADMIN_PASS`.
#[derive(Debug, Clone)]
pub struct AdminCredentials {
    pub password: String,
}

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type CreateAccountFn = dyn Fn(
        String,
        AdminCredentials,
        String,
        String,
        String,
    ) -> BoxFuture<'static, Result<CreatedAccount, PdsError>>
    + Send
    + Sync;
type CreateSessionFn =
    dyn Fn(String, String, String) -> BoxFuture<'static, Result<String, PdsError>> + Send + Sync;
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
type UpsertCredentialsFn =
    dyn Fn(String, String, String, String) -> BoxFuture<'static, Result<(), String>> + Send + Sync;
type UploadBlobFn = dyn Fn(String, String, Vec<u8>, String) -> BoxFuture<'static, Result<Value, PdsError>>
    + Send
    + Sync;

#[allow(clippy::too_many_arguments)]
async fn create_with(
    auth: String,
    input: CreateCommunityInput,
    handle_domain: String,
    pds_endpoint: String,
    admin_credentials: AdminCredentials,
    verify_auth_fn: &VerifyAuthFn,
    create_account_fn: &CreateAccountFn,
    create_session_fn: &CreateSessionFn,
    create_record_fn: &CreateRecordFn,
    upsert_credentials_fn: &UpsertCredentialsFn,
    upload_blob_fn: &UploadBlobFn,
) -> Result<Json<CreateCommunityResponse>, ErrorResponse> {
    let caller_did = verify_auth_fn(auth, String::from("social.colibri.community.create"))
        .await
        .map_err(auth_error)?;

    log::info!("community.create requested by {caller_did}");

    // Generate a placeholder handle on the AppView's domain. Colibri clients
    // identify communities by DID; the handle is an implementation detail of
    // PDS account hosting.
    let placeholder_rkey = generate_tid();
    let handle = format!("c-{placeholder_rkey}.{handle_domain}");
    let email = format!("c-{placeholder_rkey}@noreply.{handle_domain}");
    let password = pds_client::generate_strong_password();

    log::debug!("generated placeholder handle {handle} for new community");

    let account = create_account_fn(
        pds_endpoint.clone(),
        admin_credentials,
        handle,
        email,
        password.clone(),
    )
    .await
    .map_err(|e| {
        log::error!("community.create: createAccount on {pds_endpoint} failed: {e}");
        pds_error(format!("createAccount failed: {e}"))
    })?;

    let community_did = account.did.clone();
    log::info!(
        "minted community DID {community_did} (handle {handle})",
        handle = account.handle
    );

    // Persist credentials immediately so any partial failure below is still
    // recoverable via a follow-up call.
    upsert_credentials_fn(
        community_did.clone(),
        pds_endpoint.clone(),
        account.handle.clone(),
        password.clone(),
    )
    .await
    .map_err(|e| {
        log::error!(
            "community.create: failed to persist credentials for {community_did}: {e} \
             — PDS account exists but credentials are not stored, manual recovery required"
        );
        internal_error(format!("failed to persist credentials: {e}"))
    })?;

    log::debug!("persisted credentials for {community_did}");

    // Fresh session for the bootstrap writes — `account.access_jwt` from
    // createAccount is usable directly, but re-using create_session keeps the
    // dependency surface identical for testing.
    let access_jwt = create_session_fn(pds_endpoint.clone(), account.handle.clone(), password)
        .await
        .map_err(|e| {
            log::error!(
                "community.create: createSession for {community_did} failed: {e} \
                 — credentials persisted, bootstrap recoverable via follow-up call"
            );
            pds_error(format!("createSession failed: {e}"))
        })?;

    // Pre-generate rkeys for all bootstrap records up front so each record
    // can embed references to the others before any PDS round-trip.
    let category_rkey = generate_tid();
    let channel_rkey = generate_tid();
    let role_rkey = generate_tid();
    let member_rkey = generate_tid();

    // Upload the optional community picture before writing the community
    // record so the record can embed the resulting blob ref. Picture bytes
    // come in as base64 on the query string; the PDS reads the MIME type
    // off the `Content-Type` header in `upload_blob`.
    let picture_blob = match (input.picture.as_deref(), input.mime_type.as_deref()) {
        (Some(b64), Some(mime)) => {
            let bytes = decode_picture(b64, mime).inspect_err(|_e| {
                log::warn!(
                    "community.create: rejecting picture for {community_did} (mime={mime}, \
                     account already minted)"
                );
            })?;
            let byte_len = bytes.len();
            let blob = upload_blob_fn(
                pds_endpoint.clone(),
                access_jwt.clone(),
                bytes,
                mime.to_string(),
            )
            .await
            .map_err(|e| {
                log::error!("community.create: uploadBlob for {community_did} failed: {e}");
                pds_error(format!("uploadBlob failed: {e}"))
            })?;
            log::debug!("uploaded picture blob for {community_did} ({mime}, {byte_len} bytes)");
            Some(blob)
        }
        (Some(_), None) => {
            log::warn!(
                "community.create: rejecting picture for {community_did}: mime_type missing \
                 (account already minted)"
            );
            return Err(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("InvalidRequest"),
                    message: String::from("`mime_type` is required when `image` is supplied."),
                }),
            });
        }
        _ => None,
    };

    // 1. Community (singleton — rkey is fixed at "self"; categoryOrder
    //    references the not-yet-written category by its pre-generated rkey).
    let community_record = ColibriCommunity {
        r#type: String::from("social.colibri.community"),
        name: input.name.clone(),
        description: input.description.clone().unwrap_or_default(),
        category_order: vec![category_rkey.clone()],
        requires_approval_to_join: input.requires_approval_to_join,
        picture: picture_blob,
    };
    let community_ref = write_record_logged(
        create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
        &community_did,
        community_did.clone(),
        "social.colibri.community",
        Some(COMMUNITY_RKEY.to_string()),
        &community_record,
        "community",
    )
    .await?;

    // 2. Default category — references the community as "self" (the rkey
    //    we just pinned the community record at).
    let category_record = ColibriCategory {
        r#type: String::from("social.colibri.category"),
        name: String::from(DEFAULT_CATEGORY_NAME),
        channel_order: vec![channel_rkey.clone()],
        community: COMMUNITY_RKEY.to_string(),
    };
    let category_ref = write_record_logged(
        create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
        &community_did,
        community_did.clone(),
        "social.colibri.category",
        Some(category_rkey.clone()),
        &category_record,
        "category",
    )
    .await?;

    // 3. Default text channel — references category by rkey, community as
    //    "self".
    let channel_record = ColibriChannel {
        r#type: String::from("social.colibri.channel"),
        name: String::from(DEFAULT_CHANNEL_NAME),
        description: None,
        channel_type: String::from(TEXT_CHANNEL_TYPE),
        category: category_rkey.clone(),
        community: COMMUNITY_RKEY.to_string(),
        owner_only: None,
    };
    let channel_ref = write_record_logged(
        create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
        &community_did,
        community_did.clone(),
        "social.colibri.channel",
        Some(channel_rkey.clone()),
        &channel_record,
        "channel",
    )
    .await?;

    // 4. Owner role — `protected: true` so role-management endpoints can
    //    refuse to delete or mutate it.
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
    let role_ref = write_record_logged(
        create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
        &community_did,
        community_did.clone(),
        "social.colibri.role",
        Some(role_rkey.clone()),
        &owner_role,
        "role",
    )
    .await?;

    // 5. Owner member — subject is the caller, roles[] references the role
    //    we just minted.
    let owner_member = ColibriMember {
        record_type: Some(String::from("social.colibri.member")),
        subject: caller_did.clone(),
        roles: vec![role_rkey],
        joined_at: current_iso8601_utc(),
        nickname: None,
        from_membership: None,
    };
    let member_ref = write_record_logged(
        create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
        &community_did,
        community_did.clone(),
        "social.colibri.member",
        Some(member_rkey),
        &owner_member,
        "member",
    )
    .await?;

    // 6. Empty `social.colibri.actor.data` record at rkey "self". The
    //    community DID isn't a human actor, but having this record in place
    //    (a) lets `getData(<community_did>)` succeed without 404ing on the
    //    actor.data lookup, and (b) gives Tap a stable record on the
    //    community repo that downstream consumers can rely on as a "this
    //    DID is a Colibri-managed community" signal.
    //
    //    Empty everything: no emoji, empty status, no community memberships.
    //    The community DID never joins other communities.
    let actor_data_record = ColibriActorData {
        record_type: Some(String::from("social.colibri.actor.data")),
        emoji: Some(String::new()),
        status: Some(String::new()),
        communities: vec![],
    };
    write_record_logged(
        create_record_fn,
        pds_endpoint,
        access_jwt,
        &community_did,
        community_did.clone(),
        "social.colibri.actor.data",
        Some(String::from("self")),
        &actor_data_record,
        "actor.data",
    )
    .await?;

    log::info!("community.create complete for {community_did} (caller {caller_did})");

    Ok(Json(CreateCommunityResponse {
        did: community_did,
        community: community_ref.uri,
        category: category_ref.uri,
        channel: channel_ref.uri,
        owner_role: role_ref.uri,
        member: member_ref.uri,
    }))
}

/// Wraps `write_record` with bootstrap-flow logging: a `debug` line before the
/// call and an `error` line if the upstream write fails. Kept separate from
/// `write_record` so the underlying helper stays generic.
#[allow(clippy::too_many_arguments)]
async fn write_record_logged<T>(
    create_record_fn: &CreateRecordFn,
    pds_endpoint: String,
    access_jwt: String,
    community_did: &str,
    repo: String,
    collection: &'static str,
    rkey: Option<String>,
    record: &T,
    label: &'static str,
) -> Result<RecordRef, ErrorResponse>
where
    T: Serialize,
{
    log::debug!(
        "writing {label} record for {community_did} (rkey={})",
        rkey.as_deref().unwrap_or("<auto>")
    );
    write_record(
        create_record_fn,
        pds_endpoint,
        access_jwt,
        repo,
        collection,
        rkey,
        record,
        label,
    )
    .await
    .inspect_err(|e| {
        log::error!(
            "community.create: write_record({label}) for {community_did} failed: {}",
            e.body.0.message
        );
    })
}

/// Serializes a record payload and issues one `createRecord` call. Centralizes
/// the error-translation boilerplate so the five bootstrap writes above all
/// surface the same shape of error.
#[allow(clippy::too_many_arguments)]
async fn write_record<T>(
    create_record_fn: &CreateRecordFn,
    pds_endpoint: String,
    access_jwt: String,
    repo: String,
    collection: &'static str,
    rkey: Option<String>,
    record: &T,
    label: &'static str,
) -> Result<RecordRef, ErrorResponse>
where
    T: Serialize,
{
    let value = serde_json::to_value(record)
        .map_err(|e| internal_error(format!("serialize {label}: {e}")))?;
    create_record_fn(
        pds_endpoint,
        access_jwt,
        repo,
        collection.to_string(),
        rkey,
        value,
    )
    .await
    .map_err(|e| pds_error(format!("createRecord({label}) failed: {e}")))
}

fn auth_error(err: ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

fn pds_error(message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("UpstreamError"),
            message,
        }),
    }
}

fn internal_error(message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InternalServerError"),
            message,
        }),
    }
}

// ---- Boxed production dependencies --------------------------------------

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn create_account_boxed(
    pds_endpoint: String,
    admin_credentials: AdminCredentials,
    handle: String,
    email: String,
    password: String,
) -> BoxFuture<'static, Result<CreatedAccount, PdsError>> {
    Box::pin(async move {
        pds_client::create_account(
            &pds_endpoint,
            Some(&admin_credentials.password),
            &handle,
            &email,
            &password,
        )
        .await
    })
}

fn create_session_boxed(
    pds_endpoint: String,
    identifier: String,
    password: String,
) -> BoxFuture<'static, Result<String, PdsError>> {
    Box::pin(async move {
        let session = pds_client::create_session(&pds_endpoint, &identifier, &password).await?;
        Ok(session.access_jwt)
    })
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

#[post(
    "/xrpc/social.colibri.community.create?<name>&<description>&<requires_approval_to_join>&<auth>&<image>&<mime_type>"
)]
/// Mints a new community DID on the AppView's PDS and bootstraps it.
pub async fn create(
    name: &str,
    description: Option<&str>,
    requires_approval_to_join: Option<bool>,
    image: Option<&str>,
    mime_type: Option<&str>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CreateCommunityResponse>, ErrorResponse> {
    let input = CreateCommunityInput {
        name: name.to_string(),
        description: description.map(|s| s.to_string()),
        requires_approval_to_join: requires_approval_to_join.unwrap_or(true),
        picture: image.map(|s| s.to_string()),
        mime_type: mime_type.map(|s| s.to_string()),
    };

    let pds_endpoint = std::env::var("PDS_LOC")
        .map_err(|_| internal_error(String::from("PDS_LOC env var not set")))?;
    let handle_domain = std::env::var("APPVIEW_HANDLE_DOMAIN")
        .map_err(|_| internal_error(String::from("APPVIEW_HANDLE_DOMAIN env var not set")))?;
    let admin_credentials = AdminCredentials {
        password: std::env::var("PDS_ADMIN_PASS")
            .map_err(|_| internal_error(String::from("PDS_ADMIN_PASS env var not set")))?,
    };

    // The credential upsert closure has to bind to the live DB connection;
    // build it inline rather than reusing the (DB-free) boxed helper above.
    let db_for_upsert = db.inner().clone();
    let upsert = move |community_did: String,
                       pds_endpoint: String,
                       identifier: String,
                       password: String|
          -> BoxFuture<'static, Result<(), String>> {
        let db = db_for_upsert.clone();
        Box::pin(async move {
            community_credentials::upsert_credentials(
                &db,
                crypto::master_key(),
                &community_did,
                &pds_endpoint,
                &identifier,
                &password,
                SOURCE_APPVIEW_MANAGED,
            )
            .await
            .map_err(|e| e.to_string())
        })
    };

    let response = create_with(
        auth.to_string(),
        input,
        handle_domain,
        pds_endpoint,
        admin_credentials,
        &verify_auth_boxed,
        &create_account_boxed,
        &create_session_boxed,
        &create_record_boxed,
        &upsert,
        &upload_blob_boxed,
    )
    .await?;

    // Register the freshly-minted community DID with Tap so the firehose
    // starts delivering its records into `record_data`. Without this, the
    // bootstrap records (community, category, channel, role, member) sit on
    // the new PDS and never enter the local index — `listCommunities` would
    // never return the new community, even for its owner.
    //
    // Done out here (rather than in `create_with`) so the test seam surface
    // for `create_with` stays unchanged and the env-var reads inside
    // `register_dids` don't crash unit tests.
    crate::lib::tap::register_dids(vec![response.did.clone()]).await;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn input() -> CreateCommunityInput {
        CreateCommunityInput {
            name: String::from("Test"),
            description: Some(String::from("desc")),
            requires_approval_to_join: false,
            picture: None,
            mime_type: None,
        }
    }

    fn admin() -> AdminCredentials {
        AdminCredentials {
            password: String::from("admin-pass"),
        }
    }

    #[tokio::test]
    async fn bootstrap_writes_full_community_with_default_category_and_channel() {
        let created_records: Arc<Mutex<Vec<(String, Option<String>, Value)>>> =
            Arc::new(Mutex::new(vec![]));
        let credentials_captured: Arc<Mutex<Option<(String, String, String, String)>>> =
            Arc::new(Mutex::new(None));
        let cr = created_records.clone();
        let cc = credentials_captured.clone();

        let admin_captured: Arc<Mutex<Option<AdminCredentials>>> = Arc::new(Mutex::new(None));
        let ac = admin_captured.clone();

        let create_account = move |_: String,
                                   admin: AdminCredentials,
                                   handle: String,
                                   _email: String,
                                   _password: String|
              -> BoxFuture<'static, Result<CreatedAccount, PdsError>> {
            let ac = ac.clone();
            Box::pin(async move {
                *ac.lock().unwrap() = Some(admin);
                Ok(CreatedAccount {
                    did: String::from("did:plc:newcomm"),
                    access_jwt: String::from("jwt-from-create"),
                    handle,
                })
            })
        };
        let create_record = move |_: String,
                                  _: String,
                                  _: String,
                                  collection: String,
                                  rkey: Option<String>,
                                  record: Value|
              -> BoxFuture<'static, Result<RecordRef, PdsError>> {
            let cr = cr.clone();
            Box::pin(async move {
                cr.lock()
                    .unwrap()
                    .push((collection.clone(), rkey.clone(), record));
                // Pinned-rkey calls echo the supplied rkey; otherwise
                // pretend the PDS minted a stable test rkey.
                let assigned_rkey = rkey.unwrap_or_else(|| String::from("auto-rkey"));
                Ok(RecordRef {
                    uri: format!("at://did:plc:newcomm/{collection}/{assigned_rkey}"),
                    cid: String::from("cid"),
                })
            })
        };
        let upsert = move |did: String,
                           endpoint: String,
                           identifier: String,
                           password: String|
              -> BoxFuture<'static, Result<(), String>> {
            let cc = cc.clone();
            Box::pin(async move {
                *cc.lock().unwrap() = Some((did, endpoint, identifier, password));
                Ok(())
            })
        };
        let result = create_with(
            String::from("token"),
            input(),
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &create_account,
            &|_, _, _| Box::pin(async { Ok(String::from("jwt-session")) }),
            &create_record,
            &upsert,
            &|_, _, _, _| Box::pin(async { panic!("should not upload when no picture given") }),
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:newcomm");
        // The community is pinned at rkey "self".
        assert_eq!(
            result.community,
            "at://did:plc:newcomm/social.colibri.community/self"
        );

        let records = created_records.lock().unwrap();
        assert_eq!(records.len(), 6, "bootstrap writes 6 records");

        let by_collection: std::collections::HashMap<_, _> = records
            .iter()
            .map(|(c, rkey, v)| (c.as_str(), (rkey, v)))
            .collect();

        // Community: pinned rkey, has the supplied name and a non-empty
        // categoryOrder referencing the bootstrap category.
        let (community_rkey, community_value) =
            by_collection.get("social.colibri.community").unwrap();
        assert_eq!(community_rkey.as_deref(), Some("self"));
        assert_eq!(community_value["name"], "Test");
        assert_eq!(community_value["requiresApprovalToJoin"], false);
        let category_order = community_value["categoryOrder"].as_array().unwrap();
        assert_eq!(category_order.len(), 1);

        // Category: name "General", channelOrder references the bootstrap
        // channel, community = "self".
        let (_, category_value) = by_collection.get("social.colibri.category").unwrap();
        assert_eq!(category_value["name"], "General");
        assert_eq!(category_value["community"], "self");
        let channel_order = category_value["channelOrder"].as_array().unwrap();
        assert_eq!(channel_order.len(), 1);

        // Channel: text type, references category + community = "self".
        let (_, channel_value) = by_collection.get("social.colibri.channel").unwrap();
        assert_eq!(channel_value["name"], "general");
        assert_eq!(channel_value["type"], "social.colibri.channel.text");
        assert_eq!(channel_value["community"], "self");

        // Owner role: full permission catalog + protected = true.
        let (_, role_value) = by_collection.get("social.colibri.role").unwrap();
        let role_perms = role_value["permissions"].as_array().unwrap();
        assert_eq!(role_perms.len(), Permission::all().len());
        assert_eq!(role_value["protected"], true);

        // Member: caller is the subject and holds the role.
        let (_, member_value) = by_collection.get("social.colibri.member").unwrap();
        assert_eq!(member_value["subject"], "did:plc:caller");
        let member_roles = member_value["roles"].as_array().unwrap();
        assert_eq!(member_roles.len(), 1);

        // Actor data: pinned at "self" with everything explicitly empty so
        // Tap indexes a stable, queryable row.
        let (actor_data_rkey, actor_data_value) =
            by_collection.get("social.colibri.actor.data").unwrap();
        assert_eq!(actor_data_rkey.as_deref(), Some("self"));
        assert_eq!(actor_data_value["status"], "");
        assert_eq!(actor_data_value["emoji"], "");
        let community_list = actor_data_value["communities"].as_array().unwrap();
        assert!(community_list.is_empty());

        let creds = credentials_captured.lock().unwrap();
        let saved = creds.as_ref().unwrap();
        assert_eq!(saved.0, "did:plc:newcomm");
        assert_eq!(saved.1, "https://pds.example");

        // Admin credentials reach createAccount so the PDS can mint accounts
        // without an invite code.
        let forwarded_admin = admin_captured.lock().unwrap();
        let forwarded = forwarded_admin.as_ref().unwrap();
        assert_eq!(forwarded.password, "admin-pass");
    }

    #[tokio::test]
    async fn rejects_when_auth_fails() {
        let result = create_with(
            String::from("token"),
            input(),
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _| Box::pin(async { panic!("should not call") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }

    #[tokio::test]
    async fn surfaces_pds_failure_during_account_creation() {
        let result = create_with(
            String::from("token"),
            input(),
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _, _, _| {
                Box::pin(async {
                    Err(PdsError::BadStatus {
                        status: 503,
                        body: String::from("nope"),
                    })
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _| Box::pin(async { panic!("should not call") }),
        )
        .await;

        assert!(result.is_err());
        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "UpstreamError");
        assert!(body.message.contains("createAccount"));
    }

    /// A 1×1 transparent PNG, base64-encoded in the standard alphabet.
    const TINY_PNG_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";

    #[tokio::test]
    async fn uploads_picture_and_links_blob_into_community_record() {
        let created_records: Arc<Mutex<Vec<(String, Option<String>, Value)>>> =
            Arc::new(Mutex::new(vec![]));
        let upload_captured: Arc<Mutex<Option<(Vec<u8>, String)>>> = Arc::new(Mutex::new(None));
        let cr = created_records.clone();
        let uc = upload_captured.clone();

        let create_record = move |_: String,
                                  _: String,
                                  _: String,
                                  collection: String,
                                  rkey: Option<String>,
                                  record: Value|
              -> BoxFuture<'static, Result<RecordRef, PdsError>> {
            let cr = cr.clone();
            Box::pin(async move {
                cr.lock()
                    .unwrap()
                    .push((collection.clone(), rkey.clone(), record));
                let assigned_rkey = rkey.unwrap_or_else(|| String::from("auto-rkey"));
                Ok(RecordRef {
                    uri: format!("at://did:plc:newcomm/{collection}/{assigned_rkey}"),
                    cid: String::from("cid"),
                })
            })
        };
        let upload_blob = move |_: String,
                                _: String,
                                bytes: Vec<u8>,
                                mime: String|
              -> BoxFuture<'static, Result<Value, PdsError>> {
            let uc = uc.clone();
            Box::pin(async move {
                *uc.lock().unwrap() = Some((bytes, mime.clone()));
                Ok(serde_json::json!({
                    "$type": "blob",
                    "ref": { "$link": "bafyreigh2akiscaildc7fmsxxq6jr2dpqyz4khsxqzfvuxe7osnrxrxv7q" },
                    "mimeType": mime,
                    "size": 70,
                }))
            })
        };

        let input_with_picture = CreateCommunityInput {
            name: String::from("Pictured"),
            description: Some(String::from("has a picture")),
            requires_approval_to_join: false,
            picture: Some(String::from(TINY_PNG_BASE64)),
            mime_type: Some(String::from("image/png")),
        };

        let result = create_with(
            String::from("token"),
            input_with_picture,
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, handle, _, _| {
                Box::pin(async move {
                    Ok(CreatedAccount {
                        did: String::from("did:plc:newcomm"),
                        access_jwt: String::from("jwt-from-create"),
                        handle,
                    })
                })
            },
            &|_, _, _| Box::pin(async { Ok(String::from("jwt-session")) }),
            &create_record,
            &|_, _, _, _| Box::pin(async { Ok(()) }),
            &upload_blob,
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:newcomm");

        // The upload was called with the decoded bytes (≠ the base64 string)
        // and the supplied mime type.
        let captured = upload_captured.lock().unwrap();
        let (bytes, mime) = captured.as_ref().unwrap();
        assert_eq!(mime, "image/png");
        // PNG magic bytes — confirms we decoded base64, not passed the string
        // through verbatim.
        assert_eq!(
            &bytes[..8],
            &[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]
        );

        // The community record's `picture` field carries the blob ref the
        // upload returned.
        let records = created_records.lock().unwrap();
        let (_, _, community_value) = records
            .iter()
            .find(|(c, _, _)| c == "social.colibri.community")
            .unwrap();
        let picture = &community_value["picture"];
        assert_eq!(picture["$type"], "blob");
        assert_eq!(picture["mimeType"], "image/png");
        assert!(picture["ref"]["$link"].is_string());
    }

    #[tokio::test]
    async fn rejects_image_without_mime_type() {
        let input_without_mime = CreateCommunityInput {
            name: String::from("NoMime"),
            description: None,
            requires_approval_to_join: false,
            picture: Some(String::from(TINY_PNG_BASE64)),
            mime_type: None,
        };

        let result = create_with(
            String::from("token"),
            input_without_mime,
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, handle, _, _| {
                Box::pin(async move {
                    Ok(CreatedAccount {
                        did: String::from("did:plc:newcomm"),
                        access_jwt: String::from("jwt"),
                        handle,
                    })
                })
            },
            &|_, _, _| Box::pin(async { Ok(String::from("jwt")) }),
            &|_, _, _, _, _, _| Box::pin(async { panic!("should not write records") }),
            &|_, _, _, _| Box::pin(async { Ok(()) }),
            &|_, _, _, _| Box::pin(async { panic!("should not upload without mime") }),
        )
        .await;

        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "InvalidRequest");
        assert!(body.message.contains("mime_type"));
    }

    #[tokio::test]
    async fn rejects_unsupported_mime_type() {
        let input_bad_mime = CreateCommunityInput {
            name: String::from("BadMime"),
            description: None,
            requires_approval_to_join: false,
            picture: Some(String::from(TINY_PNG_BASE64)),
            mime_type: Some(String::from("image/webp")),
        };

        let result = create_with(
            String::from("token"),
            input_bad_mime,
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, handle, _, _| {
                Box::pin(async move {
                    Ok(CreatedAccount {
                        did: String::from("did:plc:newcomm"),
                        access_jwt: String::from("jwt"),
                        handle,
                    })
                })
            },
            &|_, _, _| Box::pin(async { Ok(String::from("jwt")) }),
            &|_, _, _, _, _, _| Box::pin(async { panic!("should not write records") }),
            &|_, _, _, _| Box::pin(async { Ok(()) }),
            &|_, _, _, _| Box::pin(async { panic!("should not upload with bad mime") }),
        )
        .await;

        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "InvalidRequest");
        assert!(body.message.contains("image/webp"));
    }
}
