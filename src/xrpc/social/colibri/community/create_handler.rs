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

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::colibri::{
    ColibriCategory, ColibriChannel, ColibriCommunity, ColibriMember, ColibriRole,
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

#[derive(Deserialize, Debug)]
pub struct CreateCommunityInput {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "requiresApprovalToJoin", default = "default_true")]
    pub requires_approval_to_join: bool,
}

fn default_true() -> bool {
    true
}

/// Admin credentials for the AppView's own PDS. Bundled as a struct so the
/// caller passes them through `create_with` without exploding its parameter
/// list further. Going through the live handler these come from
/// `PDS_ADMIN_USER` / `PDS_ADMIN_PASS`.
#[derive(Debug, Clone)]
pub struct AdminCredentials {
    pub user: String,
    pub password: String,
}

#[allow(clippy::too_many_arguments)]
async fn create_with<V, A, S, R, U>(
    auth: String,
    input: CreateCommunityInput,
    handle_domain: String,
    pds_endpoint: String,
    admin_credentials: AdminCredentials,
    verify_auth_fn: V,
    create_account_fn: A,
    create_session_fn: S,
    create_record_fn: R,
    upsert_credentials_fn: U,
) -> Result<Json<CreateCommunityResponse>, ErrorResponse>
where
    V: Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>,
    A: Fn(
        String,
        AdminCredentials,
        String,
        String,
        String,
    ) -> BoxFuture<'static, Result<CreatedAccount, PdsError>>,
    S: Fn(String, String, String) -> BoxFuture<'static, Result<String, PdsError>>,
    R: Fn(
        String,
        String,
        String,
        String,
        Option<String>,
        Value,
    ) -> BoxFuture<'static, Result<RecordRef, PdsError>>,
    U: Fn(String, String, String, String) -> BoxFuture<'static, Result<(), String>>,
{
    let caller_did = verify_auth_fn(auth, String::from("social.colibri.community.create"))
        .await
        .map_err(auth_error)?;

    // Generate a placeholder handle on the AppView's domain. Colibri clients
    // identify communities by DID; the handle is an implementation detail of
    // PDS account hosting.
    let placeholder_rkey = generate_tid();
    let handle = format!("c-{placeholder_rkey}.{handle_domain}");
    let email = format!("c-{placeholder_rkey}@noreply.{handle_domain}");
    let password = pds_client::generate_strong_password();

    let account = create_account_fn(
        pds_endpoint.clone(),
        admin_credentials,
        handle,
        email,
        password.clone(),
    )
    .await
    .map_err(|e| pds_error(format!("createAccount failed: {e}")))?;

    let community_did = account.did.clone();

    // Persist credentials immediately so any partial failure below is still
    // recoverable via a follow-up call.
    upsert_credentials_fn(
        community_did.clone(),
        pds_endpoint.clone(),
        account.handle.clone(),
        password.clone(),
    )
    .await
    .map_err(|e| internal_error(format!("failed to persist credentials: {e}")))?;

    // Fresh session for the bootstrap writes — `account.access_jwt` from
    // createAccount is usable directly, but re-using create_session keeps the
    // dependency surface identical for testing.
    let access_jwt = create_session_fn(pds_endpoint.clone(), account.handle.clone(), password)
        .await
        .map_err(|e| pds_error(format!("createSession failed: {e}")))?;

    // Pre-generate rkeys for all bootstrap records up front so each record
    // can embed references to the others before any PDS round-trip.
    let category_rkey = generate_tid();
    let channel_rkey = generate_tid();
    let role_rkey = generate_tid();
    let member_rkey = generate_tid();

    // 1. Community (singleton — rkey is fixed at "self"; categoryOrder
    //    references the not-yet-written category by its pre-generated rkey).
    let community_record = ColibriCommunity {
        r#type: String::from("social.colibri.community"),
        name: input.name.clone(),
        description: input.description.clone().unwrap_or_default(),
        category_order: vec![category_rkey.clone()],
        requires_approval_to_join: input.requires_approval_to_join,
        picture: None,
    };
    let community_ref = write_record(
        &create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
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
    let category_ref = write_record(
        &create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
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
    let channel_ref = write_record(
        &create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
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
    let role_ref = write_record(
        &create_record_fn,
        pds_endpoint.clone(),
        access_jwt.clone(),
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
    let member_ref = write_record(
        &create_record_fn,
        pds_endpoint,
        access_jwt,
        community_did.clone(),
        "social.colibri.member",
        Some(member_rkey),
        &owner_member,
        "member",
    )
    .await?;

    Ok(Json(CreateCommunityResponse {
        did: community_did,
        community: community_ref.uri,
        category: category_ref.uri,
        channel: channel_ref.uri,
        owner_role: role_ref.uri,
        member: member_ref.uri,
    }))
}

/// Serializes a record payload and issues one `createRecord` call. Centralizes
/// the error-translation boilerplate so the five bootstrap writes above all
/// surface the same shape of error.
#[allow(clippy::too_many_arguments)]
async fn write_record<R, T>(
    create_record_fn: &R,
    pds_endpoint: String,
    access_jwt: String,
    repo: String,
    collection: &'static str,
    rkey: Option<String>,
    record: &T,
    label: &'static str,
) -> Result<RecordRef, ErrorResponse>
where
    R: Fn(
        String,
        String,
        String,
        String,
        Option<String>,
        Value,
    ) -> BoxFuture<'static, Result<RecordRef, PdsError>>,
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
            Some((&admin_credentials.user, &admin_credentials.password)),
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

#[post(
    "/xrpc/social.colibri.community.create?<name>&<description>&<requires_approval_to_join>&<auth>"
)]
/// Mints a new community DID on the AppView's PDS and bootstraps it.
pub async fn create(
    name: &str,
    description: Option<&str>,
    requires_approval_to_join: Option<bool>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CreateCommunityResponse>, ErrorResponse> {
    let input = CreateCommunityInput {
        name: name.to_string(),
        description: description.map(|s| s.to_string()),
        requires_approval_to_join: requires_approval_to_join.unwrap_or(true),
    };
    let pds_endpoint = std::env::var("PDS_LOC")
        .map_err(|_| internal_error(String::from("PDS_LOC env var not set")))?;
    let handle_domain = std::env::var("APPVIEW_HANDLE_DOMAIN")
        .map_err(|_| internal_error(String::from("APPVIEW_HANDLE_DOMAIN env var not set")))?;
    let admin_credentials = AdminCredentials {
        user: std::env::var("PDS_ADMIN_USER")
            .map_err(|_| internal_error(String::from("PDS_ADMIN_USER env var not set")))?,
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

    create_with(
        auth.to_string(),
        input,
        handle_domain,
        pds_endpoint,
        admin_credentials,
        verify_auth_boxed,
        create_account_boxed,
        create_session_boxed,
        create_record_boxed,
        upsert,
    )
    .await
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
        }
    }

    fn admin() -> AdminCredentials {
        AdminCredentials {
            user: String::from("admin"),
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

        let result = create_with(
            String::from("token"),
            input(),
            String::from("community.test"),
            String::from("https://pds.example"),
            admin(),
            |_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            move |_, admin, handle, _email, _password| {
                let ac = ac.clone();
                Box::pin(async move {
                    *ac.lock().unwrap() = Some(admin);
                    Ok(CreatedAccount {
                        did: String::from("did:plc:newcomm"),
                        access_jwt: String::from("jwt-from-create"),
                        handle,
                    })
                })
            },
            |_, _, _| Box::pin(async { Ok(String::from("jwt-session")) }),
            move |_, _, _, collection, rkey, record| {
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
            },
            move |did, endpoint, identifier, password| {
                let cc = cc.clone();
                Box::pin(async move {
                    *cc.lock().unwrap() = Some((did, endpoint, identifier, password));
                    Ok(())
                })
            },
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
        assert_eq!(records.len(), 5, "bootstrap writes 5 records");

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

        let creds = credentials_captured.lock().unwrap();
        let saved = creds.as_ref().unwrap();
        assert_eq!(saved.0, "did:plc:newcomm");
        assert_eq!(saved.1, "https://pds.example");

        // Admin credentials reach createAccount so the PDS can mint accounts
        // without an invite code.
        let forwarded_admin = admin_captured.lock().unwrap();
        let forwarded = forwarded_admin.as_ref().unwrap();
        assert_eq!(forwarded.user, "admin");
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
            |_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            |_, _, _, _, _| Box::pin(async { panic!("should not call") }),
            |_, _, _| Box::pin(async { panic!("should not call") }),
            |_, _, _, _, _, _| Box::pin(async { panic!("should not call") }),
            |_, _, _, _| Box::pin(async { panic!("should not call") }),
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
            |_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            |_, _, _, _, _| {
                Box::pin(async {
                    Err(PdsError::BadStatus {
                        status: 503,
                        body: String::from("nope"),
                    })
                })
            },
            |_, _, _| Box::pin(async { panic!("should not call") }),
            |_, _, _, _, _, _| Box::pin(async { panic!("should not call") }),
            |_, _, _, _| Box::pin(async { panic!("should not call") }),
        )
        .await;

        assert!(result.is_err());
        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "UpstreamError");
        assert!(body.message.contains("createAccount"));
    }
}
