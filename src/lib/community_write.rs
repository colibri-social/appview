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

use futures::future::BoxFuture;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::community_credentials::{self, CommunityCredentials};
use crate::lib::community_session_cache;
use crate::lib::credential_recovery;
use crate::lib::crypto;
use crate::lib::pds_client::{self, PdsError};
use crate::lib::responses::{self, ErrorCode, ErrorResponse};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data;

// ---- Error helpers ---------------------------------------------------------

/// Preserves an inner [`DbErr`] rather than stringifying it, so callers can still
/// tell a database failure from a credentials one.
pub fn creds_err_to_db(e: community_credentials::CredentialsError) -> DbErr {
    match e {
        community_credentials::CredentialsError::Db(inner) => inner,
        other => DbErr::Custom(format!("credentials error: {other}")),
    }
}

pub fn pds_err_to_db(e: PdsError) -> DbErr {
    let message = format!("pds write failed: {e}");
    if e.classify().is_unavailable() {
        return DbErr::Custom(format!("{}{message}", responses::PDS_UNAVAILABLE_MARKER));
    }
    DbErr::Custom(message)
}

pub fn not_found_error(message: impl Into<String>) -> ErrorResponse {
    ErrorCode::NotFound.with(message.into())
}

pub fn invalid_request(message: impl Into<String>) -> ErrorResponse {
    ErrorCode::InvalidRequest.with(message.into())
}

// ---- Session helper --------------------------------------------------------

/// Returns `(pds_endpoint, access_jwt)` for `community_did`, ready for immediate
/// PDS calls.
pub async fn community_session(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<(String, String), DbErr> {
    if let Some(session) = community_session_cache::get(community_did) {
        return Ok(session);
    }
    fresh_session(db, community_did).await
}

/// Authenticates from scratch, ignoring any cached token.
async fn fresh_session(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<(String, String), DbErr> {
    match community_credentials::load_credentials(db, crypto::master_key(), community_did).await {
        Ok(Some(creds)) => login_or_recover(db, community_did, creds).await,

        // No row at all. If the community still lives on our own PDS we can mint
        // ourselves fresh credentials for it; otherwise it genuinely isn't ours and
        // the caller gets the usual missing-credentials error.
        Ok(None) => match credential_recovery::adopt_orphan_session(db, community_did).await? {
            Some(session) => Ok(session),
            None => Err(community_credentials::missing_credentials_err(
                community_did,
            )),
        },

        // The row exists but its password can never be read back — a rotated
        // `CREDENTIAL_ENCRYPTION_KEY`, say. There is nothing to retry and nothing to
        // compare against, so go straight to minting a replacement.
        Err(e) if e.is_undecryptable() => {
            log::warn!(
                "stored password for community {community_did} cannot be decrypted ({e}); \
                 attempting to mint a replacement"
            );
            match credential_recovery::recover_session(db, community_did, None).await? {
                Some(session) => Ok(session),
                None => Err(creds_err_to_db(e)),
            }
        }

        Err(e) => Err(creds_err_to_db(e)),
    }
}

/// Logs in with `creds`, falling back to minting a new password if the PDS says
/// the ones we hold are no good.
async fn login_or_recover(
    db: &DatabaseConnection,
    community_did: &str,
    creds: CommunityCredentials,
) -> Result<(String, String), DbErr> {
    match pds_client::create_session(
        &creds.pds_endpoint,
        creds.login_identifier(),
        &creds.password,
    )
    .await
    {
        Ok(session) => {
            community_session_cache::put(community_did, &creds.pds_endpoint, &session.access_jwt);
            Ok((creds.pds_endpoint, session.access_jwt))
        }

        Err(e) if e.is_invalid_credentials() => {
            log::warn!(
                "the PDS rejected the stored password for community {community_did}; attempting to \
                 mint a replacement"
            );
            // `Ok(None)` means recovery doesn't apply here (a BYO community, or one
            // on someone else's PDS), so the caller should see the PDS's own
            // refusal rather than a recovery-flavoured error.
            match credential_recovery::recover_session(db, community_did, Some(&creds.password))
                .await?
            {
                Some(session) => Ok(session),
                None => Err(pds_err_to_db(e)),
            }
        }

        Err(e) => Err(pds_err_to_db(e)),
    }
}

/// Runs `op` against the community's PDS with a session, retrying once if the PDS
/// reports the access token stale.
pub async fn with_session<'a, T, F>(
    db: &DatabaseConnection,
    community_did: &str,
    op: F,
) -> Result<T, DbErr>
where
    F: Fn(String, String) -> BoxFuture<'a, Result<T, PdsError>>,
{
    let (endpoint, jwt) = community_session(db, community_did).await?;

    match op(endpoint, jwt).await {
        Ok(value) => Ok(value),

        Err(e) if e.is_stale_token() => {
            log::debug!(
                "cached session for community {community_did} was stale; re-authenticating"
            );
            community_session_cache::invalidate(community_did);

            let (endpoint, jwt) = fresh_session(db, community_did).await?;
            op(endpoint, jwt).await.map_err(pds_err_to_db)
        }

        Err(e) => Err(pds_err_to_db(e)),
    }
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
    let record_ref = with_session(db, community_did, |endpoint, jwt| {
        let data = data.clone();
        Box::pin(async move {
            pds_client::create_record(&endpoint, &jwt, community_did, nsid, rkey, &data).await
        })
    })
    .await?;

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
    with_session(db, community_did, |endpoint, jwt| {
        let data = data.clone();
        Box::pin(async move {
            pds_client::put_record(&endpoint, &jwt, community_did, nsid, rkey, &data).await
        })
    })
    .await?;

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
    with_session(db, community_did, |endpoint, jwt| {
        Box::pin(async move {
            pds_client::delete_record(&endpoint, &jwt, community_did, nsid, rkey).await
        })
    })
    .await?;

    cache_delete(db, community_did, nsid, rkey).await;
    Ok(())
}

pub const ALLOWED_PICTURE_MIME_TYPES: &[&str] =
    &["image/jpeg", "image/png", "image/gif", "image/webp"];

pub fn is_allowed_picture_mime(content_type: &str) -> bool {
    let base = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    ALLOWED_PICTURE_MIME_TYPES.contains(&base.as_str())
}

/// Uploads a blob to the community's PDS and returns the `blob` object the PDS
/// issues back, ready to embed in a record field
pub async fn upload_blob(
    db: &DatabaseConnection,
    community_did: &str,
    bytes: Vec<u8>,
    mime_type: &str,
) -> Result<Value, DbErr> {
    with_session(db, community_did, |endpoint, jwt| {
        let bytes = bytes.clone();
        Box::pin(async move { pds_client::upload_blob(&endpoint, &jwt, bytes, mime_type).await })
    })
    .await
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
pub async fn cache_upsert(db: &DatabaseConnection, did: &str, nsid: &str, rkey: &str, data: Value) {
    let active = record_data::ActiveModel {
        did: ActiveValue::Set(did.to_string()),
        nsid: ActiveValue::Set(nsid.to_string()),
        rkey: ActiveValue::Set(rkey.to_string()),
        data: ActiveValue::Set(data),
        indexed_at: ActiveValue::Set(current_iso8601_utc()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::lib::community_credentials::{SOURCE_APPVIEW_MANAGED, SOURCE_BYO};
    use crate::lib::test_fixtures::PdsEnvGuard;
    use crate::models::community_credentials as credentials_model;

    const ADMIN_PASS: &str = "admin-secret";
    const STORED_PASSWORD: &str = "the-stored-password";

    /// The all-zeros key every test module in this crate installs. Whichever test
    /// runs first wins the `OnceLock`, so they must all agree
    fn install_key() -> Vec<u8> {
        let key = vec![0u8; 32];
        let _ = crypto::install_master_key(key.clone());
        key
    }

    /// Distinct DID per test: the session cache and the recovery attempt gate are
    /// process-global and cargo runs these on parallel threads.
    fn did_for(tag: &str) -> String {
        format!("did:plc:write-{tag}")
    }

    fn credentials_row(did: &str, pds_endpoint: &str, source: &str) -> credentials_model::Model {
        let (ciphertext, nonce) = crypto::encrypt(STORED_PASSWORD.as_bytes(), &install_key())
            .expect("fixture encryption should succeed");

        credentials_model::Model {
            community_did: did.to_string(),
            pds_endpoint: pds_endpoint.to_string(),
            identifier: String::from("c-abc.test"),
            password_ciphertext_b64: BASE64.encode(&ciphertext),
            password_nonce_b64: BASE64.encode(&nonce),
            source: source.to_string(),
            created_at: String::from("2026-07-29T00:00:00Z"),
        }
    }

    fn session_body(jwt: &str, did: &str) -> serde_json::Value {
        serde_json::json!({ "accessJwt": jwt, "did": did, "handle": "c-abc.test" })
    }

    fn unauthorized() -> ResponseTemplate {
        ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "AuthenticationRequired",
            "message": "Invalid identifier or password",
        }))
    }

    /// How many times the mock PDS was asked for a session.
    async fn login_count(server: &MockServer) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|req| req.url.path() == "/xrpc/com.atproto.server.createSession")
            .count()
    }

    /// How many times a password rotation was attempted.
    async fn rotation_count(server: &MockServer) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|req| req.url.path() == "/xrpc/com.atproto.admin.updateAccountPassword")
            .count()
    }

    /// The `(identifier, password)` each `createSession` call presented, in order.
    async fn login_attempts(server: &MockServer) -> Vec<(String, String)> {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|req| req.url.path() == "/xrpc/com.atproto.server.createSession")
            .filter_map(|req| serde_json::from_slice::<serde_json::Value>(&req.body).ok())
            .filter_map(|body: serde_json::Value| {
                Some((
                    body["identifier"].as_str()?.to_owned(),
                    body["password"].as_str()?.to_owned(),
                ))
            })
            .collect()
    }

    /// The `identifier` each `createSession` call presented.
    async fn login_identifiers(server: &MockServer) -> Vec<String> {
        login_attempts(server)
            .await
            .into_iter()
            .map(|(identifier, _)| identifier)
            .collect()
    }

    /// A managed community logs in by DID, so a stale handle can never be mistaken
    /// for a broken password.
    #[tokio::test]
    async fn a_managed_community_logs_in_by_did() {
        let did = did_for("login-by-did");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body("jwt-1", &did)))
            .mount(&server)
            .await;

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![credentials_row(
                &did,
                &server.uri(),
                SOURCE_APPVIEW_MANAGED,
            )]])
            .into_connection();

        let (_endpoint, jwt) = community_session(&db, &did).await.unwrap();
        assert_eq!(jwt, "jwt-1");
        assert_eq!(login_identifiers(&server).await, vec![did.clone()]);

        community_session_cache::invalidate(&did);
    }

    /// A BYO community keeps its stored identifier: a third-party PDS need not
    /// accept DIDs, and the secret may be an app password tied to the handle.
    #[tokio::test]
    async fn a_byo_community_logs_in_by_its_stored_identifier() {
        let did = did_for("byo-identifier");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body("jwt-byo", &did)))
            .mount(&server)
            .await;

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![credentials_row(&did, &server.uri(), SOURCE_BYO)]])
            .into_connection();

        community_session(&db, &did).await.unwrap();
        assert_eq!(
            login_identifiers(&server).await,
            vec![String::from("c-abc.test")]
        );

        community_session_cache::invalidate(&did);
    }

    /// The headline behaviour: a rejected password is repaired without the caller
    /// noticing.
    #[tokio::test]
    async fn a_rejected_password_is_rotated_and_the_write_proceeds() {
        let did = did_for("rotate");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        // First login fails with the stored password, the second succeeds with the
        // freshly minted one.
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(unauthorized())
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body("jwt-new", &did)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.admin.updateAccountPassword"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let row = credentials_row(&did, &server.uri(), SOURCE_APPVIEW_MANAGED);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            // load_credentials, then hosted_on_managed_pds, then the peer re-read.
            .append_query_results([vec![row.clone()], vec![row.clone()], vec![row.clone()]])
            // update_password
            .append_exec_results([MockExecResult {
                last_insert_id: 0,
                rows_affected: 1,
            }])
            .into_connection();

        let (_endpoint, jwt) = community_session(&db, &did)
            .await
            .expect("recovery should have restored access");

        assert_eq!(jwt, "jwt-new");
        assert_eq!(rotation_count(&server).await, 1);

        let attempts = login_attempts(&server).await;
        assert_eq!(attempts.len(), 2, "one failed login, then one that worked");
        assert_eq!(
            attempts[0].1, STORED_PASSWORD,
            "the first tries what we held"
        );
        assert_ne!(
            attempts[1].1, STORED_PASSWORD,
            "the retry must present the freshly minted password"
        );
        // Both attempts identify the community by DID, never by its stale handle.
        assert!(attempts.iter().all(|(identifier, _)| identifier == &did));

        community_session_cache::invalidate(&did);
    }

    /// BYO credentials live on a PDS we hold no admin over, so a rejection there
    /// must surface rather than triggering an admin call.
    #[tokio::test]
    async fn a_byo_rejection_never_reaches_the_admin_api() {
        let did = did_for("byo-rejected");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(unauthorized())
            .mount(&server)
            .await;

        let row = credentials_row(&did, &server.uri(), SOURCE_BYO);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row.clone()], vec![row]])
            .into_connection();

        community_session(&db, &did)
            .await
            .expect_err("a BYO rejection is terminal");

        assert_eq!(
            rotation_count(&server).await,
            0,
            "a BYO community's password must never be rotated"
        );
    }

    /// A managed row pointing somewhere other than our configured PDS is not ours
    /// to repair — and the admin secret must not be sent to it.
    #[tokio::test]
    async fn a_managed_row_on_a_foreign_pds_is_not_recovered() {
        let did = did_for("foreign-pds");
        install_key();
        let server = MockServer::start().await;
        // PDS_LOC deliberately points somewhere else than the row does.
        let _pds_env = PdsEnvGuard::with_admin_password("https://not-our-pds.example", ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(unauthorized())
            .mount(&server)
            .await;

        let row = credentials_row(&did, &server.uri(), SOURCE_APPVIEW_MANAGED);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row.clone()], vec![row]])
            .into_connection();

        community_session(&db, &did).await.expect_err("not ours");
        assert_eq!(rotation_count(&server).await, 0);
    }

    /// Without an admin password there is no authority to rotate with, so the
    /// caller keeps the PDS's own refusal.
    #[tokio::test]
    async fn recovery_is_unavailable_without_an_admin_password() {
        let did = did_for("no-admin-pass");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::without_admin_password(&server.uri());
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(unauthorized())
            .mount(&server)
            .await;

        let row = credentials_row(&did, &server.uri(), SOURCE_APPVIEW_MANAGED);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row.clone()], vec![row]])
            .into_connection();

        let err = community_session(&db, &did)
            .await
            .expect_err("no authority");
        assert!(
            err.to_string().contains("401"),
            "the PDS's own refusal should survive: {err}"
        );
        assert_eq!(rotation_count(&server).await, 0);
    }

    /// A community with no row at all, whose DID we cannot show lives on our PDS,
    /// keeps today's missing-credentials error.
    #[tokio::test]
    async fn a_community_we_hold_no_row_for_is_left_alone() {
        let did = did_for("no-row");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([
                Vec::<credentials_model::Model>::new(),
                Vec::<credentials_model::Model>::new(),
            ])
            .into_connection();

        let err = community_session(&db, &did)
            .await
            .expect_err("nothing to authenticate with");
        assert!(
            err.to_string().contains("no credentials registered"),
            "unexpected error: {err}"
        );
        assert_eq!(rotation_count(&server).await, 0);
    }

    /// The whole point of caching: a burst of writes costs one login.
    #[tokio::test]
    async fn repeated_writes_reuse_one_session() {
        let did = did_for("reuse");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body("jwt-hot", &did)))
            .mount(&server)
            .await;

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![credentials_row(
                &did,
                &server.uri(),
                SOURCE_APPVIEW_MANAGED,
            )]])
            .into_connection();

        for _ in 0..5 {
            community_session(&db, &did).await.unwrap();
        }

        assert_eq!(
            login_count(&server).await,
            1,
            "five writes should share one session"
        );

        // Invalidating forces exactly one more login.
        community_session_cache::invalidate(&did);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![credentials_row(
                &did,
                &server.uri(),
                SOURCE_APPVIEW_MANAGED,
            )]])
            .into_connection();
        community_session(&db, &did).await.unwrap();
        assert_eq!(login_count(&server).await, 2);

        community_session_cache::invalidate(&did);
    }

    /// A cached token can expire between being stored and being used. That is a
    /// token problem, not a password problem: re-authenticate, never rotate.
    #[tokio::test]
    async fn a_stale_token_is_retried_without_rotating_anything() {
        let did = did_for("stale-token");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body("jwt-a", &did)))
            .mount(&server)
            .await;
        // The first write is told its token expired; the second succeeds.
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "ExpiredToken",
                "message": "Token has expired",
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "uri": format!("at://{did}/social.colibri.category/cat1"),
                "cid": "bafyreiexample",
            })))
            .mount(&server)
            .await;

        let row = credentials_row(&did, &server.uri(), SOURCE_APPVIEW_MANAGED);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row.clone()], vec![row]])
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection();

        let rkey = create_record(
            &db,
            &did,
            "social.colibri.category",
            None,
            serde_json::json!({ "name": "General" }),
        )
        .await
        .expect("the retry should carry the write through");

        assert_eq!(rkey, "cat1");
        assert_eq!(
            login_count(&server).await,
            2,
            "the stale token should have forced exactly one re-authentication"
        );
        assert_eq!(
            rotation_count(&server).await,
            0,
            "an expired token must never rotate a password"
        );

        community_session_cache::invalidate(&did);
    }

    /// A token that is still refused after re-authentication is a real error, not
    /// a race to loop on.
    #[tokio::test]
    async fn the_stale_token_retry_happens_only_once() {
        let did = did_for("stale-twice");
        install_key();
        let server = MockServer::start().await;
        let _pds_env = PdsEnvGuard::with_admin_password(&server.uri(), ADMIN_PASS);
        community_session_cache::invalidate(&did);

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body("jwt-b", &did)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.repo.createRecord"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "ExpiredToken",
            })))
            .mount(&server)
            .await;

        let row = credentials_row(&did, &server.uri(), SOURCE_APPVIEW_MANAGED);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row.clone()], vec![row]])
            .into_connection();

        create_record(
            &db,
            &did,
            "social.colibri.category",
            None,
            serde_json::json!({ "name": "General" }),
        )
        .await
        .expect_err("a persistently rejected token must surface");

        let attempts = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|req| req.url.path() == "/xrpc/com.atproto.repo.createRecord")
            .count();
        assert_eq!(attempts, 2, "one attempt plus exactly one retry");

        community_session_cache::invalidate(&did);
    }

    #[test]
    fn allows_every_picture_format_the_lexicon_accepts() {
        assert!(is_allowed_picture_mime("image/jpeg"));
        assert!(is_allowed_picture_mime("image/png"));
        assert!(is_allowed_picture_mime("image/gif"));
        assert!(is_allowed_picture_mime("image/webp"));
    }

    #[test]
    fn ignores_picture_mime_parameters_and_casing() {
        assert!(is_allowed_picture_mime("image/webp; charset=binary"));
        assert!(is_allowed_picture_mime("image/WebP"));
        assert!(is_allowed_picture_mime("  image/png  "));
    }

    #[test]
    fn rejects_non_picture_mime_types() {
        assert!(!is_allowed_picture_mime("image/svg+xml"));
        assert!(!is_allowed_picture_mime("video/mp4"));
        assert!(!is_allowed_picture_mime("application/octet-stream"));
        assert!(!is_allowed_picture_mime(""));
    }
}
