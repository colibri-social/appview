//! Minimal `com.atproto.*` HTTP client for writing records on a PDS we hold
//! credentials for. Only the endpoints we actually need are implemented;
//! responses are decoded into structs the rest of the AppView can use directly.

use rand::Rng;
use rand::distributions::Alphanumeric;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::lib::embed_fetch::{self, FetchError};
use crate::lib::http::HTTP;

#[derive(Debug, Error)]
pub enum PdsError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("fetch error: {0}")]
    Fetch(#[from] FetchError),
    #[error("pds returned {status}: {body}")]
    BadStatus { status: u16, body: String },
    /// Reserved for richer response-shape validation in follow-up work.
    #[allow(dead_code)]
    #[error("pds response missing field `{0}`")]
    MissingField(&'static str),
}

/// Why a PDS call failed, coarse enough to be worth telling a client about.
///
/// The distinction that matters is *configuration* versus *operation*
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PdsFailure {
    /// No reply at all: connection refused, DNS failure, TLS error, timeout.
    Unreachable,
    /// Something answered, but it doesn't speak the AT Protocol
    NotAPds,
    /// A real PDS answered with an error.
    Rejected,
}

impl PdsFailure {
    /// Whether this failure means the endpoint itself is misconfigured, rather
    /// than a single request being turned down.
    pub fn is_unavailable(self) -> bool {
        matches!(self, PdsFailure::Unreachable | PdsFailure::NotAPds)
    }
}

/// Error codes a PDS uses to say the identifier/password pair itself is no good.
/// The bluesky PDS only ever raises `AuthenticationRequired` for this (its
/// `InvalidPasswordError` extends `AuthRequiredError` without a custom name), the
/// other two are for third-party implementations.
const INVALID_CREDENTIAL_CODES: [&str; 3] =
    ["AuthenticationRequired", "InvalidLogin", "InvalidPassword"];

/// Error codes meaning the access JWT we presented is no longer usable.
const STALE_TOKEN_CODES: [&str; 2] = ["ExpiredToken", "InvalidToken"];

impl PdsError {
    pub fn classify(&self) -> PdsFailure {
        match self {
            PdsError::Http(e) if e.is_connect() || e.is_timeout() => PdsFailure::Unreachable,
            PdsError::Http(_) => PdsFailure::Rejected,
            PdsError::Fetch(FetchError::Upstream(_) | FetchError::TooManyRedirects) => {
                PdsFailure::Unreachable
            }
            PdsError::Fetch(FetchError::Blocked(_) | FetchError::InvalidUrl(_)) => {
                PdsFailure::NotAPds
            }
            // A PDS answers XRPC errors as JSON with an `error` field. A 4xx
            // carrying anything else means the route doesn't exist there and whatever is at this
            // address isn't a PDS.
            PdsError::BadStatus { status, body }
                if *status < 500 && !looks_like_xrpc_error(body) =>
            {
                PdsFailure::NotAPds
            }
            PdsError::BadStatus { .. } | PdsError::MissingField(_) => PdsFailure::Rejected,
        }
    }

    /// Whether the PDS is telling us the stored identifier/password pair no
    /// longer works, so re-sending what we have will keep failing.
    pub fn is_invalid_credentials(&self) -> bool {
        match self {
            PdsError::BadStatus { status, body } if *status == 401 => xrpc_error_code(body)
                .is_some_and(|code| INVALID_CREDENTIAL_CODES.contains(&code.as_str())),
            _ => false,
        }
    }

    /// Whether the access JWT we presented is expired or unverifiable.
    /// Recoverable by logging in again
    pub fn is_stale_token(&self) -> bool {
        match self {
            PdsError::BadStatus { status, body } if *status == 400 => {
                xrpc_error_code(body).is_some_and(|code| STALE_TOKEN_CODES.contains(&code.as_str()))
            }
            _ => false,
        }
    }
}

/// The `error` code out of an AT Protocol XRPC error envelope
/// (`{"error": "...", "message": "..."}`), or `None` when `body` isn't one.
pub fn xrpc_error_code(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
}

/// Whether `body` is an AT Protocol XRPC error envelope (`{"error": "..."}`).
/// Anything else coming back from an endpoint we expect to be a PDS means it
/// isn't one.
pub fn looks_like_xrpc_error(body: &str) -> bool {
    xrpc_error_code(body).is_some()
}

/// Opaque-from-our-perspective session bundle returned by `createSession`.
/// We hold the access JWT for the duration of one logical operation and
/// re-authenticate per call; this trades a session round-trip for not having
/// to manage refresh tokens at moderation-event scale.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // `handle` is captured for completeness but unread today
pub struct PdsSession {
    #[serde(rename = "accessJwt")]
    pub access_jwt: String,
    pub did: String,
    #[serde(default)]
    pub handle: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // `cid` is captured for completeness but unread today
pub struct RecordRef {
    pub uri: String,
    pub cid: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // `access_jwt` from createAccount unused; we re-login via createSession instead
pub struct CreatedAccount {
    pub did: String,
    #[serde(rename = "accessJwt")]
    pub access_jwt: String,
    pub handle: String,
}

#[derive(Serialize)]
struct CreateSessionBody<'a> {
    identifier: &'a str,
    password: &'a str,
}

#[derive(Serialize)]
struct CreateRecordBody<'a> {
    repo: &'a str,
    collection: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    rkey: Option<&'a str>,
    record: &'a Value,
}

#[derive(Serialize)]
struct PutRecordBody<'a> {
    repo: &'a str,
    collection: &'a str,
    rkey: &'a str,
    record: &'a Value,
}

#[derive(Serialize)]
struct DeleteRecordBody<'a> {
    repo: &'a str,
    collection: &'a str,
    rkey: &'a str,
}

#[derive(Serialize)]
struct CreateAccountBody<'a> {
    handle: &'a str,
    email: &'a str,
    password: &'a str,
    #[serde(rename = "inviteCode")]
    invite_code: &'a str,
}

#[derive(Serialize)]
struct InviteCodeBody {
    #[serde(rename = "useCount")]
    use_count: i32,
}

#[derive(Deserialize)]
struct InviteCodeResponse {
    code: String,
}

/// Calls `com.atproto.server.createSession` and returns the resulting session.
/// Used both as a credential-verification step and to obtain the access JWT
/// needed for subsequent writes.
pub async fn create_session(
    pds_endpoint: &str,
    identifier: &str,
    password: &str,
) -> Result<PdsSession, PdsError> {
    let url = format!(
        "{}/xrpc/com.atproto.server.createSession",
        pds_endpoint.trim_end_matches('/')
    );
    let body = CreateSessionBody {
        identifier,
        password,
    };

    let resp = HTTP.clone().post(url).json(&body).send().await?;
    handle_response::<PdsSession>(resp).await
}

/// Calls `com.atproto.repo.getRecord` and returns the record's `value` (the
/// actual record payload). Used as a fallback when the local `record_data`
/// cache doesn't yet know about a record we care about — e.g. a brand-new
/// community whose `self` record hasn't been backfilled from the firehose.
///
/// Returns `Ok(None)` when the PDS responds with a not-found error;
/// propagates any other HTTP/PDS error.
pub async fn get_record(
    pds_endpoint: &str,
    repo: &str,
    collection: &str,
    rkey: &str,
) -> Result<Option<Value>, PdsError> {
    let resp =
        embed_fetch::guarded_get(&get_record_url(pds_endpoint, repo, collection, rkey)).await?;
    interpret_get_record(resp).await
}

/// Like [`get_record`], but issued with the shared unguarded client because the
/// endpoint is one this AppView is *configured* with (a community's stored
/// `pds_endpoint` on our own `PDS_LOC`) rather than one derived from a DID
/// document. Such an endpoint may legitimately be loopback on a non-default
/// port during development, which the SSRF guard rejects by design
pub async fn get_record_trusted(
    pds_endpoint: &str,
    repo: &str,
    collection: &str,
    rkey: &str,
) -> Result<Option<Value>, PdsError> {
    let resp = HTTP
        .clone()
        .get(get_record_url(pds_endpoint, repo, collection, rkey))
        .send()
        .await?;
    interpret_get_record(resp).await
}

fn get_record_url(pds_endpoint: &str, repo: &str, collection: &str, rkey: &str) -> String {
    format!(
        "{}/xrpc/com.atproto.repo.getRecord?repo={repo}&collection={collection}&rkey={rkey}",
        pds_endpoint.trim_end_matches('/')
    )
}

/// Shared response handling for both `getRecord` variants: a missing record is
/// `Ok(None)`, everything else is an error or the record value.
async fn interpret_get_record(resp: reqwest::Response) -> Result<Option<Value>, PdsError> {
    #[derive(Deserialize)]
    struct GetRecordResponse {
        value: Value,
    }

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if resp.status() == reqwest::StatusCode::BAD_REQUEST {
        // atproto returns 400 with `error: "RecordNotFound"` for missing
        // records on some PDS implementations; treat that as a soft miss.
        let body = resp.text().await.unwrap_or_default();
        if body.contains("RecordNotFound") {
            return Ok(None);
        }
        return Err(PdsError::BadStatus { status: 400, body });
    }
    let parsed: GetRecordResponse = handle_response(resp).await?;
    Ok(Some(parsed.value))
}

/// Calls `com.atproto.repo.createRecord` and returns the new record's
/// URI + CID. Pass `Some(rkey)` to pin the record at a specific rkey
/// (e.g. `"self"` for singleton records); pass `None` to let the PDS
/// generate a TID.
pub async fn create_record(
    pds_endpoint: &str,
    access_jwt: &str,
    repo: &str,
    collection: &str,
    rkey: Option<&str>,
    record: &Value,
) -> Result<RecordRef, PdsError> {
    let url = format!(
        "{}/xrpc/com.atproto.repo.createRecord",
        pds_endpoint.trim_end_matches('/')
    );
    let body = CreateRecordBody {
        repo,
        collection,
        rkey,
        record,
    };

    let resp = HTTP
        .clone()
        .post(url)
        .bearer_auth(access_jwt)
        .json(&body)
        .send()
        .await?;
    handle_response::<RecordRef>(resp).await
}

/// Calls `com.atproto.repo.putRecord` to create or overwrite a record at a
/// specific rkey. Used by community-management endpoints that update existing
/// singleton-like records (e.g. the community's `"self"` record, a category,
/// a channel).
pub async fn put_record(
    pds_endpoint: &str,
    access_jwt: &str,
    repo: &str,
    collection: &str,
    rkey: &str,
    record: &Value,
) -> Result<RecordRef, PdsError> {
    let url = format!(
        "{}/xrpc/com.atproto.repo.putRecord",
        pds_endpoint.trim_end_matches('/')
    );
    let body = PutRecordBody {
        repo,
        collection,
        rkey,
        record,
    };

    let resp = HTTP
        .clone()
        .post(url)
        .bearer_auth(access_jwt)
        .json(&body)
        .send()
        .await?;
    handle_response::<RecordRef>(resp).await
}

/// Calls `com.atproto.repo.deleteRecord`. Used by member-record revocation
/// on ban / kick / self-leave (see `lib::moderation::revoke_community_member`).
pub async fn delete_record(
    pds_endpoint: &str,
    access_jwt: &str,
    repo: &str,
    collection: &str,
    rkey: &str,
) -> Result<(), PdsError> {
    let url = format!(
        "{}/xrpc/com.atproto.repo.deleteRecord",
        pds_endpoint.trim_end_matches('/')
    );
    let body = DeleteRecordBody {
        repo,
        collection,
        rkey,
    };

    let resp = HTTP
        .clone()
        .post(url)
        .bearer_auth(access_jwt)
        .json(&body)
        .send()
        .await?;

    if resp.status().is_success() {
        return Ok(());
    }
    Err(error_from_response(resp).await)
}

/// Calls `com.atproto.repo.uploadBlob`. Uploads raw bytes with the supplied
/// `mime_type` (sent verbatim as the `Content-Type` header — atproto's
/// uploadBlob reads the type off the header rather than from a JSON
/// wrapper). Returns the inner `blob` object the PDS issues back, ready to
/// embed verbatim into a blob-typed record field.
pub async fn upload_blob(
    pds_endpoint: &str,
    access_jwt: &str,
    bytes: Vec<u8>,
    mime_type: &str,
) -> Result<Value, PdsError> {
    let url = format!(
        "{}/xrpc/com.atproto.repo.uploadBlob",
        pds_endpoint.trim_end_matches('/')
    );

    let resp = HTTP
        .clone()
        .post(url)
        .bearer_auth(access_jwt)
        .header(reqwest::header::CONTENT_TYPE, mime_type)
        .body(bytes)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(error_from_response(resp).await);
    }
    let envelope: Value = resp.json().await.map_err(PdsError::Http)?;
    envelope
        .get("blob")
        .cloned()
        .ok_or(PdsError::MissingField("blob"))
}

/// Calls `com.atproto.server.createAccount`. Used when the AppView mints a new
/// community DID on its own PDS.
///
/// `admin_password` is `Some(password)` to send an `Authorization: Basic …`
/// header (`admin:<password>`), which bypasses invite-code requirements and
/// lets the AppView act as a PDS administrator. Variant A registration
/// always passes one.
pub async fn create_account(
    pds_endpoint: &str,
    admin_password: Option<&str>,
    handle: &str,
    email: &str,
    password: &str,
) -> Result<CreatedAccount, PdsError> {
    let invite_code_url = format!(
        "{}/xrpc/com.atproto.server.createInviteCode",
        pds_endpoint.trim_end_matches('/')
    );

    let invite_code_body = InviteCodeBody { use_count: 1 };

    let mut invite_code_req = HTTP.clone().post(invite_code_url).json(&invite_code_body);

    if let Some(pass) = admin_password {
        invite_code_req = invite_code_req.basic_auth("admin", Some(pass));
    }

    let invite_code_resp = invite_code_req
        .send()
        .await?
        .json::<InviteCodeResponse>()
        .await?;

    let url = format!(
        "{}/xrpc/com.atproto.server.createAccount",
        pds_endpoint.trim_end_matches('/')
    );
    let body = CreateAccountBody {
        handle,
        email,
        password,
        invite_code: &invite_code_resp.code,
    };

    let req = HTTP.clone().post(url).json(&body);

    let resp = req.send().await?;
    handle_response::<CreatedAccount>(resp).await
}

/// Calls `com.atproto.admin.deleteAccount`, tearing down the account (and its
/// entire repo) for `did`. Used when an AppView-managed community is deleted.
///
/// `admin_password` authenticates as the PDS administrator via an
/// `Authorization: Basic admin:<password>` header — the same admin identity
/// used to mint accounts in [`create_account`]. Only works against a PDS the
/// AppView administers; BYO communities on external PDSs cannot be torn down
/// this way.
pub async fn admin_delete_account(
    pds_endpoint: &str,
    admin_password: &str,
    did: &str,
) -> Result<(), PdsError> {
    #[derive(Serialize)]
    struct DeleteAccountBody<'a> {
        did: &'a str,
    }

    let url = format!(
        "{}/xrpc/com.atproto.admin.deleteAccount",
        pds_endpoint.trim_end_matches('/')
    );
    let body = DeleteAccountBody { did };

    let resp = HTTP
        .clone()
        .post(url)
        .basic_auth("admin", Some(admin_password))
        .json(&body)
        .send()
        .await?;

    if resp.status().is_success() {
        return Ok(());
    }
    Err(error_from_response(resp).await)
}

/// Calls `com.atproto.admin.updateAccountPassword`, replacing the account
/// password for `did`. This is how the AppView recovers write access to a
/// community it provisioned when the stored password is gone or no longer works:
/// it holds PDS admin over those accounts, so it can always mint itself a new
/// one.
pub async fn admin_update_account_password(
    pds_endpoint: &str,
    admin_password: &str,
    did: &str,
    new_password: &str,
) -> Result<(), PdsError> {
    #[derive(Serialize)]
    struct UpdatePasswordBody<'a> {
        did: &'a str,
        password: &'a str,
    }

    let url = format!(
        "{}/xrpc/com.atproto.admin.updateAccountPassword",
        pds_endpoint.trim_end_matches('/')
    );
    let body = UpdatePasswordBody {
        did,
        password: new_password,
    };

    let resp = HTTP
        .clone()
        .post(url)
        .basic_auth("admin", Some(admin_password))
        .json(&body)
        .send()
        .await?;

    if resp.status().is_success() {
        return Ok(());
    }
    Err(error_from_response(resp).await)
}

/// Calls `com.atproto.admin.getAccountInfo`. `Ok(None)` means this PDS doesn't
/// host `did`
pub async fn admin_get_account_info(
    pds_endpoint: &str,
    admin_password: &str,
    did: &str,
) -> Result<Option<Value>, PdsError> {
    let url = format!(
        "{}/xrpc/com.atproto.admin.getAccountInfo?did={did}",
        pds_endpoint.trim_end_matches('/')
    );

    let resp = HTTP
        .clone()
        .get(url)
        .basic_auth("admin", Some(admin_password))
        .send()
        .await?;

    if resp.status().is_success() {
        return resp.json::<Value>().await.map(Some).map_err(PdsError::Http);
    }

    let err = error_from_response(resp).await;
    if let PdsError::BadStatus { status, body } = &err
        && (*status == 404
            || xrpc_error_code(body)
                .is_some_and(|code| code == "NotFound" || code == "AccountNotFound"))
    {
        return Ok(None);
    }
    Err(err)
}

/// Generates a long random password suitable for AppView-minted accounts.
/// The AppView stores it encrypted; humans never need to type it.
pub fn generate_strong_password() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(48)
        .map(char::from)
        .collect()
}

async fn handle_response<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, PdsError> {
    if resp.status().is_success() {
        return resp.json::<T>().await.map_err(PdsError::Http);
    }
    Err(error_from_response(resp).await)
}

async fn error_from_response(resp: reqwest::Response) -> PdsError {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    PdsError::BadStatus { status, body }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use wiremock::matchers::{basic_auth, body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// An XRPC error envelope at `status`, the shape every PDS rejection takes.
    fn xrpc_status(status: u16, code: &str) -> PdsError {
        PdsError::BadStatus {
            status,
            body: format!(r#"{{"error":"{code}","message":"whatever"}}"#),
        }
    }

    #[test]
    fn an_html_404_means_the_endpoint_is_not_a_pds() {
        let err = PdsError::BadStatus {
            status: 404,
            body: String::from("<!doctype html><title>Not found</title>"),
        };
        assert_eq!(err.classify(), PdsFailure::NotAPds);
        assert!(err.classify().is_unavailable());
    }

    #[test]
    fn an_xrpc_error_envelope_is_a_rejection() {
        let err = PdsError::BadStatus {
            status: 400,
            body: String::from(r#"{"error":"InvalidRequest","message":"bad handle"}"#),
        };
        assert_eq!(err.classify(), PdsFailure::Rejected);
        assert!(!err.classify().is_unavailable());
    }

    #[test]
    fn a_non_json_5xx_stays_a_rejection() {
        // A real PDS behind a reverse proxy serves HTML gateway errors; that
        // must not read as "this isn't a PDS".
        let err = PdsError::BadStatus {
            status: 503,
            body: String::from("<html>502 Bad Gateway</html>"),
        };
        assert_eq!(err.classify(), PdsFailure::Rejected);
    }

    #[test]
    fn the_ssrf_guard_refusing_the_endpoint_is_a_misconfiguration() {
        let err = PdsError::Fetch(FetchError::Blocked(String::from("127.0.0.1")));
        assert_eq!(err.classify(), PdsFailure::NotAPds);
    }

    #[test]
    fn a_transport_failure_is_unreachable() {
        let err = PdsError::Fetch(FetchError::Upstream(String::from("connection refused")));
        assert_eq!(err.classify(), PdsFailure::Unreachable);
        assert!(err.classify().is_unavailable());
    }

    #[test]
    fn a_wrong_password_is_the_recoverable_credential_failure() {
        // Byte-for-byte what the bluesky PDS answers `createSession` with.
        let err = PdsError::BadStatus {
            status: 401,
            body: String::from(
                r#"{"error":"AuthenticationRequired","message":"Invalid identifier or password"}"#,
            ),
        };
        assert!(err.is_invalid_credentials());
        assert!(!err.is_stale_token());
    }

    #[test]
    fn third_party_credential_codes_are_allowlisted_too() {
        assert!(xrpc_status(401, "InvalidLogin").is_invalid_credentials());
        assert!(xrpc_status(401, "InvalidPassword").is_invalid_credentials());
    }

    /// These are the ones that would wrongly reach the PDS admin API if this
    /// matched on status alone: `AccountTakedown` and `AuthFactorTokenRequired`
    /// are *also* 401s, and no fresh password fixes any of them.
    #[test]
    fn other_auth_failures_never_look_like_bad_credentials() {
        for err in [
            xrpc_status(401, "AccountTakedown"),
            xrpc_status(401, "AuthFactorTokenRequired"),
            xrpc_status(429, "RateLimitExceeded"),
            xrpc_status(400, "InvalidRequest"),
            xrpc_status(500, "InternalServerError"),
            PdsError::BadStatus {
                status: 401,
                body: String::from("<html>nope</html>"),
            },
        ] {
            assert!(
                !err.is_invalid_credentials(),
                "{err:?} must not read as bad credentials"
            );
        }
    }

    #[test]
    fn an_expired_token_is_a_stale_token_at_status_400() {
        // Both are `InvalidRequestError` upstream, so they arrive as 400 rather
        // than the 401 an auth failure would suggest.
        assert!(xrpc_status(400, "ExpiredToken").is_stale_token());
        assert!(xrpc_status(400, "InvalidToken").is_stale_token());
        assert!(!xrpc_status(401, "AuthenticationRequired").is_stale_token());
    }

    /// The invariant that keeps a token blip from ever rotating a password.
    #[test]
    fn stale_tokens_and_bad_credentials_are_disjoint() {
        for err in [
            xrpc_status(401, "AuthenticationRequired"),
            xrpc_status(400, "ExpiredToken"),
            xrpc_status(400, "InvalidToken"),
            xrpc_status(401, "AccountTakedown"),
            xrpc_status(429, "RateLimitExceeded"),
        ] {
            assert!(
                !(err.is_invalid_credentials() && err.is_stale_token()),
                "{err:?} satisfied both predicates"
            );
        }
    }

    #[test]
    fn xrpc_error_code_reads_the_envelope() {
        assert_eq!(
            xrpc_error_code(r#"{"error":"ExpiredToken","message":"Token has expired"}"#).as_deref(),
            Some("ExpiredToken")
        );
        assert!(xrpc_error_code("<!doctype html><title>Not found</title>").is_none());
        // The wrapper's existing behaviour is unchanged.
        assert!(looks_like_xrpc_error(r#"{"error":"InvalidRequest"}"#));
        assert!(!looks_like_xrpc_error("<html>502 Bad Gateway</html>"));
    }

    #[tokio::test]
    async fn admin_update_account_password_authenticates_as_admin() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.admin.updateAccountPassword"))
            .and(basic_auth("admin", "hunter2"))
            .and(body_json(serde_json::json!({
                "did": "did:plc:comm",
                "password": "fresh-password",
            })))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        admin_update_account_password(&server.uri(), "hunter2", "did:plc:comm", "fresh-password")
            .await
            .expect("the admin call should succeed");
    }

    #[tokio::test]
    async fn admin_update_account_password_surfaces_a_rejected_admin_password() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.admin.updateAccountPassword"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_json(serde_json::json!({ "error": "AuthenticationRequired" })),
            )
            .mount(&server)
            .await;

        let err = admin_update_account_password(&server.uri(), "wrong", "did:plc:comm", "p")
            .await
            .expect_err("a bad admin password must not look like success");
        assert!(matches!(err, PdsError::BadStatus { status: 401, .. }));
    }

    #[tokio::test]
    async fn admin_get_account_info_reports_an_unhosted_did_as_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.admin.getAccountInfo"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "NotFound",
                "message": "Account not found",
            })))
            .mount(&server)
            .await;

        let info = admin_get_account_info(&server.uri(), "hunter2", "did:plc:nope")
            .await
            .unwrap();
        assert!(info.is_none());
    }

    #[tokio::test]
    async fn get_record_trusted_reaches_a_loopback_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.repo.getRecord"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "uri": "at://did:plc:comm/social.colibri.community/self",
                "value": { "name": "Local" }
            })))
            .mount(&server)
            .await;

        let value = get_record_trusted(
            &server.uri(),
            "did:plc:comm",
            "social.colibri.community",
            "self",
        )
        .await
        .unwrap()
        .expect("record should be found");

        assert_eq!(value["name"], "Local");
    }

    #[tokio::test]
    async fn get_record_refuses_a_loopback_endpoint() {
        let server = MockServer::start().await;
        let err = get_record(
            &server.uri(),
            "did:plc:comm",
            "social.colibri.community",
            "self",
        )
        .await
        .expect_err("the guarded path must reject a private address");

        assert!(
            matches!(err, PdsError::Fetch(_)),
            "expected a guard rejection, got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_record_trusted_treats_missing_records_as_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.repo.getRecord"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "RecordNotFound",
                "message": "Could not locate record"
            })))
            .mount(&server)
            .await;

        let found = get_record_trusted(
            &server.uri(),
            "did:plc:comm",
            "social.colibri.community",
            "self",
        )
        .await
        .unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn generate_strong_password_is_48_alphanumeric_chars() {
        let pw = generate_strong_password();
        assert_eq!(pw.len(), 48);
        assert!(pw.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(pw, generate_strong_password(), "should differ each call");
    }

    #[test]
    fn deserializes_session_response() {
        let raw = r#"{"accessJwt":"jwt","did":"did:plc:abc","handle":"h.test"}"#;
        let session: PdsSession = serde_json::from_str(raw).unwrap();
        assert_eq!(session.access_jwt, "jwt");
        assert_eq!(session.did, "did:plc:abc");
        assert_eq!(session.handle.as_deref(), Some("h.test"));
    }

    #[test]
    fn deserializes_session_response_without_handle() {
        let raw = r#"{"accessJwt":"jwt","did":"did:plc:abc"}"#;
        let session: PdsSession = serde_json::from_str(raw).unwrap();
        assert!(session.handle.is_none());
    }

    #[test]
    fn deserializes_record_ref() {
        let raw = r#"{"uri":"at://did:plc:abc/social.colibri.community/c1","cid":"bafy..."}"#;
        let r: RecordRef = serde_json::from_str(raw).unwrap();
        assert_eq!(r.uri, "at://did:plc:abc/social.colibri.community/c1");
    }
}
