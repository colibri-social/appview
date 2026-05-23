//! Minimal `com.atproto.*` HTTP client for writing records on a PDS we hold
//! credentials for. Only the endpoints we actually need are implemented;
//! responses are decoded into structs the rest of the AppView can use directly.

use rand::Rng;
use rand::distributions::Alphanumeric;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PdsError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("pds returned {status}: {body}")]
    BadStatus { status: u16, body: String },
    /// Reserved for richer response-shape validation in follow-up work.
    #[allow(dead_code)]
    #[error("pds response missing field `{0}`")]
    MissingField(&'static str),
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

    let resp = reqwest::Client::new().post(url).json(&body).send().await?;
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
    #[derive(Deserialize)]
    struct GetRecordResponse {
        value: Value,
    }

    let url = format!(
        "{}/xrpc/com.atproto.repo.getRecord?repo={repo}&collection={collection}&rkey={rkey}",
        pds_endpoint.trim_end_matches('/')
    );
    let resp = reqwest::Client::new().get(url).send().await?;

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

    let resp = reqwest::Client::new()
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

    let resp = reqwest::Client::new()
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

    let resp = reqwest::Client::new()
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

    let mut invite_code_req = reqwest::Client::new()
        .post(invite_code_url)
        .json(&invite_code_body);

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

    let req = reqwest::Client::new().post(url).json(&body);

    let resp = req.send().await?;
    handle_response::<CreatedAccount>(resp).await
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
