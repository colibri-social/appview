//! FCM HTTP v1 delivery.
//!
//! Kept free of any `DatabaseConnection` so it's unit-testable purely against
//! mocked HTTP — `push_send.rs` owns the DB-side pruning decision, this
//! module only mints tokens, sends, and classifies the response.
//!
//! Auth is Google's service-account JWT-bearer flow: sign a short-lived JWT
//! with the service account's RSA key (`iss`/`scope`/`aud`/`iat`/`exp`
//! claims, RS256), trade it for an OAuth2 access token at
//! `https://oauth2.googleapis.com/token`, then send messages to FCM's HTTP v1
//! API with that token as a bearer credential. The access token is cached for
//! its lifetime (minus a minute of safety margin) rather than re-minted per
//! send.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use rocket::tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

use crate::lib::fcm_config::FcmConfig;

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const FCM_SEND_URL_BASE: &str = "https://fcm.googleapis.com/v1/projects";
const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

/// Outcome of a single FCM send attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Delivered,
    /// The token will never be valid again (app uninstalled / data cleared) —
    /// the caller should prune the subscription.
    Unregistered,
    /// Anything else: quota, malformed payload, transient 5xx. Not pruned.
    Failed(String),
}

#[derive(Serialize)]
struct TokenClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// Mints/caches FCM access tokens and sends FCM HTTP v1 messages. One
/// instance is shared process-wide via [`client`] so the token cache is
/// actually reused across sends; tests construct their own instance pointed
/// at a mock server instead of touching Google's real endpoints.
pub struct FcmClient {
    http: Client,
    token_url: String,
    send_url_base: String,
    cache: Mutex<Option<CachedToken>>,
}

impl FcmClient {
    fn new(token_url: impl Into<String>, send_url_base: impl Into<String>) -> Self {
        Self {
            http: Client::new(),
            token_url: token_url.into(),
            send_url_base: send_url_base.into(),
            cache: Mutex::new(None),
        }
    }

    /// Returns a cached access token if still valid, otherwise mints a fresh
    /// one via the service account's JWT-bearer grant.
    pub async fn access_token(&self, config: &FcmConfig) -> Result<String, String> {
        {
            let guard = self.cache.lock().await;
            if let Some(cached) = guard.as_ref()
                && cached.expires_at > Instant::now()
            {
                return Ok(cached.access_token.clone());
            }
        }

        let response = self.mint_access_token(config).await?;
        let mut guard = self.cache.lock().await;
        *guard = Some(CachedToken {
            access_token: response.access_token.clone(),
            expires_at: Instant::now()
                + Duration::from_secs(response.expires_in.saturating_sub(60)),
        });
        Ok(response.access_token)
    }

    async fn mint_access_token(&self, config: &FcmConfig) -> Result<TokenResponse, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();
        let claims = TokenClaims {
            iss: &config.client_email,
            scope: FCM_SCOPE,
            aud: &self.token_url,
            iat: now,
            exp: now + 3600,
        };
        let key = EncodingKey::from_rsa_pem(config.private_key.as_bytes())
            .map_err(|e| format!("invalid FCM private key: {e}"))?;
        let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key)
            .map_err(|e| format!("failed to sign FCM JWT: {e}"))?;

        let res = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", jwt.as_str()),
            ])
            .send()
            .await
            .map_err(|e| format!("token request failed: {e}"))?;

        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("token endpoint returned {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("malformed token response: {e}"))
    }

    /// Sends a single FCM message. `data` values must all be strings, per
    /// FCM's requirement for the `data` payload map.
    pub async fn send(
        &self,
        config: &FcmConfig,
        access_token: &str,
        registration_token: &str,
        title: &str,
        body: &str,
        data: &HashMap<&str, &str>,
    ) -> SendOutcome {
        let payload = serde_json::json!({
            "message": {
                "token": registration_token,
                "notification": { "title": title, "body": body },
                "data": data,
            }
        });

        let url = format!("{}/{}/messages:send", self.send_url_base, config.project_id);
        let res = match self
            .http
            .post(&url)
            .bearer_auth(access_token)
            .json(&payload)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return SendOutcome::Failed(format!("request failed: {e}")),
        };

        let status = res.status();
        if status.is_success() {
            return SendOutcome::Delivered;
        }

        let body_text = res.text().await.unwrap_or_default();
        if is_unregistered_error(&body_text) {
            return SendOutcome::Unregistered;
        }
        SendOutcome::Failed(format!("fcm returned {status}: {body_text}"))
    }
}

/// FCM's error body shape:
/// `{"error": {"status": "...", "details": [{"errorCode": "..."}]}}`.
/// `NOT_FOUND`/`UNREGISTERED` mean the token is gone for good; anything else
/// (notably `INVALID_ARGUMENT`, which can also fire on our own malformed
/// payload) is left for the caller to log rather than treated as dead.
fn is_unregistered_error(body: &str) -> bool {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let error = &parsed["error"];
    if error["status"].as_str() == Some("NOT_FOUND") {
        return true;
    }
    error["details"].as_array().is_some_and(|details| {
        details
            .iter()
            .any(|d| d["errorCode"].as_str() == Some("UNREGISTERED"))
    })
}

static CLIENT: OnceLock<FcmClient> = OnceLock::new();

/// The process-wide FCM client, sharing one token cache across every send.
pub fn client() -> &'static FcmClient {
    CLIENT.get_or_init(|| FcmClient::new(GOOGLE_TOKEN_URL, FCM_SEND_URL_BASE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config() -> FcmConfig {
        FcmConfig {
            project_id: String::from("colibri-test"),
            client_email: String::from("fcm@colibri-test.iam.gserviceaccount.com"),
            private_key: TEST_PRIVATE_KEY.to_string(),
        }
    }

    // A throwaway 2048-bit RSA key, used only to sign test JWTs against a
    // local mock server — never talks to Google.
    const TEST_PRIVATE_KEY: &str = include_str!("../../tests/fixtures/fcm_test_key.pem");

    #[tokio::test]
    async fn mints_and_caches_access_token_within_ttl() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token-1",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = FcmClient::new(format!("{}/token", server.uri()), server.uri());
        let config = test_config();

        let first = client.access_token(&config).await.unwrap();
        let second = client.access_token(&config).await.unwrap();
        assert_eq!(first, "token-1");
        assert_eq!(second, "token-1");
        // `.expect(1)` above asserts the mock was hit exactly once when the
        // server is dropped/verified at the end of the test.
    }

    #[tokio::test]
    async fn mints_a_fresh_token_after_expiry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token-1",
                "expires_in": 0,
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "token-2",
                "expires_in": 3600,
            })))
            .mount(&server)
            .await;

        let client = FcmClient::new(format!("{}/token", server.uri()), server.uri());
        let config = test_config();

        let first = client.access_token(&config).await.unwrap();
        let second = client.access_token(&config).await.unwrap();
        assert_eq!(first, "token-1");
        assert_eq!(second, "token-2");
    }

    #[tokio::test]
    async fn send_classifies_unregistered_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/colibri-test/messages:send"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": {
                    "status": "NOT_FOUND",
                    "details": [{ "errorCode": "UNREGISTERED" }],
                }
            })))
            .mount(&server)
            .await;

        let client = FcmClient::new(format!("{}/token", server.uri()), server.uri());
        let config = test_config();
        let outcome = client
            .send(
                &config,
                "access-token",
                "dead-token",
                "t",
                "b",
                &HashMap::new(),
            )
            .await;
        assert_eq!(outcome, SendOutcome::Unregistered);
    }

    #[tokio::test]
    async fn send_does_not_prune_on_invalid_argument() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/colibri-test/messages:send"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": { "status": "INVALID_ARGUMENT", "details": [] }
            })))
            .mount(&server)
            .await;

        let client = FcmClient::new(format!("{}/token", server.uri()), server.uri());
        let config = test_config();
        let outcome = client
            .send(
                &config,
                "access-token",
                "some-token",
                "t",
                "b",
                &HashMap::new(),
            )
            .await;
        assert!(matches!(outcome, SendOutcome::Failed(_)));
    }

    #[tokio::test]
    async fn send_reports_failure_on_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/colibri-test/messages:send"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = FcmClient::new(format!("{}/token", server.uri()), server.uri());
        let config = test_config();
        let outcome = client
            .send(
                &config,
                "access-token",
                "some-token",
                "t",
                "b",
                &HashMap::new(),
            )
            .await;
        assert!(matches!(outcome, SendOutcome::Failed(_)));
    }

    #[tokio::test]
    async fn send_delivers_on_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/colibri-test/messages:send"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({ "name": "projects/colibri-test/messages/0" }),
                ),
            )
            .mount(&server)
            .await;

        let client = FcmClient::new(format!("{}/token", server.uri()), server.uri());
        let config = test_config();
        let outcome = client
            .send(
                &config,
                "access-token",
                "some-token",
                "t",
                "b",
                &HashMap::new(),
            )
            .await;
        assert_eq!(outcome, SendOutcome::Delivered);
    }
}
