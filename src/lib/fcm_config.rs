//! Firebase Cloud Messaging (FCM) service-account configuration.
//!
//! The AppView owns a single Firebase service-account key (downloaded from
//! Firebase console → Project Settings → Service Accounts), stored whole as
//! `FCM_SERVICE_ACCOUNT_JSON` rather than split into separate env vars — the
//! private key inside it is a multi-line PEM, and splitting it out risks
//! env-escaping bugs across deploy targets. Configuration is optional — when
//! unset, FCM delivery is disabled and `fcm_config()` returns `None`,
//! mirroring `vapid_config`.

use std::sync::OnceLock;

use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct FcmConfig {
    /// The Firebase/GCP project ID FCM messages are sent under.
    pub project_id: String,
    /// The service account's client email, used as the JWT `iss` claim.
    pub client_email: String,
    /// PEM-encoded RSA private key, used to sign the JWT-bearer assertion.
    pub private_key: String,
}

#[derive(Deserialize)]
struct ServiceAccountJson {
    project_id: String,
    client_email: String,
    private_key: String,
}

static FCM: OnceLock<Option<FcmConfig>> = OnceLock::new();

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn load_from_env() -> Option<FcmConfig> {
    let raw = env_non_empty("FCM_SERVICE_ACCOUNT_JSON")?;
    let parsed: ServiceAccountJson = serde_json::from_str(&raw).ok()?;
    Some(FcmConfig {
        project_id: parsed.project_id,
        client_email: parsed.client_email,
        private_key: parsed.private_key,
    })
}

/// Returns the configured Firebase service account, or `None` when FCM is not
/// configured. Memoized on first read.
pub fn fcm_config() -> Option<&'static FcmConfig> {
    FCM.get_or_init(load_from_env).as_ref()
}

/// Parses `private_key` as a PEM-encoded RSA key, without minting a token.
/// Called at boot so a malformed `FCM_SERVICE_ACCOUNT_JSON` fails fast
/// instead of silently failing every FCM send later.
pub fn validate_private_key(private_key: &str) -> Result<(), String> {
    jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_private_key_rejects_malformed_key() {
        assert!(validate_private_key("not-a-valid-key").is_err());
    }

    #[test]
    fn load_from_env_parses_service_account_json() {
        // SAFETY: tests in this crate run single-threaded per module and this
        // var isn't read anywhere else concurrently within this test.
        let raw = serde_json::json!({
            "type": "service_account",
            "project_id": "colibri-test",
            "client_email": "fcm@colibri-test.iam.gserviceaccount.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMII...\n-----END PRIVATE KEY-----\n",
            "private_key_id": "abc123",
        })
        .to_string();

        let parsed: Result<ServiceAccountJson, _> = serde_json::from_str(&raw);
        let parsed = parsed.expect("well-formed service account JSON should parse");
        assert_eq!(parsed.project_id, "colibri-test");
        assert_eq!(
            parsed.client_email,
            "fcm@colibri-test.iam.gserviceaccount.com"
        );
    }

    #[test]
    fn load_from_env_rejects_malformed_json() {
        let parsed: Result<ServiceAccountJson, _> = serde_json::from_str("not json");
        assert!(parsed.is_err());
    }
}
