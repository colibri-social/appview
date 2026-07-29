//! Where to read a repo from, and how far that address is trusted.

use std::net::IpAddr;

use sea_orm::DatabaseConnection;

use crate::lib::community_credentials::{self, SOURCE_APPVIEW_MANAGED};
use crate::lib::embed_fetch;

/// A PDS endpoint plus the provenance that decides which HTTP client may be
/// pointed at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoEndpoint {
    /// This AppView's own `PDS_LOC`
    Trusted(String),
    /// Anything else: a DID-document lookup, or a caller-supplied BYO endpoint.
    /// Must keep going through `embed_fetch::guarded_get`.
    Untrusted(String),
}

impl RepoEndpoint {
    /// The endpoint with any trailing slash removed, ready to concatenate with
    /// an `/xrpc/...` path.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Trusted(endpoint) | Self::Untrusted(endpoint) => endpoint.trim_end_matches('/'),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    #[error("database error: {0}")]
    Db(#[from] sea_orm::DbErr),
    #[error("failed to resolve DID {did}: {message}")]
    ResolveDid { did: String, message: String },
    #[error("DID {did} has no AtprotoPersonalDataServer service entry")]
    NoPdsService { did: String },
}

/// Decides where to read `did`'s repo from.
pub async fn resolve(db: &DatabaseConnection, did: &str) -> Result<RepoEndpoint, EndpointError> {
    if let Some((endpoint, source)) = community_credentials::stored_pds_endpoint(db, did).await? {
        return Ok(if is_own_pds(&endpoint, &source) {
            RepoEndpoint::Trusted(endpoint)
        } else {
            RepoEndpoint::Untrusted(endpoint)
        });
    }

    let doc = crate::xrpc::com::atproto::identity::resolve_did(did)
        .await
        .map_err(|e| EndpointError::ResolveDid {
            did: did.to_string(),
            message: e.body.into_inner().message,
        })?;

    let endpoint = doc
        .pds_endpoint()
        .ok_or_else(|| EndpointError::NoPdsService {
            did: did.to_string(),
        })?;

    // An endpoint that *is* our configured `PDS_LOC` is trusted because we
    // configured it, not because a credentials row vouches for it. Without this,
    // a community whose row was lost becomes unreadable whenever the PDS is on
    // loopback or a private network.
    Ok(match matches_own_pds(endpoint) {
        Some(configured) => RepoEndpoint::Trusted(configured),
        None => RepoEndpoint::Untrusted(endpoint.to_string()),
    })
}

/// The **configured** `PDS_LOC` when `endpoint` names it, else `None`.
pub fn matches_own_pds(endpoint: &str) -> Option<String> {
    let configured = std::env::var("PDS_LOC").ok()?;
    (normalize_endpoint(&configured) == normalize_endpoint(endpoint)).then_some(configured)
}

/// As [`matches_own_pds`], additionally requiring that the credentials row was
/// one we minted ourselves rather than one a caller brought along.
pub fn own_pds_endpoint(endpoint: &str, source: &str) -> Option<String> {
    if source != SOURCE_APPVIEW_MANAGED {
        return None;
    }
    matches_own_pds(endpoint)
}

/// Whether a stored credentials row describes an account on the PDS this
/// AppView administers, rather than one a caller brought along.
fn is_own_pds(endpoint: &str, source: &str) -> bool {
    own_pds_endpoint(endpoint, source).is_some()
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_lowercase()
}

/// Whether `endpoint`'s host resolves *only* to addresses no other container
/// can reach, a PDS bound to loopback or a private LAN address, as in local
/// development.
pub async fn is_unreachable_from_containers(endpoint: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };

    // `host_str` keeps the brackets around an IPv6 literal, which `IpAddr`
    // won't parse.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = host.parse::<IpAddr>() {
        return embed_fetch::is_blocked_ip(&ip);
    }

    // Port is irrelevant to the classification but `lookup_host` wants one.
    let Ok(addrs) =
        rocket::tokio::net::lookup_host((host, url.port_or_known_default().unwrap_or(443))).await
    else {
        return false;
    };

    let mut resolved = addrs.peekable();
    if resolved.peek().is_none() {
        return false;
    }
    resolved.all(|addr| embed_fetch::is_blocked_ip(&addr.ip()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    use crate::lib::test_fixtures::PdsEnvGuard;
    use crate::models::community_credentials as community_credentials_model;

    #[test]
    fn as_str_trims_trailing_slash() {
        assert_eq!(
            RepoEndpoint::Trusted(String::from("http://localhost:3000/")).as_str(),
            "http://localhost:3000"
        );
        assert_eq!(
            RepoEndpoint::Untrusted(String::from("https://pds.example")).as_str(),
            "https://pds.example"
        );
    }

    #[test]
    fn only_managed_rows_on_the_configured_pds_are_trusted() {
        let _pds_env = PdsEnvGuard::without_admin_password("http://localhost:3000");

        assert!(is_own_pds("http://localhost:3000", SOURCE_APPVIEW_MANAGED));
        // Trailing slash / casing differences are the same endpoint.
        assert!(is_own_pds("http://LOCALHOST:3000/", SOURCE_APPVIEW_MANAGED));
        // A BYO row pointing at our own PDS is still caller-supplied.
        assert!(!is_own_pds("http://localhost:3000", "byo"));
        // A managed row for some other host (e.g. after PDS_LOC changed).
        assert!(!is_own_pds("http://other.example", SOURCE_APPVIEW_MANAGED));
    }

    /// The value handed back is always *our* configured endpoint, never the
    /// caller's spelling of it
    #[test]
    fn own_pds_endpoint_returns_the_configured_value() {
        let _pds_env = PdsEnvGuard::without_admin_password("http://localhost:3000");

        assert_eq!(
            own_pds_endpoint("http://LOCALHOST:3000/", SOURCE_APPVIEW_MANAGED).as_deref(),
            Some("http://localhost:3000")
        );
        assert_eq!(own_pds_endpoint("http://localhost:3000", "byo"), None);
        assert_eq!(
            own_pds_endpoint("http://other.example", SOURCE_APPVIEW_MANAGED),
            None
        );
    }

    /// Source-independent, because the orphan-adoption path has no row to read a
    /// `source` from.
    #[test]
    fn matches_own_pds_ignores_provenance() {
        let _pds_env = PdsEnvGuard::without_admin_password("http://localhost:3000");

        assert_eq!(
            matches_own_pds("http://localhost:3000/").as_deref(),
            Some("http://localhost:3000")
        );
        assert_eq!(matches_own_pds("https://pds.example"), None);
    }

    #[test]
    fn matches_own_pds_is_none_without_configuration() {
        let _pds_env = PdsEnvGuard::unset();
        assert_eq!(matches_own_pds("http://localhost:3000"), None);
    }

    fn credentials_row(pds_endpoint: &str, source: &str) -> community_credentials_model::Model {
        community_credentials_model::Model {
            community_did: String::from("did:plc:comm"),
            pds_endpoint: pds_endpoint.to_string(),
            identifier: String::from("c-abc.test"),
            password_ciphertext_b64: String::from("ciphertext"),
            password_nonce_b64: String::from("nonce"),
            source: source.to_string(),
            created_at: String::from("2026-07-25T00:00:00Z"),
        }
    }

    fn db_with_row(row: community_credentials_model::Model) -> sea_orm::DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row]])
            .into_connection()
    }

    /// The stored row short-circuits DID resolution entirely: this test passes
    /// with no network access, which is the property local development needs.
    #[tokio::test]
    async fn resolve_prefers_a_managed_row_on_our_own_pds() {
        let _pds_env = PdsEnvGuard::without_admin_password("http://localhost:3000");

        let db = db_with_row(credentials_row(
            "http://localhost:3000",
            SOURCE_APPVIEW_MANAGED,
        ));
        let endpoint = resolve(&db, "did:plc:comm").await.unwrap();

        assert_eq!(
            endpoint,
            RepoEndpoint::Trusted(String::from("http://localhost:3000"))
        );
    }

    /// A BYO endpoint is caller-supplied, so it stays behind the SSRF guard even
    /// though we hold credentials for it.
    #[tokio::test]
    async fn resolve_keeps_byo_rows_untrusted() {
        let _pds_env = PdsEnvGuard::without_admin_password("http://localhost:3000");

        let db = db_with_row(credentials_row("http://localhost:3000", "byo"));
        let endpoint = resolve(&db, "did:plc:comm").await.unwrap();

        assert_eq!(
            endpoint,
            RepoEndpoint::Untrusted(String::from("http://localhost:3000"))
        );
    }

    #[tokio::test]
    async fn classifies_ip_literals_without_dns() {
        for endpoint in [
            "http://127.0.0.1:3000",
            "http://[::1]:3000",
            "http://10.0.0.5",
            "http://192.168.1.10:3000",
            "http://169.254.1.1",
        ] {
            assert!(
                is_unreachable_from_containers(endpoint).await,
                "{endpoint} should be unreachable from another container"
            );
        }

        for endpoint in ["https://1.1.1.1", "https://8.8.8.8:8443"] {
            assert!(
                !is_unreachable_from_containers(endpoint).await,
                "{endpoint} should be considered reachable"
            );
        }
    }

    #[tokio::test]
    async fn unparseable_endpoints_are_treated_as_reachable() {
        assert!(!is_unreachable_from_containers("").await);
        assert!(!is_unreachable_from_containers("not a url").await);
    }
}
