//! Boot-time reachability check for the AppView's own PDS (`PDS_LOC`).

use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;

use crate::lib::http::HTTP;

/// Outcome of the boot-time probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PdsStatus {
    /// The endpoint answered `com.atproto.server.describeServer` like a PDS.
    Reachable,
    /// Something answered, but not as a PDS.
    NotAPds,
    /// Nothing answered: connection refused, DNS failure, TLS error, timeout.
    Unreachable,
    /// `PDS_LOC` is unset, empty, or not a URL.
    Unconfigured,
}

impl PdsStatus {
    pub fn is_reachable(self) -> bool {
        matches!(self, PdsStatus::Reachable)
    }
}

/// Shape reported by `describeServer` in debug builds. Carries no endpoint,
/// credentials, or error text.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct PdsStatusReport {
    pub configured: bool,
    pub reachable: bool,
    pub status: PdsStatus,
}

static STATUS: OnceLock<PdsStatus> = OnceLock::new();

/// Boot blocks on this, so it stays under the shared client's connect timeout.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The probe result, or [`PdsStatus::Unconfigured`] if [`probe`] never ran.
pub fn status() -> PdsStatus {
    STATUS.get().copied().unwrap_or(PdsStatus::Unconfigured)
}

/// The `describeServer` payload, or `None` in release builds — that endpoint is
/// public and unauthenticated, so deployment state stays out of it.
pub fn report() -> Option<PdsStatusReport> {
    if !cfg!(debug_assertions) {
        return None;
    }

    let status = status();
    Some(PdsStatusReport {
        configured: status != PdsStatus::Unconfigured,
        reachable: status.is_reachable(),
        status,
    })
}

/// Probes `PDS_LOC` once and records the outcome for [`status`]. Logs a line
/// either way; a bad PDS is a warning rather than a hard failure because reads
/// keep working without one.
pub async fn probe() {
    let configured = std::env::var("PDS_LOC").unwrap_or_default();
    let endpoint = configured.trim().trim_end_matches('/');

    let (status, host) = match reqwest::Url::parse(endpoint) {
        Ok(url) => {
            let host = url.host_str().unwrap_or(endpoint).to_string();
            (probe_endpoint(endpoint).await, host)
        }
        Err(_) => (PdsStatus::Unconfigured, endpoint.to_string()),
    };

    let _ = STATUS.set(status);

    match status {
        PdsStatus::Reachable => log::info!("PDS reachable at {host}."),
        PdsStatus::NotAPds => log::warn!(
            "PDS at {host} did not answer com.atproto.server.describeServer like a PDS. \
             Community create/delete/migrate and every community write will fail. \
             Point PDS_LOC at a real PDS — docker-compose.pds.yml runs one locally."
        ),
        PdsStatus::Unreachable => log::warn!(
            "PDS at {host} is unreachable. Community create/delete/migrate and every \
             community write will fail. Check PDS_LOC and that the PDS is running — \
             docker-compose.pds.yml runs one locally."
        ),
        PdsStatus::Unconfigured => log::warn!(
            "PDS_LOC is not a usable URL ({host:?}). Community create/delete/migrate and \
             every community write will fail."
        ),
    }
}

async fn probe_endpoint(endpoint: &str) -> PdsStatus {
    let url = format!("{endpoint}/xrpc/com.atproto.server.describeServer");

    let response = match HTTP.clone().get(url).timeout(PROBE_TIMEOUT).send().await {
        Ok(response) => response,
        Err(_) => return PdsStatus::Unreachable,
    };

    if !response.status().is_success() {
        return PdsStatus::NotAPds;
    }

    // Every PDS answers this unauthenticated and with a JSON object.
    match response.json::<serde_json::Value>().await {
        Ok(body) if body.is_object() => PdsStatus::Reachable,
        _ => PdsStatus::NotAPds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn a_pds_answering_describe_server_is_reachable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.describeServer"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "availableUserDomains": [".test"]
            })))
            .mount(&server)
            .await;

        assert_eq!(probe_endpoint(&server.uri()).await, PdsStatus::Reachable);
    }

    #[tokio::test]
    async fn a_web_server_that_is_not_a_pds_is_detected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.describeServer"))
            .respond_with(ResponseTemplate::new(404).set_body_string("<html>Not found</html>"))
            .mount(&server)
            .await;

        assert_eq!(probe_endpoint(&server.uri()).await, PdsStatus::NotAPds);
    }

    #[tokio::test]
    async fn a_200_that_is_not_json_is_not_a_pds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.describeServer"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>Landing page</html>"))
            .mount(&server)
            .await;

        assert_eq!(probe_endpoint(&server.uri()).await, PdsStatus::NotAPds);
    }

    /// The client mirrors these strings as a union type in
    /// `packages/client/src/utils/appview.ts`; keep the two in sync.
    #[test]
    fn status_serializes_to_the_wire_names_the_client_expects() {
        let names: Vec<String> = [
            PdsStatus::Reachable,
            PdsStatus::NotAPds,
            PdsStatus::Unreachable,
            PdsStatus::Unconfigured,
        ]
        .iter()
        .map(|s| serde_json::to_string(s).expect("status serializes"))
        .collect();

        assert_eq!(
            names,
            vec![
                "\"reachable\"",
                "\"notAPds\"",
                "\"unreachable\"",
                "\"unconfigured\""
            ]
        );
    }

    #[tokio::test]
    async fn a_dead_endpoint_is_unreachable() {
        // Port 9 (discard) refuses connections.
        assert_eq!(
            probe_endpoint("http://127.0.0.1:9").await,
            PdsStatus::Unreachable
        );
    }
}
