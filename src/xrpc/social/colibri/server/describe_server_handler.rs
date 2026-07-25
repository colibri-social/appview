use rocket::{get, serde::json::Json};
use serde::Serialize;

use crate::lib::pds_status::{self, PdsStatusReport};

/// Static self-description of this AppView, returned by
/// [`describe_server`]. Lets a client confirm that a user-entered URL actually
/// points at a Colibri AppView (and learn which version) before persisting it
/// as their preferred AppView.
#[derive(Serialize, Debug)]
pub struct DescribeServerResponse {
    /// Stable software identifier — always `"colibri-appview"`. This is the
    /// field clients key on to decide whether a host is a Colibri AppView.
    pub software: String,
    /// Used to identify what kind of type the AppView is. "vanilla" for the stock AppView.
    /// Can be any arbitrary string.
    pub flavor: String,
    /// Version of the running AppView: the release tag when built in CI
    /// (`APPVIEW_VERSION`), otherwise the crate version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Whether this AppView's own PDS answered the boot-time probe. **Debug
    /// builds only**
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pds: Option<PdsStatusReport>,
}

#[get("/xrpc/social.colibri.server.describeServer")]
/// Identifies this server as a Colibri AppView.
///
/// Public, unauthenticated, and side-effect free — intended as a lightweight
/// identity/health probe a client can hit against an arbitrary host to verify
/// it speaks the Colibri protocol before pointing itself at it.
pub fn describe_server() -> Json<DescribeServerResponse> {
    Json(DescribeServerResponse {
        software: String::from("colibri-appview"),
        flavor: String::from("vanilla"),
        version: String::from(
            option_env!("APPVIEW_VERSION")
                .filter(|v| !v.is_empty())
                .unwrap_or(env!("CARGO_PKG_VERSION")),
        ),
        pds: pds_status::report(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_as_colibri_appview() {
        let res = describe_server();
        assert_eq!(res.software, "colibri-appview");
        assert!(!res.version.is_empty());
    }

    /// The public shape must not change: `pds` is a debug-build diagnostic and
    /// has to be absent from the serialized payload in release.
    #[test]
    fn pds_diagnostics_are_debug_only() {
        let res = describe_server();
        let json = serde_json::to_value(&*res).expect("response serializes");

        assert!(json.get("software").is_some());
        assert!(json.get("flavor").is_some());
        assert!(json.get("version").is_some());

        if cfg!(debug_assertions) {
            assert!(json.get("pds").is_some(), "debug builds report PDS status");
        } else {
            assert!(
                json.get("pds").is_none(),
                "release builds must not leak deployment state"
            );
        }
    }
}
