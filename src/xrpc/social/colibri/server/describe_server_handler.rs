use rocket::{get, serde::json::Json};
use serde::Serialize;

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
}
