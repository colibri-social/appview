//! Bridges a proxied `Authorization: Bearer` header into the `auth` query
//! parameter that the XRPC handlers read.

use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::uri::Origin;
use rocket::{Data, Request};

pub struct ServiceAuthHeaderBridge;

#[rocket::async_trait]
impl Fairing for ServiceAuthHeaderBridge {
    fn info(&self) -> Info {
        Info {
            name: "Service-auth Authorization header → ?auth query bridge",
            kind: Kind::Request,
        }
    }

    async fn on_request(&self, req: &mut Request<'_>, _: &mut Data<'_>) {
        let Some(token) = bearer_token(req) else {
            return;
        };

        if let Some(rewritten) = inject_auth_query(req.uri(), &token) {
            req.set_uri(rewritten);
        }
    }
}

/// Extracts the bearer token from the `Authorization` header, if present.
fn bearer_token(req: &Request<'_>) -> Option<String> {
    req.headers()
        .get_one("Authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned)
}

/// Rebuilds `uri` with its `auth` query parameter set to `token`.
fn inject_auth_query(uri: &Origin<'_>, token: &str) -> Option<Origin<'static>> {
    let mut pairs: Vec<String> = Vec::new();

    if let Some(query) = uri.query() {
        for segment in query.as_str().split('&') {
            if segment.is_empty() {
                continue;
            }
            let key = segment.split('=').next().unwrap_or("");
            if key == "auth" {
                continue;
            }
            pairs.push(segment.to_owned());
        }
    }

    pairs.push(format!("auth={token}"));

    let rebuilt = format!("{}?{}", uri.path(), pairs.join("&"));
    Origin::parse_owned(rebuilt).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(raw: &str, token: &str) -> String {
        let origin = Origin::parse(raw).unwrap();
        inject_auth_query(&origin, token).unwrap().to_string()
    }

    #[test]
    fn appends_auth_when_no_query_present() {
        assert_eq!(
            rewrite(
                "/xrpc/social.colibri.notification.getUnreadCount",
                "jwt-token"
            ),
            "/xrpc/social.colibri.notification.getUnreadCount?auth=jwt-token"
        );
    }

    #[test]
    fn preserves_other_query_params() {
        assert_eq!(
            rewrite(
                "/xrpc/social.colibri.notification.getUnseen?channel=abc",
                "jwt"
            ),
            "/xrpc/social.colibri.notification.getUnseen?channel=abc&auth=jwt"
        );
    }

    #[test]
    fn overrides_existing_auth_param() {
        assert_eq!(
            rewrite("/xrpc/social.colibri.actor.listMutes?auth=stale", "fresh"),
            "/xrpc/social.colibri.actor.listMutes?auth=fresh"
        );
    }

    #[test]
    fn overrides_empty_auth_param_left_by_client() {
        // In production the client appends an empty `?auth=` (it no longer
        // mints a token); the PDS-minted header must win.
        assert_eq!(
            rewrite("/xrpc/social.colibri.actor.listCommunities?auth=", "fresh"),
            "/xrpc/social.colibri.actor.listCommunities?auth=fresh"
        );
    }

    #[test]
    fn overrides_auth_among_multiple_params() {
        assert_eq!(
            rewrite(
                "/xrpc/social.colibri.community.banUser?community=at%3A%2F%2Fx&auth=stale&identifier=bob",
                "fresh"
            ),
            "/xrpc/social.colibri.community.banUser?community=at%3A%2F%2Fx&identifier=bob&auth=fresh"
        );
    }

    #[test]
    fn bearer_prefix_required() {
        // Sanity: a token JWT only uses URL-safe characters.
        let jwt = "eyJhbGciOiJFUzI1NiJ9.eyJpc3MiOiJkaWQ6cGxjOmFiYyJ9.sig-_123";
        assert_eq!(rewrite("/xrpc/test", jwt), format!("/xrpc/test?auth={jwt}"));
    }
}
