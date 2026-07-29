//! Operator-only endpoints, authenticated with a shared secret rather than
//! atproto service auth.
//! The routes are mounted only when `APPVIEW_ADMIN_PASS` is set, so a deployment
//! that has not configured one exposes no surface at all.

use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::lib::credential_recovery;
use crate::lib::responses::ErrorResponse;

/// Environment variable holding the operator secret. Absent means these routes
/// are not mounted at all.
pub const ADMIN_PASS_ENV: &str = "APPVIEW_ADMIN_PASS";

/// Whether operator endpoints are configured, and therefore worth mounting.
pub fn is_configured() -> bool {
    admin_password().is_some()
}

fn admin_password() -> Option<String> {
    std::env::var(ADMIN_PASS_ENV)
        .ok()
        .filter(|pass| !pass.trim().is_empty())
}

/// Proof that the caller presented the operator secret via HTTP basic auth as
/// `admin:<APPVIEW_ADMIN_PASS>`
pub struct AdminAuth;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AdminAuth {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let Some(expected) = admin_password() else {
            return Outcome::Error((Status::NotFound, ()));
        };
        let Some(presented) = req
            .headers()
            .get_one("Authorization")
            .and_then(parse_basic_admin)
        else {
            return Outcome::Error((Status::Unauthorized, ()));
        };

        // Constant-time, so a wrong secret leaks nothing about how much of it was
        // right. `ct_eq` is only defined for equal-length inputs, hence the length
        // check first — which does leak the length, and that is fine.
        let matches = presented.len() == expected.len()
            && bool::from(presented.as_bytes().ct_eq(expected.as_bytes()));

        if matches {
            Outcome::Success(AdminAuth)
        } else {
            Outcome::Error((Status::Unauthorized, ()))
        }
    }
}

/// The password out of an `Authorization: Basic <base64(admin:password)>` header,
/// or `None` if the header is malformed or names another user.
fn parse_basic_admin(header: &str) -> Option<String> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

    let encoded = header.strip_prefix("Basic ")?;
    let decoded = BASE64.decode(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;

    let (user, password) = decoded.split_once(':')?;
    (user == "admin").then(|| password.to_string())
}

/// What a recovery attempt did, in enough detail to act on.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RecoverCredentialsResponse {
    pub did: String,
    /// Whether the community is hosted on the PDS this AppView administers. When
    /// false, nothing was attempted and nothing could be — this alone is the
    /// useful diagnostic.
    pub hosted_on_managed_pds: bool,
    /// Whether a live session was established.
    pub recovered: bool,
    pub detail: String,
}

/// Forces credential recovery for `did`, whether or not a credentials row still
/// exists for it
#[post("/admin/recover-credentials?<did>")]
pub async fn recover_credentials(
    did: &str,
    _auth: AdminAuth,
    db: &State<DatabaseConnection>,
) -> Result<Json<RecoverCredentialsResponse>, ErrorResponse> {
    let db = db.inner();

    let hosted = credential_recovery::hosted_on_managed_pds(db, did)
        .await?
        .is_some();

    if !hosted {
        return Ok(Json(RecoverCredentialsResponse {
            did: did.to_string(),
            hosted_on_managed_pds: false,
            recovered: false,
            detail: String::from(
                "This community is not hosted on the PDS this AppView administers, so its \
                 password cannot be reset from here. Re-register its credentials instead.",
            ),
        }));
    }

    // Works whether or not a row survives
    match credential_recovery::force_recovery(db, did).await {
        Ok(Some(_)) => Ok(Json(RecoverCredentialsResponse {
            did: did.to_string(),
            hosted_on_managed_pds: true,
            recovered: true,
            detail: String::from("Minted a new password and verified it by logging in."),
        })),
        Ok(None) => Ok(Json(RecoverCredentialsResponse {
            did: did.to_string(),
            hosted_on_managed_pds: true,
            recovered: false,
            detail: String::from(
                "Recovery was skipped, most likely because it was attempted moments ago. \
                 Try again shortly.",
            ),
        })),
        Err(e) => Err(ErrorResponse::from(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

    fn basic(user: &str, password: &str) -> String {
        format!("Basic {}", BASE64.encode(format!("{user}:{password}")))
    }

    #[test]
    fn reads_the_password_out_of_a_basic_auth_header() {
        assert_eq!(
            parse_basic_admin(&basic("admin", "s3cret")).as_deref(),
            Some("s3cret")
        );
        // A password containing a colon survives: only the first one splits.
        assert_eq!(
            parse_basic_admin(&basic("admin", "a:b:c")).as_deref(),
            Some("a:b:c")
        );
    }

    #[test]
    fn rejects_headers_that_are_not_admin_basic_auth() {
        assert!(parse_basic_admin(&basic("root", "s3cret")).is_none());
        assert!(parse_basic_admin("Bearer some-token").is_none());
        assert!(parse_basic_admin("Basic not-base64!!").is_none());
        // Valid base64 with no colon is not a credential pair.
        assert!(parse_basic_admin(&format!("Basic {}", BASE64.encode("nocolon"))).is_none());
    }
}
