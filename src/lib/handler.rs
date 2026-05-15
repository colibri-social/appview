//! Shared scaffolding for authenticated XRPC handlers.
//!
//! Every authenticated community endpoint repeats the same prelude — parse
//! the community AT-URI, verify the service-auth JWT, load the caller's
//! authz state for the community, optionally check a permission — and the
//! same error shapes for each of those failure modes. This module holds:
//!
//! - `CallerContext`: what an authenticated handler typically needs.
//! - `with_community_authz`: the combinator that runs the prelude and hands
//!   `CallerContext` + `DatabaseConnection` to a body closure.
//! - `with_authenticated`: the simpler variant for handlers that need
//!   service auth but no community context (notification endpoints, etc.).
//! - Reusable dependency types (`VerifyAuthFn`, `LoadAuthzFn`) and the
//!   production-side boxed implementations (`verify_auth_boxed`,
//!   `load_authz_boxed`), so each handler stops redefining its own copy.
//! - Error-response constructors (`auth_error`, `invalid_community_uri`,
//!   `forbidden`) used both by the combinator and by handlers that need to
//!   emit the same shapes from their own bodies.

use std::future::Future;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use sea_orm::{DatabaseConnection, DbErr};

use crate::lib::at_uri::AtUri;
use crate::lib::community_authz::{self, ActorAuthz};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};

// ---- Reusable dependency types ------------------------------------------

pub type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
pub type LoadAuthzFn = dyn Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<ActorAuthz, DbErr>>
    + Send
    + Sync;

pub fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

pub fn load_authz_boxed(
    db: DatabaseConnection,
    community_uri: String,
    did: String,
) -> BoxFuture<'static, Result<ActorAuthz, DbErr>> {
    Box::pin(async move { community_authz::load_actor_authz(&db, &community_uri, &did).await })
}

// ---- Error-response constructors ----------------------------------------

pub fn auth_error(err: ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

pub fn invalid_community_uri() -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    }
}

pub fn forbidden(permission: Permission) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("Forbidden"),
            message: format!("Missing permission: {permission}"),
        }),
    }
}

// ---- Caller context -----------------------------------------------------

/// Aggregated state every authenticated community handler needs once the
/// prelude has run.
#[derive(Debug)]
pub struct CallerContext {
    pub caller_did: String,
    pub community: AtUri,
    /// The original AT-URI string as supplied to the combinator. Retained so
    /// handlers that need to re-issue authz lookups for a different DID can
    /// reuse the same `LoadAuthzFn` without rebuilding the URI from the
    /// parsed parts.
    pub community_uri: String,
    pub authz: ActorAuthz,
}

// ---- Combinators --------------------------------------------------------

/// Authenticated community handler combinator.
///
/// Runs the prelude every community-scoped endpoint shares:
///
/// 1. Parse `community_uri` to an [`AtUri`] (→ `InvalidRequest` on failure).
/// 2. Verify `auth` for `lxm` to a caller DID (→ `AuthError`).
/// 3. Load the caller's [`ActorAuthz`] for the community.
/// 4. If `permission` is `Some`, refuse with `Forbidden` unless the caller
///    holds it (no channel scope — pass channel-scoped checks through the
///    body if needed).
///
/// On success, invokes `body` with [`CallerContext`] and the same
/// `DatabaseConnection` so the body can issue further queries.
///
/// The argument list is deliberately wide so handlers don't have to thread
/// these concerns separately; eight args is intentional.
#[allow(clippy::too_many_arguments)]
pub async fn with_community_authz<F, Fut, T>(
    auth: String,
    lxm: &'static str,
    community_uri: String,
    permission: Option<Permission>,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    body: F,
) -> Result<T, ErrorResponse>
where
    F: FnOnce(CallerContext, DatabaseConnection) -> Fut,
    Fut: Future<Output = Result<T, ErrorResponse>>,
{
    let community = AtUri::parse(&community_uri).ok_or_else(invalid_community_uri)?;
    let caller_did = verify_auth_fn(auth, lxm.to_string())
        .await
        .map_err(auth_error)?;
    let authz = load_authz_fn(db.clone(), community_uri.clone(), caller_did.clone()).await?;

    if let Some(perm) = permission
        && !authz.has(perm, None)
    {
        return Err(forbidden(perm));
    }

    body(
        CallerContext {
            caller_did,
            community,
            community_uri,
            authz,
        },
        db,
    )
    .await
}

/// Authenticated-only handler combinator (no community context).
///
/// Verifies the service-auth JWT and hands `caller_did` + `db` to the body.
/// Use this for endpoints that only need to know who's calling — notification
/// endpoints, anything keyed by the caller's own DID.
pub async fn with_authenticated<F, Fut, T>(
    auth: String,
    lxm: &'static str,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    body: F,
) -> Result<T, ErrorResponse>
where
    F: FnOnce(String, DatabaseConnection) -> Fut,
    Fut: Future<Output = Result<T, ErrorResponse>>,
{
    let caller_did = verify_auth_fn(auth, lxm.to_string())
        .await
        .map_err(auth_error)?;
    body(caller_did, db).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::permissions::Permission;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn mock_db() -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres).into_connection()
    }

    fn empty_authz(is_owner: bool) -> ActorAuthz {
        ActorAuthz {
            is_owner,
            member: None,
            roles: vec![],
        }
    }

    #[tokio::test]
    async fn with_community_authz_runs_body_when_owner_and_no_permission_required() {
        let db = mock_db();
        let result: Result<&'static str, ErrorResponse> = with_community_authz(
            String::from("token"),
            "social.colibri.test",
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            None,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(empty_authz(true)) }),
            |ctx, _db| async move {
                assert_eq!(ctx.caller_did, "did:plc:owner");
                assert_eq!(ctx.community.authority, "did:plc:owner");
                assert!(ctx.authz.is_owner);
                Ok("ran")
            },
        )
        .await;
        assert_eq!(result.unwrap(), "ran");
    }

    #[tokio::test]
    async fn with_community_authz_rejects_invalid_uri_without_calling_anything() {
        let db = mock_db();
        let result: Result<(), ErrorResponse> = with_community_authz(
            String::from("token"),
            "social.colibri.test",
            String::from("not-a-uri"),
            None,
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate") }),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            |_, _| async { panic!("should not run body") },
        )
        .await;
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn with_community_authz_rejects_when_permission_missing() {
        let db = mock_db();
        let result: Result<(), ErrorResponse> = with_community_authz(
            String::from("token"),
            "social.colibri.test",
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            Some(Permission::MemberBan),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| Box::pin(async { Ok(empty_authz(false)) }),
            |_, _| async { panic!("should not run body when permission missing") },
        )
        .await;
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn with_community_authz_surfaces_auth_error() {
        let db = mock_db();
        let result: Result<(), ErrorResponse> = with_community_authz(
            String::from("token"),
            "social.colibri.test",
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            None,
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            |_, _| async { panic!("should not run body") },
        )
        .await;
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }

    #[tokio::test]
    async fn with_authenticated_passes_caller_did_to_body() {
        let db = mock_db();
        let result: Result<String, ErrorResponse> = with_authenticated(
            String::from("token"),
            "social.colibri.test",
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            |caller, _| async move { Ok(caller) },
        )
        .await;
        assert_eq!(result.unwrap(), "did:plc:me");
    }
}
