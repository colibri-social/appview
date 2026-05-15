use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::notifications;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::lib::time::current_iso8601_utc;

#[derive(Serialize, Debug)]
pub struct UpdateSeenResponse {
    pub updated: u64,
}

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type MarkSeenFn = dyn Fn(DatabaseConnection, String, String, String) -> BoxFuture<'static, Result<u64, DbErr>>
    + Send
    + Sync;

async fn update_seen_with(
    auth: String,
    seen_at_input: Option<String>,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    mark_seen_fn: &MarkSeenFn,
) -> Result<Json<UpdateSeenResponse>, ErrorResponse> {
    let did = verify_auth_fn(auth, String::from("social.colibri.notification.updateSeen"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    // The cutoff defaults to "now" — anything indexed at or before this point
    // counts as "seen". Lets clients catch up without a timestamp round-trip.
    let cutoff = seen_at_input.clone().unwrap_or_else(current_iso8601_utc);
    let seen_at = seen_at_input.unwrap_or_else(current_iso8601_utc);

    let updated = mark_seen_fn(db, did, seen_at, cutoff).await?;
    Ok(Json(UpdateSeenResponse { updated }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn mark_seen_boxed(
    db: DatabaseConnection,
    did: String,
    seen_at: String,
    cutoff: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move { notifications::mark_seen_up_to(&db, &did, &seen_at, &cutoff).await })
}

#[post("/xrpc/social.colibri.notification.updateSeen?<seen_at>&<auth>")]
/// Marks every unseen notification for the authenticated user with
/// `indexed_at <= seen_at` as seen. Defaults to `now` when `seen_at` is omitted.
pub async fn update_seen(
    seen_at: Option<&str>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<UpdateSeenResponse>, ErrorResponse> {
    update_seen_with(
        auth.to_string(),
        seen_at.map(|s| s.to_string()),
        db.inner().clone(),
        &verify_auth_boxed,
        &mark_seen_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn marks_seen_with_default_cutoff() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let captured: Arc<Mutex<Option<(String, String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let mark_seen = move |_: DatabaseConnection,
                              did: String,
                              seen: String,
                              cutoff: String|
              -> BoxFuture<'static, Result<u64, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((did, seen, cutoff));
                Ok(3)
            })
        };
        let result = update_seen_with(
            String::from("token"),
            None,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &mark_seen,
        )
        .await
        .unwrap();

        assert_eq!(result.updated, 3);
        let (did, seen, cutoff) = captured.lock().unwrap().take().unwrap();
        assert_eq!(did, "did:plc:me");
        // When no seen_at is provided, both values should be the same "now" timestamp.
        assert_eq!(seen, cutoff);
        assert!(seen.ends_with('Z'));
    }

    #[tokio::test]
    async fn uses_provided_seen_at_when_given() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let mark_seen = move |_: DatabaseConnection,
                              _did: String,
                              seen: String,
                              cutoff: String|
              -> BoxFuture<'static, Result<u64, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((seen, cutoff));
                Ok(1)
            })
        };
        let _ = update_seen_with(
            String::from("token"),
            Some(String::from("2026-05-14T05:00:00.000Z")),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &mark_seen,
        )
        .await
        .unwrap();

        let (seen, cutoff) = captured.lock().unwrap().take().unwrap();
        assert_eq!(seen, "2026-05-14T05:00:00.000Z");
        assert_eq!(cutoff, "2026-05-14T05:00:00.000Z");
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = update_seen_with(
            String::from("token"),
            None,
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _, _| Box::pin(async { panic!("should not mark when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
