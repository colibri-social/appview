use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::{Deserialize, Serialize};

use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::push_subscriptions;
use crate::lib::responses::ErrorResponse;

#[derive(Deserialize, Clone, Debug)]
pub struct UnregisterPushInput {
    pub endpoint: String,
}

#[derive(Serialize, Debug)]
pub struct UnregisterPushResponse {
    pub unregistered: bool,
}

type UnregisterFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<(), DbErr>> + Send + Sync;

async fn unregister_push_with(
    auth: String,
    endpoint: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    unregister_fn: &UnregisterFn,
) -> Result<Json<UnregisterPushResponse>, ErrorResponse> {
    with_authenticated(
        auth,
        "social.colibri.notification.unregisterPush",
        db,
        verify_auth_fn,
        |_caller_did, db| async move {
            unregister_fn(db, endpoint).await?;
            Ok(Json(UnregisterPushResponse { unregistered: true }))
        },
    )
    .await
}

fn unregister_boxed(
    db: DatabaseConnection,
    endpoint: String,
) -> BoxFuture<'static, Result<(), DbErr>> {
    Box::pin(async move { push_subscriptions::delete_by_endpoint(&db, &endpoint).await })
}

#[post(
    "/xrpc/social.colibri.notification.unregisterPush?<auth>",
    data = "<input>"
)]
/// Drops a previously registered Web Push subscription for the authenticated
/// user, identified by its endpoint.
pub async fn unregister_push(
    auth: &str,
    input: Json<UnregisterPushInput>,
    db: &State<DatabaseConnection>,
) -> Result<Json<UnregisterPushResponse>, ErrorResponse> {
    unregister_push_with(
        auth.to_string(),
        input.into_inner().endpoint,
        db.inner().clone(),
        &verify_auth_boxed,
        &unregister_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn unregisters_for_authenticated_caller() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let unregister = move |_: DatabaseConnection,
                               endpoint: String|
              -> BoxFuture<'static, Result<(), DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some(endpoint);
                Ok(())
            })
        };

        let result = unregister_push_with(
            String::from("token"),
            String::from("https://push.example/abc"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &unregister,
        )
        .await
        .unwrap();

        assert!(result.unregistered);
        assert_eq!(
            captured.lock().unwrap().take().unwrap(),
            "https://push.example/abc"
        );
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = unregister_push_with(
            String::from("token"),
            String::from("https://push.example/abc"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _| Box::pin(async { panic!("should not unregister when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
