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
    /// `"web"` or `"fcm"`. Defaults to `"web"` when omitted, for older
    /// clients that predate multi-provider support.
    pub provider: Option<String>,
    pub endpoint: String,
}

#[derive(Serialize, Debug)]
pub struct UnregisterPushResponse {
    pub unregistered: bool,
}

type UnregisterFn = dyn Fn(DatabaseConnection, String, String, String) -> BoxFuture<'static, Result<(), DbErr>>
    + Send
    + Sync;

async fn unregister_push_with(
    auth: String,
    input: UnregisterPushInput,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    unregister_fn: &UnregisterFn,
) -> Result<Json<UnregisterPushResponse>, ErrorResponse> {
    let provider = input.provider.unwrap_or_else(|| String::from("web"));
    let endpoint = input.endpoint;
    with_authenticated(
        auth,
        "social.colibri.notification.unregisterPush",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            unregister_fn(db, caller_did, provider, endpoint).await?;
            Ok(Json(UnregisterPushResponse { unregistered: true }))
        },
    )
    .await
}

fn unregister_boxed(
    db: DatabaseConnection,
    caller_did: String,
    provider: String,
    endpoint: String,
) -> BoxFuture<'static, Result<(), DbErr>> {
    Box::pin(async move {
        push_subscriptions::delete_by_endpoint_for_actor(&db, &caller_did, &provider, &endpoint)
            .await
    })
}

#[post(
    "/xrpc/social.colibri.notification.unregisterPush?<auth>",
    data = "<input>"
)]
/// Drops a previously registered push subscription for the authenticated
/// user, identified by its provider + endpoint.
pub async fn unregister_push(
    auth: &str,
    input: Json<UnregisterPushInput>,
    db: &State<DatabaseConnection>,
) -> Result<Json<UnregisterPushResponse>, ErrorResponse> {
    unregister_push_with(
        auth.to_string(),
        input.into_inner(),
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

    fn sample_input() -> UnregisterPushInput {
        UnregisterPushInput {
            provider: None,
            endpoint: String::from("https://push.example/abc"),
        }
    }

    #[tokio::test]
    async fn unregisters_for_authenticated_caller() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let unregister = move |_: DatabaseConnection,
                               caller_did: String,
                               provider: String,
                               endpoint: String|
              -> BoxFuture<'static, Result<(), DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((caller_did, provider, endpoint));
                Ok(())
            })
        };

        let result = unregister_push_with(
            String::from("token"),
            sample_input(),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &unregister,
        )
        .await
        .unwrap();

        assert!(result.unregistered);
        let (caller_did, provider, endpoint) = captured.lock().unwrap().take().unwrap();
        assert_eq!(caller_did, "did:plc:me");
        assert_eq!(provider, "web");
        assert_eq!(endpoint, "https://push.example/abc");
    }

    #[tokio::test]
    async fn defaults_provider_to_web_when_omitted() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let unregister = move |_: DatabaseConnection,
                               _: String,
                               provider: String,
                               _: String|
              -> BoxFuture<'static, Result<(), DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some(provider);
                Ok(())
            })
        };

        unregister_push_with(
            String::from("token"),
            sample_input(),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &unregister,
        )
        .await
        .unwrap();

        assert_eq!(captured.lock().unwrap().take().unwrap(), "web");
    }

    #[tokio::test]
    async fn respects_explicit_fcm_provider() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let unregister = move |_: DatabaseConnection,
                               _: String,
                               provider: String,
                               _: String|
              -> BoxFuture<'static, Result<(), DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some(provider);
                Ok(())
            })
        };

        let input = UnregisterPushInput {
            provider: Some(String::from("fcm")),
            endpoint: String::from("fcm-registration-token"),
        };

        unregister_push_with(
            String::from("token"),
            input,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &unregister,
        )
        .await
        .unwrap();

        assert_eq!(captured.lock().unwrap().take().unwrap(), "fcm");
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = unregister_push_with(
            String::from("token"),
            sample_input(),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _, _| Box::pin(async { panic!("should not unregister when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
