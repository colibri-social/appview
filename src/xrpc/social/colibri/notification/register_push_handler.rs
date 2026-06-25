use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::{Deserialize, Serialize};

use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::push_subscriptions;
use crate::lib::responses::ErrorResponse;

/// Web Push subscription keys, as produced by the browser `PushSubscription`.
#[derive(Deserialize, Clone, Debug)]
pub struct PushKeys {
    pub p256dh: String,
    pub auth: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct RegisterPushInput {
    /// `"web"` or `"tauri"`. Recorded for future use; only `web` is delivered.
    pub platform: String,
    pub endpoint: String,
    pub keys: PushKeys,
}

#[derive(Serialize, Debug)]
pub struct RegisterPushResponse {
    pub registered: bool,
}

type RegisterFn = dyn Fn(DatabaseConnection, String, RegisterPushInput) -> BoxFuture<'static, Result<(), DbErr>>
    + Send
    + Sync;

async fn register_push_with(
    auth: String,
    input: RegisterPushInput,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    register_fn: &RegisterFn,
) -> Result<Json<RegisterPushResponse>, ErrorResponse> {
    with_authenticated(
        auth,
        "social.colibri.notification.registerPush",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            register_fn(db, caller_did, input).await?;
            Ok(Json(RegisterPushResponse { registered: true }))
        },
    )
    .await
}

fn register_boxed(
    db: DatabaseConnection,
    caller_did: String,
    input: RegisterPushInput,
) -> BoxFuture<'static, Result<(), DbErr>> {
    Box::pin(async move {
        push_subscriptions::upsert(
            &db,
            &caller_did,
            &input.endpoint,
            &input.keys.p256dh,
            &input.keys.auth,
            &input.platform,
        )
        .await
    })
}

#[post(
    "/xrpc/social.colibri.notification.registerPush?<auth>",
    data = "<input>"
)]
/// Stores a Web Push subscription for the authenticated user, keyed (deduped)
/// by its endpoint.
pub async fn register_push(
    auth: &str,
    input: Json<RegisterPushInput>,
    db: &State<DatabaseConnection>,
) -> Result<Json<RegisterPushResponse>, ErrorResponse> {
    register_push_with(
        auth.to_string(),
        input.into_inner(),
        db.inner().clone(),
        &verify_auth_boxed,
        &register_boxed,
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

    fn sample_input() -> RegisterPushInput {
        RegisterPushInput {
            platform: String::from("web"),
            endpoint: String::from("https://push.example/abc"),
            keys: PushKeys {
                p256dh: String::from("p256dh-key"),
                auth: String::from("auth-key"),
            },
        }
    }

    #[tokio::test]
    async fn registers_for_authenticated_caller() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let register = move |_: DatabaseConnection,
                             did: String,
                             input: RegisterPushInput|
              -> BoxFuture<'static, Result<(), DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() =
                    Some((did, input.endpoint, input.keys.p256dh));
                Ok(())
            })
        };

        let result = register_push_with(
            String::from("token"),
            sample_input(),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &register,
        )
        .await
        .unwrap();

        assert!(result.registered);
        let (did, endpoint, p256dh) = captured.lock().unwrap().take().unwrap();
        assert_eq!(did, "did:plc:me");
        assert_eq!(endpoint, "https://push.example/abc");
        assert_eq!(p256dh, "p256dh-key");
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = register_push_with(
            String::from("token"),
            sample_input(),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _| Box::pin(async { panic!("should not register when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
