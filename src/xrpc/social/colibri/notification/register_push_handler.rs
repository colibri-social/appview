use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::{Deserialize, Serialize};

use crate::lib::embed_fetch;
use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::push_subscriptions;
use crate::lib::responses::{ErrorBody, ErrorResponse};

/// Web Push subscription keys, as produced by the browser `PushSubscription`.
#[derive(Deserialize, Clone, Debug)]
pub struct PushKeys {
    pub p256dh: String,
    pub auth: String,
}

/// The provider-specific half of a registration — a discriminated union so
/// each shape can carry only what its provider actually needs (an opaque FCM
/// token isn't a URL, and a Web Push endpoint isn't a bearer token).
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "$type")]
pub enum PushSubscriptionInput {
    #[serde(rename = "social.colibri.notification.registerPush#webPushSubscription")]
    Web { endpoint: String, keys: PushKeys },
    #[serde(rename = "social.colibri.notification.registerPush#fcmSubscription")]
    Fcm { token: String },
}

#[derive(Deserialize, Clone, Debug)]
pub struct RegisterPushInput {
    /// `"web"`, `"tauri"`, or `"android"`. Recorded for future use; delivery
    /// branches on `subscription`'s provider, not on this field.
    pub platform: String,
    pub subscription: PushSubscriptionInput,
}

#[derive(Serialize, Debug)]
pub struct RegisterPushResponse {
    pub registered: bool,
}

/// Generous upper bound on an FCM registration token's length — real tokens
/// are ~150-200 bytes; this is headroom against abuse, not a correctness
/// constraint (Google doesn't publish a hard cap).
const MAX_FCM_TOKEN_LEN: usize = 4096;

/// FCM tokens are opaque, not URLs — `embed_fetch::assert_url_allowed` is the
/// wrong tool here. This is a cheap sanity check instead: non-empty, bounded,
/// no control characters.
fn validate_fcm_token(token: &str) -> Result<(), String> {
    if token.is_empty() {
        return Err(String::from("token must not be empty"));
    }
    if token.len() > MAX_FCM_TOKEN_LEN {
        return Err(String::from("token exceeds maximum length"));
    }
    if token.chars().any(char::is_control) {
        return Err(String::from("token contains control characters"));
    }
    Ok(())
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
            match &input.subscription {
                PushSubscriptionInput::Web { endpoint, .. } => {
                    embed_fetch::assert_url_allowed(endpoint)
                        .await
                        .map_err(|e| ErrorResponse {
                            body: Json(ErrorBody {
                                error: String::from("InvalidRequest"),
                                message: format!("Invalid push endpoint: {e}"),
                            }),
                        })?;
                }
                PushSubscriptionInput::Fcm { token } => {
                    validate_fcm_token(token).map_err(|e| ErrorResponse {
                        body: Json(ErrorBody {
                            error: String::from("InvalidRequest"),
                            message: format!("Invalid FCM token: {e}"),
                        }),
                    })?;
                }
            }
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
        let (provider, endpoint, p256dh, auth) = match &input.subscription {
            PushSubscriptionInput::Web { endpoint, keys } => (
                "web",
                endpoint.as_str(),
                Some(keys.p256dh.as_str()),
                Some(keys.auth.as_str()),
            ),
            PushSubscriptionInput::Fcm { token } => ("fcm", token.as_str(), None, None),
        };
        push_subscriptions::upsert(
            &db,
            &caller_did,
            provider,
            endpoint,
            p256dh,
            auth,
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
            subscription: PushSubscriptionInput::Web {
                endpoint: String::from("https://1.1.1.1/abc"),
                keys: PushKeys {
                    p256dh: String::from("p256dh-key"),
                    auth: String::from("auth-key"),
                },
            },
        }
    }

    fn sample_fcm_input() -> RegisterPushInput {
        RegisterPushInput {
            platform: String::from("android"),
            subscription: PushSubscriptionInput::Fcm {
                token: String::from("fcm-registration-token"),
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
                let PushSubscriptionInput::Web { endpoint, keys } = input.subscription else {
                    panic!("expected a web subscription");
                };
                *captured.lock().unwrap() = Some((did, endpoint, keys.p256dh));
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
        assert_eq!(endpoint, "https://1.1.1.1/abc");
        assert_eq!(p256dh, "p256dh-key");
    }

    #[tokio::test]
    async fn rejects_endpoint_pointing_at_private_ip() {
        let db = mock_db();
        let input = RegisterPushInput {
            platform: String::from("web"),
            subscription: PushSubscriptionInput::Web {
                endpoint: String::from("http://127.0.0.1/abc"),
                keys: PushKeys {
                    p256dh: String::from("p256dh-key"),
                    auth: String::from("auth-key"),
                },
            },
        };

        let result = register_push_with(
            String::from("token"),
            input,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _| Box::pin(async { panic!("should not register a blocked endpoint") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn registers_an_fcm_subscription_without_url_validation() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let register = move |_: DatabaseConnection,
                             did: String,
                             input: RegisterPushInput|
              -> BoxFuture<'static, Result<(), DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                let PushSubscriptionInput::Fcm { token } = input.subscription else {
                    panic!("expected an fcm subscription");
                };
                *captured.lock().unwrap() = Some((did, token));
                Ok(())
            })
        };

        // A bare alphanumeric string would fail `assert_url_allowed` (no
        // scheme/host) — proves the Fcm branch never routes through it.
        let result = register_push_with(
            String::from("token"),
            sample_fcm_input(),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &register,
        )
        .await
        .unwrap();

        assert!(result.registered);
        let (did, token) = captured.lock().unwrap().take().unwrap();
        assert_eq!(did, "did:plc:me");
        assert_eq!(token, "fcm-registration-token");
    }

    #[tokio::test]
    async fn rejects_empty_fcm_token() {
        let db = mock_db();
        let input = RegisterPushInput {
            platform: String::from("android"),
            subscription: PushSubscriptionInput::Fcm {
                token: String::new(),
            },
        };

        let result = register_push_with(
            String::from("token"),
            input,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _| Box::pin(async { panic!("should not register an empty token") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
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
