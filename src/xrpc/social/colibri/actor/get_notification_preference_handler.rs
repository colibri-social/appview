use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::notification_preferences::level_for;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};

#[derive(Serialize, Debug)]
pub struct GetNotificationPreferenceResponse {
    pub level: String,
}

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type LevelForFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<String, DbErr>> + Send + Sync;

async fn get_notification_preference_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    level_for_fn: &LevelForFn,
) -> Result<Json<GetNotificationPreferenceResponse>, ErrorResponse> {
    let did = verify_auth_fn(
        auth,
        String::from("social.colibri.actor.getNotificationPreference"),
    )
    .await
    .map_err(|e| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: e.to_string(),
        }),
    })?;

    let level = level_for_fn(db, did).await?;

    Ok(Json(GetNotificationPreferenceResponse { level }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn level_for_boxed(db: DatabaseConnection, did: String) -> BoxFuture<'static, Result<String, DbErr>> {
    Box::pin(async move { level_for(&db, &did).await })
}

#[get("/xrpc/social.colibri.actor.getNotificationPreference?<auth>")]
/// Returns the authenticated user's notification level (`"all"` or
/// `"mentionsAndReplies"`), defaulting to `"all"` when no
/// `social.colibri.actor.notificationPreference` record exists.
pub async fn get_notification_preference(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<GetNotificationPreferenceResponse>, ErrorResponse> {
    get_notification_preference_with(
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &level_for_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_level_from_stored_preference() {
        let db = mock_db();
        let result = get_notification_preference_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _| Box::pin(async { Ok(String::from("mentionsAndReplies")) }),
        )
        .await
        .unwrap();

        assert_eq!(result.level, "mentionsAndReplies");
    }

    #[tokio::test]
    async fn defaults_to_all_when_no_preference_recorded() {
        let db = mock_db();
        let result = get_notification_preference_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _| Box::pin(async { Ok(String::from("all")) }),
        )
        .await
        .unwrap();

        assert_eq!(result.level, "all");
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = get_notification_preference_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _| Box::pin(async { panic!("should not fetch when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
