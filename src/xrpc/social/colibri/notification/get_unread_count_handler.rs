use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::notifications;
use crate::lib::responses::ErrorResponse;

#[derive(Serialize, Debug)]
pub struct UnreadCountResponse {
    pub count: u64,
}

type CountFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<u64, DbErr>> + Send + Sync;

async fn get_unread_count_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    count_fn: &CountFn,
) -> Result<Json<UnreadCountResponse>, ErrorResponse> {
    with_authenticated(
        auth,
        "social.colibri.notification.getUnreadCount",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            let count = count_fn(db, caller_did).await?;
            Ok(Json(UnreadCountResponse { count }))
        },
    )
    .await
}

fn count_boxed(db: DatabaseConnection, did: String) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move { notifications::unseen_count(&db, &did).await })
}

#[get("/xrpc/social.colibri.notification.getUnreadCount?<auth>")]
/// Returns the number of unseen notifications for the authenticated user.
pub async fn get_unread_count(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<UnreadCountResponse>, ErrorResponse> {
    get_unread_count_with(
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &count_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_count_from_helper() {
        let db = mock_db();
        let result = get_unread_count_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, did| {
                assert_eq!(did, "did:plc:me");
                Box::pin(async { Ok(7) })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.count, 7);
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = get_unread_count_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _| Box::pin(async { panic!("should not count when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
