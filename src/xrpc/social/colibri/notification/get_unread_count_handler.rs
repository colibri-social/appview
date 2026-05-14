use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::notifications;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};

#[derive(Serialize, Debug)]
pub struct UnreadCountResponse {
    pub count: u64,
}

async fn get_unread_count_with<V, C>(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: V,
    count_fn: C,
) -> Result<Json<UnreadCountResponse>, ErrorResponse>
where
    V: Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>,
    C: Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<u64, DbErr>>,
{
    let did = verify_auth_fn(
        auth,
        String::from("social.colibri.notification.getUnreadCount"),
    )
    .await
    .map_err(|e| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: e.to_string(),
        }),
    })?;

    let count = count_fn(db, did).await?;
    Ok(Json(UnreadCountResponse { count }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn count_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
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
        verify_auth_boxed,
        count_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn returns_count_from_helper() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = get_unread_count_with(
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            |_, did| {
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
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = get_unread_count_with(
            String::from("token"),
            db,
            |_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            |_, _| Box::pin(async { panic!("should not count when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
