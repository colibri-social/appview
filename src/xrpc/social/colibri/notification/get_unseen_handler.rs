use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::notifications;
use crate::lib::responses::ErrorResponse;
use crate::models::notifications as notifications_model;

/// A single unseen notification, trimmed to what a client needs to watch for
/// the originating message scrolling into view.
#[derive(Serialize, Debug)]
pub struct UnseenNotification {
    pub id: i64,
    #[serde(rename = "messageUri")]
    pub message_uri: String,
    #[serde(rename = "indexedAt")]
    pub indexed_at: String,
}

#[derive(Serialize, Debug)]
pub struct GetUnseenResponse {
    pub notifications: Vec<UnseenNotification>,
}

type FetchFn = dyn Fn(
        DatabaseConnection,
        String,
        String,
    ) -> BoxFuture<'static, Result<Vec<notifications_model::Model>, DbErr>>
    + Send
    + Sync;

async fn get_unseen_with(
    channel_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    fetch_fn: &FetchFn,
) -> Result<Json<GetUnseenResponse>, ErrorResponse> {
    with_authenticated(
        auth,
        "social.colibri.notification.getUnseen",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            let rows = fetch_fn(db, caller_did, channel_uri).await?;
            let notifications = rows
                .into_iter()
                .map(|row| UnseenNotification {
                    id: row.id,
                    message_uri: row.message_uri,
                    indexed_at: row.indexed_at,
                })
                .collect();
            Ok(Json(GetUnseenResponse { notifications }))
        },
    )
    .await
}

fn fetch_boxed(
    db: DatabaseConnection,
    did: String,
    channel_uri: String,
) -> BoxFuture<'static, Result<Vec<notifications_model::Model>, DbErr>> {
    Box::pin(async move { notifications::list_unseen_for_channel(&db, &did, &channel_uri).await })
}

#[get("/xrpc/social.colibri.notification.getUnseen?<channel>&<auth>")]
/// Lists the authenticated user's unseen notifications within one channel, so a
/// client can clear each ping as its message scrolls into view.
pub async fn get_unseen(
    channel: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<GetUnseenResponse>, ErrorResponse> {
    get_unseen_with(
        channel.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &fetch_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn row(id: i64) -> notifications_model::Model {
        notifications_model::Model {
            id,
            recipient_did: String::from("did:plc:me"),
            kind: String::from("mention"),
            message_uri: format!("at://did:plc:author/social.colibri.message/m{id}"),
            author_did: String::from("did:plc:author"),
            channel_uri: String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            indexed_at: String::from("2026-05-14T00:00:00Z"),
            seen_at: None,
            mention_role_name: None,
        }
    }

    #[tokio::test]
    async fn returns_unseen_rows_for_channel() {
        let db = mock_db();
        let result = get_unseen_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, did, channel| {
                assert_eq!(did, "did:plc:me");
                assert_eq!(channel, "at://did:plc:owner/social.colibri.channel/chan-a");
                Box::pin(async { Ok(vec![row(20), row(10)]) })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.notifications.len(), 2);
        assert_eq!(result.notifications[0].id, 20);
        assert_eq!(
            result.notifications[0].message_uri,
            "at://did:plc:author/social.colibri.message/m20"
        );
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = get_unseen_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _| Box::pin(async { panic!("should not fetch when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
