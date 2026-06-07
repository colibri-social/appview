use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::notifications::{self, NotificationMessage, NotificationView};
use crate::lib::responses::ErrorResponse;
use crate::models::notifications as notifications_model;

const DEFAULT_LIMIT: u64 = 50;

#[derive(Serialize, Debug)]
pub struct ListNotificationsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub notifications: Vec<NotificationView>,
}

type FetchFn = dyn Fn(
        DatabaseConnection,
        String,
        u64,
        Option<String>,
    ) -> BoxFuture<'static, Result<Vec<notifications_model::Model>, DbErr>>
    + Send
    + Sync;
type HydrateFn = dyn Fn(
        DatabaseConnection,
        Vec<String>,
    ) -> BoxFuture<'static, Result<HashMap<String, NotificationMessage>, DbErr>>
    + Send
    + Sync;

async fn list_notifications_with(
    auth: String,
    limit: Option<u64>,
    cursor: Option<String>,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    fetch_fn: &FetchFn,
    hydrate_fn: &HydrateFn,
) -> Result<Json<ListNotificationsResponse>, ErrorResponse> {
    with_authenticated(
        auth,
        "social.colibri.notification.listNotifications",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            let effective_limit = limit.unwrap_or(DEFAULT_LIMIT);
            let rows = fetch_fn(db.clone(), caller_did, effective_limit, cursor).await?;

            let next_cursor = if (rows.len() as u64) == effective_limit {
                rows.last().map(|r| r.id.to_string())
            } else {
                None
            };

            let message_uris: Vec<String> = rows.iter().map(|r| r.message_uri.clone()).collect();
            let mut hydrated = hydrate_fn(db, message_uris).await?;

            let notifications = rows
                .into_iter()
                .map(|row| {
                    let message = hydrated.remove(&row.message_uri);
                    NotificationView::from_row(row, message)
                })
                .collect();

            Ok(Json(ListNotificationsResponse {
                cursor: next_cursor,
                notifications,
            }))
        },
    )
    .await
}

fn fetch_boxed(
    db: DatabaseConnection,
    did: String,
    limit: u64,
    cursor: Option<String>,
) -> BoxFuture<'static, Result<Vec<notifications_model::Model>, DbErr>> {
    Box::pin(
        async move { notifications::list_notifications(&db, &did, limit, cursor.as_deref()).await },
    )
}

fn hydrate_boxed(
    db: DatabaseConnection,
    uris: Vec<String>,
) -> BoxFuture<'static, Result<HashMap<String, NotificationMessage>, DbErr>> {
    Box::pin(async move { notifications::hydrate_messages(&db, &uris).await })
}

#[get("/xrpc/social.colibri.notification.listNotifications?<limit>&<cursor>&<auth>")]
/// Lists notifications for the authenticated user, newest first.
pub async fn list_notifications(
    auth: &str,
    limit: Option<u64>,
    cursor: Option<&str>,
    db: &State<DatabaseConnection>,
) -> Result<Json<ListNotificationsResponse>, ErrorResponse> {
    list_notifications_with(
        auth.to_string(),
        limit,
        cursor.map(|c| c.to_string()),
        db.inner().clone(),
        &verify_auth_boxed,
        &fetch_boxed,
        &hydrate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn row(id: i64, kind: &str) -> notifications_model::Model {
        notifications_model::Model {
            id,
            recipient_did: String::from("did:plc:me"),
            kind: kind.to_string(),
            message_uri: format!("at://did:plc:author/social.colibri.message/m{id}"),
            author_did: String::from("did:plc:author"),
            channel_uri: String::from("at://did:plc:owner/social.colibri.channel.text/chan-a"),
            indexed_at: String::from("2026-05-14T00:00:00Z"),
            seen_at: None,
        }
    }

    fn sample_message(text: &str) -> NotificationMessage {
        NotificationMessage {
            text: text.to_string(),
            facets: vec![],
            created_at: String::from("2026-05-14T00:00:00Z"),
            parent: None,
            attachments: vec![],
            edited: None,
        }
    }

    #[tokio::test]
    async fn returns_rows_with_hydrated_messages_and_cursor_when_page_is_full() {
        let db = mock_db();
        let result = list_notifications_with(
            String::from("token"),
            Some(2),
            None,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _, _| Box::pin(async { Ok(vec![row(20, "mention"), row(10, "reply")]) }),
            &|_, uris| {
                Box::pin(async move {
                    let mut map = HashMap::new();
                    for uri in uris {
                        let text = format!("body of {uri}");
                        map.insert(uri, sample_message(&text));
                    }
                    Ok(map)
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.notifications.len(), 2);
        assert_eq!(result.notifications[0].id, 20);
        assert_eq!(
            result.notifications[0].message.as_ref().unwrap().text,
            "body of at://did:plc:author/social.colibri.message/m20"
        );
        assert_eq!(result.notifications[1].kind, "reply");
        assert_eq!(result.cursor.as_deref(), Some("10"));
    }

    #[tokio::test]
    async fn omits_message_when_hydration_returns_no_match() {
        let db = mock_db();
        let result = list_notifications_with(
            String::from("token"),
            Some(10),
            None,
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _, _| Box::pin(async { Ok(vec![row(1, "mention")]) }),
            &|_, _| Box::pin(async { Ok(HashMap::new()) }),
        )
        .await
        .unwrap();

        assert!(result.cursor.is_none());
        assert!(result.notifications[0].message.is_none());
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = list_notifications_with(
            String::from("token"),
            None,
            None,
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _, _| Box::pin(async { panic!("should not fetch when auth fails") }),
            &|_, _| Box::pin(async { panic!("should not hydrate when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
