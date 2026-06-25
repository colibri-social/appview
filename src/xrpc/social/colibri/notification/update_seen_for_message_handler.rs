use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::events::{SeenEvent, SeenEventData};
use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::notifications;
use crate::lib::responses::ErrorResponse;
use crate::lib::tap::CommsBridge;
use crate::lib::time::current_iso8601_utc;

#[derive(Serialize, Debug)]
pub struct UpdateSeenForMessageResponse {
    pub updated: u64,
}

type MarkSeenFn = dyn Fn(DatabaseConnection, String, String, String) -> BoxFuture<'static, Result<u64, DbErr>>
    + Send
    + Sync;
type FetchChannelFn = dyn Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<Option<String>, DbErr>>
    + Send
    + Sync;
type BroadcastFn = dyn Fn(SeenEvent) + Send + Sync;

async fn update_seen_for_message_with(
    message_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    mark_seen_fn: &MarkSeenFn,
    fetch_channel_fn: &FetchChannelFn,
    broadcast_fn: &BroadcastFn,
) -> Result<Json<UpdateSeenForMessageResponse>, ErrorResponse> {
    with_authenticated(
        auth,
        "social.colibri.notification.updateSeenForMessage",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            let seen_at = current_iso8601_utc();

            // Resolve the channel before marking, so a `message_seen` event can
            // tell the user's other clients which channel badge to decrement.
            let channel =
                fetch_channel_fn(db.clone(), caller_did.clone(), message_uri.clone()).await?;
            let updated =
                mark_seen_fn(db, caller_did.clone(), message_uri.clone(), seen_at).await?;

            // Only broadcast when something actually changed (idempotent re-clears
            // shouldn't echo) and we know which channel to target.
            if updated > 0
                && let Some(channel_uri) = channel
            {
                broadcast_fn(SeenEvent {
                    recipient_did: caller_did,
                    data: SeenEventData {
                        event: String::from("message_seen"),
                        channel_uri,
                        message_uri: Some(message_uri),
                        cleared: Some(updated),
                    },
                });
            }

            Ok(Json(UpdateSeenForMessageResponse { updated }))
        },
    )
    .await
}

fn mark_seen_boxed(
    db: DatabaseConnection,
    did: String,
    message_uri: String,
    seen_at: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move {
        notifications::mark_seen_for_message(&db, &did, &message_uri, &seen_at).await
    })
}

fn fetch_channel_boxed(
    db: DatabaseConnection,
    did: String,
    message_uri: String,
) -> BoxFuture<'static, Result<Option<String>, DbErr>> {
    Box::pin(async move {
        notifications::channel_for_message_notification(&db, &did, &message_uri).await
    })
}

#[post("/xrpc/social.colibri.notification.updateSeenForMessage?<message>&<auth>")]
/// Marks the authenticated user's unseen notifications for a single message as
/// seen. Used for per-message ping clearing as a message scrolls into view.
/// Broadcasts a `message_seen` `seen_event` to the user's other clients.
pub async fn update_seen_for_message(
    message: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
) -> Result<Json<UpdateSeenForMessageResponse>, ErrorResponse> {
    let sender = bridge.seen.clone();
    update_seen_for_message_with(
        message.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &mark_seen_boxed,
        &fetch_channel_boxed,
        &move |event| {
            let _ = sender.send(event);
        },
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
    async fn marks_the_given_message_seen_and_broadcasts() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();
        let events: Arc<Mutex<Vec<SeenEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        let mark_seen = move |_: DatabaseConnection,
                              did: String,
                              message: String,
                              _seen_at: String|
              -> BoxFuture<'static, Result<u64, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((did, message));
                Ok(2)
            })
        };

        let result = update_seen_for_message_with(
            String::from("at://did:plc:author/social.colibri.message/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &mark_seen,
            &|_, _, _| {
                Box::pin(async {
                    Ok(Some(String::from(
                        "at://did:plc:owner/social.colibri.channel/chan-a",
                    )))
                })
            },
            &move |event| events_clone.lock().unwrap().push(event),
        )
        .await
        .unwrap();

        assert_eq!(result.updated, 2);
        let (did, message) = captured.lock().unwrap().take().unwrap();
        assert_eq!(did, "did:plc:me");
        assert_eq!(message, "at://did:plc:author/social.colibri.message/m1");

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].recipient_did, "did:plc:me");
        assert_eq!(events[0].data.event, "message_seen");
        assert_eq!(
            events[0].data.channel_uri,
            "at://did:plc:owner/social.colibri.channel/chan-a"
        );
        assert_eq!(events[0].data.cleared, Some(2));
    }

    #[tokio::test]
    async fn does_not_broadcast_when_nothing_was_unseen() {
        let db = mock_db();
        let events: Arc<Mutex<Vec<SeenEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();

        let result = update_seen_for_message_with(
            String::from("at://did:plc:author/social.colibri.message/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _, _| Box::pin(async { Ok(0) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(Some(String::from(
                        "at://did:plc:owner/social.colibri.channel/chan-a",
                    )))
                })
            },
            &move |event| events_clone.lock().unwrap().push(event),
        )
        .await
        .unwrap();

        assert_eq!(result.updated, 0);
        assert!(events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = update_seen_for_message_with(
            String::from("at://did:plc:author/social.colibri.message/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _, _| Box::pin(async { panic!("should not mark when auth fails") }),
            &|_, _, _| Box::pin(async { panic!("should not fetch when auth fails") }),
            &|_| panic!("should not broadcast when auth fails"),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
