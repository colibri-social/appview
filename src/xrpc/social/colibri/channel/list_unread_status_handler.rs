use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::channel_unread::{self, ChannelUnreadStatus};
use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::responses::{ErrorBody, ErrorResponse};

#[derive(Serialize, Debug)]
pub struct ListUnreadStatusResponse {
    pub channels: Vec<ChannelUnreadStatus>,
}

type ComputeFn = dyn Fn(
        DatabaseConnection,
        String,
        AtUri,
    ) -> BoxFuture<'static, Result<Vec<ChannelUnreadStatus>, DbErr>>
    + Send
    + Sync;

async fn list_unread_status_with(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    compute_fn: &ComputeFn,
) -> Result<Json<ListUnreadStatusResponse>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    with_authenticated(
        auth,
        "social.colibri.channel.listUnreadStatus",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            let channels = compute_fn(db, caller_did, community).await?;
            Ok(Json(ListUnreadStatusResponse { channels }))
        },
    )
    .await
}

fn compute_boxed(
    db: DatabaseConnection,
    caller_did: String,
    community: AtUri,
) -> BoxFuture<'static, Result<Vec<ChannelUnreadStatus>, DbErr>> {
    Box::pin(async move {
        channel_unread::community_channel_unread_status(&db, &caller_did, &community).await
    })
}

#[get("/xrpc/social.colibri.channel.listUnreadStatus?<community>&<auth>")]
/// Returns per-channel unread status (white-dot + red-ping-count) for every
/// channel in a community, for the authenticated user.
pub async fn list_unread_status(
    community: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ListUnreadStatusResponse>, ErrorResponse> {
    list_unread_status_with(
        community.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &compute_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn status(uri: &str, unread: bool, pings: u64) -> ChannelUnreadStatus {
        ChannelUnreadStatus {
            channel_uri: uri.to_string(),
            has_unread_messages: unread,
            unread_ping_count: pings,
        }
    }

    #[tokio::test]
    async fn returns_statuses_from_compute_fn() {
        let db = mock_db();
        let result = list_unread_status_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, did, community| {
                assert_eq!(did, "did:plc:me");
                assert_eq!(community.rkey, "c1");
                Box::pin(async {
                    Ok(vec![
                        status("at://did:plc:owner/social.colibri.channel/a", true, 2),
                        status("at://did:plc:owner/social.colibri.channel/b", false, 0),
                    ])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.channels.len(), 2);
        assert!(result.channels[0].has_unread_messages);
        assert_eq!(result.channels[0].unread_ping_count, 2);
        assert!(!result.channels[1].has_unread_messages);
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = list_unread_status_with(
            String::from("not-a-uri"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate when uri is invalid") }),
            &|_, _, _| Box::pin(async { panic!("should not compute when uri is invalid") }),
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
        let result = list_unread_status_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _| Box::pin(async { panic!("should not compute when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
