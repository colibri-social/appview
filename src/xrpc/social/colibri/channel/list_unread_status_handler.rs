use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::channel_unread::{self, ChannelUnreadStatus};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
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

fn not_a_member() -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("Forbidden"),
            message: String::from("caller is not a member of this community"),
        }),
    }
}

async fn list_unread_status_with(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    compute_fn: &ComputeFn,
) -> Result<Json<ListUnreadStatusResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.channel.listUnreadStatus",
        community_uri,
        None,
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            // Not gated by a specific permission — any member may read their
            // own unread status — but this echoes back the community's full
            // channel list, so a non-member shouldn't be able to use it to
            // enumerate channel URIs of a community they haven't joined.
            if !ctx.authz.is_owner && ctx.authz.member.is_none() {
                return Err(not_a_member());
            }

            let channels = compute_fn(db, ctx.caller_did, ctx.community).await?;
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
        &load_authz_boxed,
        &compute_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::{empty_authz, mock_db, owner_authz};
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
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, did, community| {
                assert_eq!(did, "did:plc:owner");
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
            &|_, _, _| Box::pin(async { panic!("should not load authz when uri is invalid") }),
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
            &|_, _, _| Box::pin(async { panic!("should not load authz when auth fails") }),
            &|_, _, _| Box::pin(async { panic!("should not compute when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }

    #[tokio::test]
    async fn rejects_caller_who_is_not_a_member() {
        let db = mock_db();
        let result = list_unread_status_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| Box::pin(async { Ok(empty_authz()) }),
            &|_, _, _| Box::pin(async { panic!("should not compute for a non-member") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }
}
