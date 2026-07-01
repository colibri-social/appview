use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::notifications;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::xrpc::social::colibri::channel::get_read_cursor_handler::{
    ReadCursor, fetch_latest_read_cursor,
};
use crate::xrpc::social::colibri::channel::list_messages_handler::{Message, build_message_list};
use crate::xrpc::social::colibri::notification::get_unseen_handler::UnseenNotification;

#[derive(Serialize, Debug)]
pub struct GetChannelViewResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub messages: Vec<Message>,
    #[serde(rename = "readCursor", skip_serializing_if = "Option::is_none")]
    pub read_cursor: Option<ReadCursor>,
    pub unseen: Vec<UnseenNotification>,
}

async fn get_channel_view_with(
    channel_uri: String,
    auth: String,
    limit: Option<u64>,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
) -> Result<Json<GetChannelViewResponse>, ErrorResponse> {
    if AtUri::parse(&channel_uri).is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid channel AT-URI."),
            }),
        });
    }

    with_authenticated(
        auth,
        "social.colibri.channel.getChannelView",
        db,
        verify_auth_fn,
        |caller_did, db| async move {
            let list = build_message_list(&db, &channel_uri, limit, None, false).await?;

            let read_cursor = fetch_latest_read_cursor(&db, &caller_did, &channel_uri)
                .await?
                .and_then(|record| {
                    let cursor = record.data.get("cursor")?.as_str()?.to_string();
                    Some(ReadCursor {
                        uri: format!("at://{}/{}/{}", record.did, record.nsid, record.rkey),
                        cursor,
                        channel: channel_uri.clone(),
                    })
                });

            let unseen = notifications::list_unseen_for_channel(&db, &caller_did, &channel_uri)
                .await?
                .into_iter()
                .map(|row| UnseenNotification {
                    id: row.id,
                    message_uri: row.message_uri,
                    indexed_at: row.indexed_at,
                })
                .collect();

            Ok(Json(GetChannelViewResponse {
                cursor: list.cursor,
                messages: list.messages,
                read_cursor,
                unseen,
            }))
        },
    )
    .await
}

#[get("/xrpc/social.colibri.channel.getChannelView?<channel>&<limit>&<auth>")]
pub async fn get_channel_view(
    channel: &str,
    limit: Option<u64>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<GetChannelViewResponse>, ErrorResponse> {
    get_channel_view_with(
        channel.to_string(),
        auth.to_string(),
        limit,
        db.inner().clone(),
        &verify_auth_boxed,
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
    async fn rejects_invalid_channel_uri() {
        let db = mock_db();
        let result = get_channel_view_with(
            String::from("not-a-uri"),
            String::from("token"),
            None,
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate when uri is invalid") }),
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
        let result = get_channel_view_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            String::from("token"),
            None,
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
