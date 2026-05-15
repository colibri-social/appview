use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::models::record_data;

#[derive(Serialize, Debug)]
pub struct ReadCursor {
    pub uri: String,
    pub cursor: String,
    pub channel: String,
}

#[derive(serde::Deserialize)]
struct StoredCursor {
    cursor: String,
}

pub async fn fetch_latest_read_cursor(
    db: &DatabaseConnection,
    did: &str,
    channel_uri: &str,
) -> Result<Option<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(did))
        .filter(record_data::Column::Nsid.eq("social.colibri.channel.read"))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'channel' = $1"#,
            vec![sea_orm::Value::from(channel_uri.to_string())],
        ))
        .order_by_desc(record_data::Column::Rkey)
        .limit(1)
        .one(db)
        .await
}

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type FetchCursorFn = dyn Fn(
        DatabaseConnection,
        String,
        String,
    ) -> BoxFuture<'static, Result<Option<record_data::Model>, DbErr>>
    + Send
    + Sync;

async fn get_read_cursor_with(
    channel_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    fetch_cursor_fn: &FetchCursorFn,
) -> Result<Json<ReadCursor>, ErrorResponse> {
    if AtUri::parse(&channel_uri).is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid channel AT-URI."),
            }),
        });
    }

    let did = verify_auth_fn(auth, String::from("social.colibri.channel.getReadCursor"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let maybe_record = fetch_cursor_fn(db, did, channel_uri.clone()).await?;

    let record = maybe_record.ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("NotFound"),
            message: String::from("No read cursor exists for this channel."),
        }),
    })?;

    let stored: StoredCursor =
        serde_json::from_value(record.data.clone()).map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InternalServerError"),
                message: e.to_string(),
            }),
        })?;

    Ok(Json(ReadCursor {
        uri: format!("at://{}/{}/{}", record.did, record.nsid, record.rkey),
        cursor: stored.cursor,
        channel: channel_uri,
    }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn fetch_cursor_boxed(
    db: DatabaseConnection,
    did: String,
    channel_uri: String,
) -> BoxFuture<'static, Result<Option<record_data::Model>, DbErr>> {
    Box::pin(async move { fetch_latest_read_cursor(&db, &did, &channel_uri).await })
}

#[get("/xrpc/social.colibri.channel.getReadCursor?<channel>&<auth>")]
/// Returns the latest read cursor for the authenticated user and channel.
pub async fn get_read_cursor(
    channel: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReadCursor>, ErrorResponse> {
    get_read_cursor_with(
        channel.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &fetch_cursor_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn cursor_record(rkey: &str, cursor: &str) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from("did:plc:me"),
            nsid: String::from("social.colibri.channel.read"),
            rkey: rkey.to_string(),
            data: serde_json::json!({
                "channel": "at://did:plc:owner/social.colibri.channel/chan-a",
                "cursor": cursor,
            }),
        }
    }

    #[tokio::test]
    async fn returns_cursor_from_stored_record() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = get_read_cursor_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(Some(cursor_record(
                        "rkey-latest",
                        "2026-05-13T12:34:56.000Z",
                    )))
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(
            result.channel,
            "at://did:plc:owner/social.colibri.channel/chan-a"
        );
        assert_eq!(
            result.uri,
            "at://did:plc:me/social.colibri.channel.read/rkey-latest"
        );
        assert_eq!(result.cursor, "2026-05-13T12:34:56.000Z");
    }

    #[tokio::test]
    async fn rejects_invalid_channel_uri() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = get_read_cursor_with(
            String::from("not-a-uri"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate when uri is invalid") }),
            &|_, _, _| Box::pin(async { panic!("should not fetch when uri is invalid") }),
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
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = get_read_cursor_with(
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

    #[tokio::test]
    async fn returns_not_found_when_no_cursor_exists() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = get_read_cursor_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _| Box::pin(async { Ok(None) }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }
}
