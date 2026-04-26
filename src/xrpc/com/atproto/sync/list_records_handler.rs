use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::DatabaseConnection;
use serde::Serialize;
use serde_json::Value;

use crate::lib::list_atproto_records::list_atproto_records;
use crate::lib::responses::{ErrorBody, ErrorResponse};

#[derive(Serialize, Debug)]
pub struct ListRecordsResponse {
    pub cursor: Option<String>,
    pub records: Vec<Value>,
}

async fn list_records_with_db(
    db: &DatabaseConnection,
    repo: &str,
    collection: &str,
    limit: Option<u64>,
    cursor: Option<&str>,
    reverse: Option<bool>,
) -> Result<Json<ListRecordsResponse>, ErrorResponse> {
    let records = list_atproto_records::<Value>(
        repo.to_string(),
        collection.to_string(),
        limit,
        cursor,
        reverse,
        db,
    )
    .await;

    if records.is_err() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InternalServerError"),
                message: String::from("An error occurred while attempting to fetch the records."),
            }),
        });
    }

    let safe_records = records.unwrap();

    Ok(Json(ListRecordsResponse {
        cursor: Some(String::from(cursor.unwrap_or(""))),
        records: safe_records,
    }))
}

#[get("/xrpc/com.atproto.sync.listRecords?<repo>&<collection>&<limit>&<cursor>&<reverse>")]
/// Returns a cached record from the database.
pub async fn list_records(
    repo: &str,
    collection: &str,
    limit: Option<u64>,
    cursor: Option<&str>,
    reverse: Option<bool>,
    db: &State<DatabaseConnection>,
) -> Result<Json<ListRecordsResponse>, ErrorResponse> {
    list_records_with_db(db.inner(), repo, collection, limit, cursor, reverse).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use serde_json::json;

    #[tokio::test]
    async fn list_records_returns_records() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![crate::models::record_data::Model {
                id: 1,
                did: String::from("did:plc:abc"),
                nsid: String::from("social.colibri.message"),
                rkey: String::from("r1"),
                data: json!({"text":"hello"}),
            }]])
            .into_connection();

        let result = list_records_with_db(
            &db,
            "did:plc:abc",
            "social.colibri.message",
            Some(10),
            Some("cursor"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.cursor.as_deref(), Some("cursor"));
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0]["text"], "hello");
    }

    #[tokio::test]
    async fn list_records_returns_internal_error() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_errors([sea_orm::DbErr::Custom(String::from("boom"))])
            .into_connection();

        let result = list_records_with_db(
            &db,
            "did:plc:none",
            "social.colibri.message",
            Some(10),
            None,
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InternalServerError"
        );
    }
}
