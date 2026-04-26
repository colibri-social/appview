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
