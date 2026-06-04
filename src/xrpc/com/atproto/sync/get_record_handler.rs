use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::DatabaseConnection;
use serde::Serialize;
use serde_json::Value;

use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::responses::{ErrorBody, ErrorResponse};

#[derive(Serialize, Debug)]
pub struct GetRecordResponse {
    pub uri: String,
    pub value: Value,
}

async fn get_record_with_db(
    db: &DatabaseConnection,
    repo: &str,
    collection: &str,
    rkey: &str,
) -> Result<Json<GetRecordResponse>, ErrorResponse> {
    let record = get_atproto_record::<Value>(
        repo.to_string(),
        collection.to_string(),
        rkey.to_string(),
        db,
    )
    .await;

    if record.is_err() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("NotFound"),
                message: String::from("Unable to find record in AppView cache."),
            }),
        });
    }

    let safe_record = record.unwrap();

    Ok(Json(GetRecordResponse {
        uri: format!("at://{}/{}/{}", repo, collection, rkey),
        value: safe_record,
    }))
}

#[get("/xrpc/com.atproto.sync.getRecord?<repo>&<collection>&<rkey>")]
/// Returns a cached record from the database.
pub async fn get_record(
    repo: &str,
    collection: &str,
    rkey: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<GetRecordResponse>, ErrorResponse> {
    get_record_with_db(db.inner(), repo, collection, rkey).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use serde_json::json;

    #[tokio::test]
    async fn get_record_returns_record() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![crate::models::record_data::Model {
                id: 1,
                did: String::from("did:plc:abc"),
                nsid: String::from("social.colibri.message"),
                rkey: String::from("r1"),
                data: json!({"text":"hello"}),
                indexed_at: String::from(""),
            }]])
            .into_connection();

        let result = get_record_with_db(&db, "did:plc:abc", "social.colibri.message", "r1")
            .await
            .unwrap();

        assert_eq!(
            result.uri,
            "at://did:plc:abc/social.colibri.message/r1".to_string()
        );
        assert_eq!(result.value["text"], "hello");
    }

    #[tokio::test]
    async fn get_record_returns_not_found() {
        let db = mock_db();

        let result = get_record_with_db(&db, "did:plc:none", "col", "rkey").await;

        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_body = err.body.into_inner();
        assert_eq!(err_body.error, "NotFound");
    }
}
