use rocket::get;
use rocket::serde::json::Json;
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter};
use serde::Serialize;

use crate::lib::db::init_db;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::repo_records::{self, Entity as RepoRecord};

#[derive(Serialize, Debug)]
pub struct GetRecordResponse {
    pub uri: String,
    pub value: repo_records::Model,
    pub cid: Option<String>,
}

async fn get_record_with_db(
    db: &DatabaseConnection,
    repo: &str,
    collection: &str,
    rkey: &str,
    cid: Option<&str>,
) -> Result<Json<GetRecordResponse>, ErrorResponse> {
    let record = RepoRecord::find()
        .filter(
            Condition::any()
                .add(
                    Condition::all()
                        .add(repo_records::Column::Did.eq(repo))
                        .add(repo_records::Column::Collection.eq(collection))
                        .add(repo_records::Column::Rkey.eq(rkey)),
                )
                .add(Condition::all().add(repo_records::Column::Cid.eq(cid))),
        )
        .one(db)
        .await;

    if record.is_err() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("NotFound"),
                message: String::from("Unable to find record in AppView cache."),
            }),
        });
    }

    if record.as_ref().unwrap().is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("NotFound"),
                message: String::from("Unable to find record in AppView cache."),
            }),
        });
    }

    let safe_record = record.unwrap().unwrap();

    // TODO: This returns the wrong data - it should return the actual record, not Tap's reference to it.
    Ok(Json(GetRecordResponse {
        uri: format!(
            "at://{}/{}/{}",
            safe_record.did, safe_record.collection, safe_record.rkey
        ),
        cid: if safe_record.cid.is_empty() {
            None
        } else {
            Some(safe_record.cid.clone())
        },
        value: safe_record,
    }))
}

#[get("/xrpc/com.atproto.repo.getRecord?<repo>&<collection>&<rkey>&<cid>")]
/// Returns a cached record from the database.
pub async fn get_record(
    repo: &str,
    collection: &str,
    rkey: &str,
    cid: Option<&str>,
) -> Result<Json<GetRecordResponse>, ErrorResponse> {
    let db = init_db().await?;
    get_record_with_db(&db, repo, collection, rkey, cid).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn get_record_returns_record() {
        let model = repo_records::Model {
            did: "did:plc:example".to_string(),
            collection: "social.colibri.message".to_string(),
            rkey: "rkey123".to_string(),
            cid: "cid123".to_string(),
        };

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results(vec![vec![model.clone()]])
            .into_connection();

        let result = get_record_with_db(
            &db,
            "did:plc:example",
            "social.colibri.message",
            "rkey123",
            Some("cid123"),
        )
        .await;

        assert!(result.is_ok());

        let body = result.unwrap().into_inner();
        assert_eq!(
            body.uri,
            "at://did:plc:example/social.colibri.message/rkey123"
        );
        assert_eq!(body.cid, Some("cid123".to_string()));
        assert_eq!(body.value, model);
    }

    #[tokio::test]
    async fn get_record_returns_not_found() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();

        let result = get_record_with_db(&db, "did:plc:none", "col", "rkey", None).await;

        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_body = err.body.into_inner();
        assert_eq!(err_body.error, "NotFound");
    }
}
