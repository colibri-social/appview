use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QuerySelect};
use serde::Serialize;

use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::models::record_data;

const MUTE_NSID: &str = "social.colibri.actor.mute";
const MAX_MUTES: u64 = 1000;

#[derive(Serialize, Debug)]
pub struct Mute {
    pub uri: String,
    pub subject: String,
}

#[derive(Serialize, Debug)]
pub struct ListMutesResponse {
    pub mutes: Vec<Mute>,
}

#[derive(serde::Deserialize)]
struct StoredMute {
    subject: String,
}

/// Fetches every `social.colibri.actor.mute` record authored by `did`.
pub async fn fetch_mutes(
    db: &DatabaseConnection,
    did: &str,
) -> Result<Vec<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(did))
        .filter(record_data::Column::Nsid.eq(MUTE_NSID))
        .limit(MAX_MUTES)
        .all(db)
        .await
}

/// Fetches every `social.colibri.actor.mute` record authored by any of
/// `dids`, grouped by author. Used where mutes for many members must be
/// checked against a single message (e.g. expanding "notify everyone" to
/// exclude members who muted the channel) without one query per member.
pub async fn fetch_mutes_for_dids(
    db: &DatabaseConnection,
    dids: &[String],
) -> Result<HashMap<String, Vec<record_data::Model>>, DbErr> {
    if dids.is_empty() {
        return Ok(HashMap::new());
    }

    let records = record_data::Entity::find()
        .filter(record_data::Column::Did.is_in(dids.to_vec()))
        .filter(record_data::Column::Nsid.eq(MUTE_NSID))
        .limit(MAX_MUTES * dids.len() as u64)
        .all(db)
        .await?;

    let mut by_did: HashMap<String, Vec<record_data::Model>> = HashMap::new();
    for record in records {
        by_did.entry(record.did.clone()).or_default().push(record);
    }
    Ok(by_did)
}

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type FetchMutesFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>>
    + Send
    + Sync;

async fn list_mutes_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    fetch_mutes_fn: &FetchMutesFn,
) -> Result<Json<ListMutesResponse>, ErrorResponse> {
    let did = verify_auth_fn(auth, String::from("social.colibri.actor.listMutes"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let records = fetch_mutes_fn(db, did).await?;

    // Skip rows whose payload lacks a `subject` rather than failing the whole
    // request — a malformed record shouldn't blind the client to the rest.
    let mutes = records
        .into_iter()
        .filter_map(|record| {
            let stored: StoredMute = serde_json::from_value(record.data.clone()).ok()?;
            Some(Mute {
                uri: format!("at://{}/{}/{}", record.did, record.nsid, record.rkey),
                subject: stored.subject,
            })
        })
        .collect();

    Ok(Json(ListMutesResponse { mutes }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn fetch_mutes_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>> {
    Box::pin(async move { fetch_mutes(&db, &did).await })
}

#[get("/xrpc/social.colibri.actor.listMutes?<auth>")]
/// Returns every channel/community the authenticated user has muted.
pub async fn list_mutes(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ListMutesResponse>, ErrorResponse> {
    list_mutes_with(
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &fetch_mutes_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn mute_record(rkey: &str, subject: &str) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from("did:plc:me"),
            nsid: String::from("social.colibri.actor.mute"),
            rkey: rkey.to_string(),
            data: serde_json::json!({ "subject": subject }),
            indexed_at: String::from(""),
        }
    }

    #[tokio::test]
    async fn returns_mutes_from_stored_records() {
        let db = mock_db();
        let result = list_mutes_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _| {
                Box::pin(async {
                    Ok(vec![
                        mute_record(
                            "chan-a",
                            "at://did:plc:owner/social.colibri.channel.text/chan-a",
                        ),
                        mute_record(
                            "did:plc:owner",
                            "at://did:plc:owner/social.colibri.community/self",
                        ),
                    ])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.mutes.len(), 2);
        assert_eq!(
            result.mutes[0].uri,
            "at://did:plc:me/social.colibri.actor.mute/chan-a"
        );
        assert_eq!(
            result.mutes[0].subject,
            "at://did:plc:owner/social.colibri.channel.text/chan-a"
        );
        assert_eq!(
            result.mutes[1].subject,
            "at://did:plc:owner/social.colibri.community/self"
        );
    }

    #[tokio::test]
    async fn returns_empty_when_no_mutes_exist() {
        let db = mock_db();
        let result = list_mutes_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _| Box::pin(async { Ok(vec![]) }),
        )
        .await
        .unwrap();

        assert!(result.mutes.is_empty());
    }

    #[tokio::test]
    async fn fetch_mutes_for_dids_groups_records_by_author() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                record_data::Model {
                    id: 1,
                    did: String::from("did:plc:alice"),
                    nsid: MUTE_NSID.to_string(),
                    rkey: String::from("r1"),
                    data: serde_json::json!({ "subject": "at://did:plc:owner/social.colibri.community/self" }),
                    indexed_at: String::new(),
                },
                record_data::Model {
                    id: 2,
                    did: String::from("did:plc:bob"),
                    nsid: MUTE_NSID.to_string(),
                    rkey: String::from("r2"),
                    data: serde_json::json!({ "subject": "at://did:plc:owner/social.colibri.channel.text/general" }),
                    indexed_at: String::new(),
                },
            ]])
            .into_connection();

        let grouped = fetch_mutes_for_dids(
            &db,
            &[String::from("did:plc:alice"), String::from("did:plc:bob")],
        )
        .await
        .unwrap();

        assert_eq!(grouped.get("did:plc:alice").unwrap().len(), 1);
        assert_eq!(grouped.get("did:plc:bob").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn fetch_mutes_for_dids_returns_empty_map_for_no_dids() {
        let db = mock_db();
        let grouped = fetch_mutes_for_dids(&db, &[]).await.unwrap();
        assert!(grouped.is_empty());
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = list_mutes_with(
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _| Box::pin(async { panic!("should not fetch when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
