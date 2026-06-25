//! `social.colibri.community.delete` — tears down a community.
//!
//! Requires the caller to hold the `community.delete` permission. The teardown
//! at the PDS depends on how the community is hosted:
//!
//! - **AppView-managed** (minted via `community.create`): the entire PDS
//!   account is deleted via `com.atproto.admin.deleteAccount`, which removes
//!   the repo and every record on it in one shot.
//! - **BYO**: the AppView does not administer the hosting PDS, and the DID is
//!   the user's own identity — so the account must survive. Instead we log in
//!   with the stored credentials and delete the community's records
//!   individually, emptying the repo while leaving the account intact.
//!
//! Regardless of source, the stored credentials and every locally cached
//! `record_data` row for the community DID are removed so the community no
//! longer surfaces through any read endpoint.

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::Serialize;

use crate::lib::community_credentials::{self, CommunityCredentials, SOURCE_APPVIEW_MANAGED};
use crate::lib::community_write::not_found_error;
use crate::lib::crypto;
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::pds_client::{self, PdsError};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

#[derive(Serialize, Debug)]
pub struct DeleteCommunityResponse {
    pub did: String,
}

// ---- Dependency seams ----------------------------------------------------

type LoadCredsFn = dyn Fn(
        DatabaseConnection,
        String,
    ) -> BoxFuture<'static, Result<Option<CommunityCredentials>, DbErr>>
    + Send
    + Sync;
type DeleteAccountFn =
    dyn Fn(String, String, String) -> BoxFuture<'static, Result<(), PdsError>> + Send + Sync;
/// Opens a PDS session for the BYO teardown. Args: (pds_endpoint, identifier,
/// password) → access JWT.
type CreateSessionFn =
    dyn Fn(String, String, String) -> BoxFuture<'static, Result<String, PdsError>> + Send + Sync;
/// Enumerates the (collection, rkey) of every locally-indexed record on the
/// community DID — the work-list for the BYO per-record delete.
type ListRecordsFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<(String, String)>, DbErr>>
    + Send
    + Sync;
/// Deletes one record at the PDS. Args: (pds_endpoint, access_jwt, repo,
/// collection, rkey).
type DeleteRecordFn = dyn Fn(String, String, String, String, String) -> BoxFuture<'static, Result<(), PdsError>>
    + Send
    + Sync;
type DeleteCredsFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<u64, DbErr>> + Send + Sync;
type PurgeRecordsFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<u64, DbErr>> + Send + Sync;

fn upstream_error(message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("UpstreamError"),
            message,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
async fn delete_community_with(
    community_uri: String,
    auth: String,
    admin_password: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    load_creds_fn: &LoadCredsFn,
    delete_account_fn: &DeleteAccountFn,
    create_session_fn: &CreateSessionFn,
    list_records_fn: &ListRecordsFn,
    delete_record_fn: &DeleteRecordFn,
    delete_creds_fn: &DeleteCredsFn,
    purge_records_fn: &PurgeRecordsFn,
) -> Result<Json<DeleteCommunityResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.delete",
        community_uri,
        Some(Permission::CommunityDelete),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let did = ctx.community.authority.clone();

            let creds = load_creds_fn(db.clone(), did.clone())
                .await?
                .ok_or_else(|| not_found_error("No credentials registered for this community."))?;

            if creds.source == SOURCE_APPVIEW_MANAGED {
                // Managed account: it lives on a PDS the AppView administers,
                // so tear down the whole account (and its repo) in one shot.
                delete_account_fn(creds.pds_endpoint.clone(), admin_password, did.clone())
                    .await
                    .map_err(|e| {
                        log::error!("deleteCommunity: admin deleteAccount for {did} failed: {e}");
                        upstream_error(format!("deleteAccount failed: {e}"))
                    })?;
            } else {
                // BYO: the DID is the user's own identity on a PDS we don't
                // administer, so the account must survive. Log in with the
                // stored credentials and delete the community's records one by
                // one, emptying the repo while leaving the account intact.
                let access_jwt = create_session_fn(
                    creds.pds_endpoint.clone(),
                    creds.identifier.clone(),
                    creds.password.clone(),
                )
                .await
                .map_err(|e| {
                    log::error!("deleteCommunity(byo): createSession for {did} failed: {e}");
                    upstream_error(format!("createSession failed: {e}"))
                })?;

                let records = list_records_fn(db.clone(), did.clone()).await?;
                let total = records.len();
                let mut deleted = 0u64;
                for (collection, rkey) in records {
                    match delete_record_fn(
                        creds.pds_endpoint.clone(),
                        access_jwt.clone(),
                        did.clone(),
                        collection.clone(),
                        rkey.clone(),
                    )
                    .await
                    {
                        Ok(()) => deleted += 1,
                        // Tolerate individual failures (e.g. a record already
                        // gone) so one bad rkey doesn't strand the teardown —
                        // local state is dropped below regardless.
                        Err(e) => log::warn!(
                            "deleteCommunity(byo): deleteRecord {collection}/{rkey} on {did} \
                             failed: {e}"
                        ),
                    }
                }
                log::info!(
                    "deleteCommunity(byo): deleted {deleted}/{total} record(s) from {did}'s PDS; \
                     account left intact"
                );
            }

            // Drop stored credentials and purge the local cache so no read
            // endpoint surfaces the community any more.
            delete_creds_fn(db.clone(), did.clone()).await?;
            let purged = purge_records_fn(db, did.clone()).await?;
            log::info!("deleteCommunity: purged {purged} cached record(s) for {did}");

            Ok(Json(DeleteCommunityResponse { did }))
        },
    )
    .await
}

// ---- Boxed production dependencies ---------------------------------------

fn load_creds_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<Option<CommunityCredentials>, DbErr>> {
    Box::pin(async move {
        community_credentials::load_credentials(&db, crypto::master_key(), &did)
            .await
            .map_err(|e| DbErr::Custom(format!("credentials error: {e}")))
    })
}

fn delete_account_boxed(
    pds_endpoint: String,
    admin_password: String,
    did: String,
) -> BoxFuture<'static, Result<(), PdsError>> {
    Box::pin(
        async move { pds_client::admin_delete_account(&pds_endpoint, &admin_password, &did).await },
    )
}

fn create_session_boxed(
    pds_endpoint: String,
    identifier: String,
    password: String,
) -> BoxFuture<'static, Result<String, PdsError>> {
    Box::pin(async move {
        let session = pds_client::create_session(&pds_endpoint, &identifier, &password).await?;
        Ok(session.access_jwt)
    })
}

fn list_records_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<Vec<(String, String)>, DbErr>> {
    Box::pin(async move {
        let rows = record_data::Entity::find()
            .filter(record_data::Column::Did.eq(&did))
            .all(&db)
            .await?;
        Ok(rows.into_iter().map(|r| (r.nsid, r.rkey)).collect())
    })
}

fn delete_record_boxed(
    pds_endpoint: String,
    access_jwt: String,
    repo: String,
    collection: String,
    rkey: String,
) -> BoxFuture<'static, Result<(), PdsError>> {
    Box::pin(async move {
        pds_client::delete_record(&pds_endpoint, &access_jwt, &repo, &collection, &rkey).await
    })
}

fn delete_creds_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move { community_credentials::delete_credentials(&db, &did).await })
}

fn purge_records_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move {
        let res = record_data::Entity::delete_many()
            .filter(record_data::Column::Did.eq(&did))
            .exec(&db)
            .await?;
        Ok(res.rows_affected)
    })
}

#[post("/xrpc/social.colibri.community.delete?<community>&<auth>")]
/// Deletes a community. Requires the `community.delete` permission.
pub async fn delete_community(
    community: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<DeleteCommunityResponse>, ErrorResponse> {
    let admin_password = std::env::var("PDS_ADMIN_PASS").map_err(|_| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InternalServerError"),
            message: String::from("PDS_ADMIN_PASS env var not set"),
        }),
    })?;

    let response = delete_community_with(
        community.to_string(),
        auth.to_string(),
        admin_password,
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &load_creds_boxed,
        &delete_account_boxed,
        &create_session_boxed,
        &list_records_boxed,
        &delete_record_boxed,
        &delete_creds_boxed,
        &purge_records_boxed,
    )
    .await?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::{empty_authz, mock_db, owner_authz};
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn creds(source: &str) -> CommunityCredentials {
        CommunityCredentials {
            community_did: String::from("did:plc:owner"),
            pds_endpoint: String::from("https://pds.example"),
            identifier: String::from("c.test"),
            password: String::from("pw"),
            source: source.to_string(),
        }
    }

    #[tokio::test]
    async fn deletes_managed_account_credentials_and_cache_when_authorized() {
        let db = mock_db();
        let deleted_account: Arc<Mutex<Option<(String, String, String)>>> =
            Arc::new(Mutex::new(None));
        let da = deleted_account.clone();
        let purged: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let pu = purged.clone();
        let creds_deleted: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cd = creds_deleted.clone();

        let delete_account = move |endpoint: String,
                                   pass: String,
                                   did: String|
              -> BoxFuture<'static, Result<(), PdsError>> {
            let da = da.clone();
            Box::pin(async move {
                *da.lock().unwrap() = Some((endpoint, pass, did));
                Ok(())
            })
        };
        let delete_creds =
            move |_: DatabaseConnection, did: String| -> BoxFuture<'static, Result<u64, DbErr>> {
                let cd = cd.clone();
                Box::pin(async move {
                    *cd.lock().unwrap() = Some(did);
                    Ok(1)
                })
            };
        let purge =
            move |_: DatabaseConnection, did: String| -> BoxFuture<'static, Result<u64, DbErr>> {
                let pu = pu.clone();
                Box::pin(async move {
                    *pu.lock().unwrap() = Some(did);
                    Ok(7)
                })
            };

        let result = delete_community_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            String::from("admin-pass"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, _| Box::pin(async { Ok(Some(creds(SOURCE_APPVIEW_MANAGED))) }),
            &delete_account,
            &|_, _, _| Box::pin(async { panic!("managed teardown must not open a PDS session") }),
            &|_, _| Box::pin(async { panic!("managed teardown must not enumerate records") }),
            &|_, _, _, _, _| {
                Box::pin(async { panic!("managed teardown must not delete records individually") })
            },
            &delete_creds,
            &purge,
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:owner");

        let (endpoint, pass, did) = deleted_account.lock().unwrap().take().unwrap();
        assert_eq!(endpoint, "https://pds.example");
        assert_eq!(pass, "admin-pass");
        assert_eq!(did, "did:plc:owner");
        assert_eq!(
            creds_deleted.lock().unwrap().take().unwrap(),
            "did:plc:owner"
        );
        assert_eq!(purged.lock().unwrap().take().unwrap(), "did:plc:owner");
    }

    #[tokio::test]
    async fn byo_community_deletes_records_but_keeps_account() {
        let db = mock_db();
        let deleted_records: Arc<Mutex<Vec<(String, String, String)>>> =
            Arc::new(Mutex::new(vec![]));
        let dr = deleted_records.clone();
        let creds_deleted: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let cd = creds_deleted.clone();
        let purged: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let pu = purged.clone();

        // Two records sit on the BYO repo; both must be deleted at the PDS,
        // addressed against the community DID (the repo).
        let list_records =
            move |_: DatabaseConnection,
                  _: String|
                  -> BoxFuture<'static, Result<Vec<(String, String)>, DbErr>> {
                Box::pin(async {
                    Ok(vec![
                        (
                            String::from("social.colibri.community"),
                            String::from("self"),
                        ),
                        (String::from("social.colibri.role"), String::from("r1")),
                    ])
                })
            };
        let delete_record = move |_: String,
                                  _: String,
                                  repo: String,
                                  collection: String,
                                  rkey: String|
              -> BoxFuture<'static, Result<(), PdsError>> {
            let dr = dr.clone();
            Box::pin(async move {
                dr.lock().unwrap().push((repo, collection, rkey));
                Ok(())
            })
        };
        let delete_creds =
            move |_: DatabaseConnection, _: String| -> BoxFuture<'static, Result<u64, DbErr>> {
                let cd = cd.clone();
                Box::pin(async move {
                    *cd.lock().unwrap() = true;
                    Ok(1)
                })
            };
        let purge =
            move |_: DatabaseConnection, _: String| -> BoxFuture<'static, Result<u64, DbErr>> {
                let pu = pu.clone();
                Box::pin(async move {
                    *pu.lock().unwrap() = true;
                    Ok(2)
                })
            };

        let result = delete_community_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            String::from("admin-pass"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, _| Box::pin(async { Ok(Some(creds("byo"))) }),
            &|_, _, _| Box::pin(async { panic!("BYO must not delete the PDS account") }),
            &|_, _, _| Box::pin(async { Ok(String::from("byo-access-jwt")) }),
            &list_records,
            &delete_record,
            &delete_creds,
            &purge,
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:owner");

        // Both records were deleted at the PDS, each addressed to the community
        // repo (the DID), never the account.
        let records = deleted_records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|(repo, _, _)| repo == "did:plc:owner"));
        assert!(
            records
                .iter()
                .any(|(_, c, r)| c == "social.colibri.community" && r == "self")
        );

        // Local state is still dropped so the community stops surfacing.
        assert!(*creds_deleted.lock().unwrap());
        assert!(*purged.lock().unwrap());
    }

    #[tokio::test]
    async fn byo_teardown_tolerates_individual_record_delete_failure() {
        let db = mock_db();
        let creds_deleted: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let cd = creds_deleted.clone();

        let list_records =
            move |_: DatabaseConnection,
                  _: String|
                  -> BoxFuture<'static, Result<Vec<(String, String)>, DbErr>> {
                Box::pin(async {
                    Ok(vec![
                        (String::from("social.colibri.role"), String::from("r1")),
                        (String::from("social.colibri.role"), String::from("r2")),
                    ])
                })
            };
        // The first delete fails; the teardown must press on and still drop
        // local state rather than aborting.
        let delete_record = move |_: String,
                                  _: String,
                                  _: String,
                                  _: String,
                                  rkey: String|
              -> BoxFuture<'static, Result<(), PdsError>> {
            Box::pin(async move {
                if rkey == "r1" {
                    Err(PdsError::BadStatus {
                        status: 400,
                        body: String::from("RecordNotFound"),
                    })
                } else {
                    Ok(())
                }
            })
        };
        let delete_creds =
            move |_: DatabaseConnection, _: String| -> BoxFuture<'static, Result<u64, DbErr>> {
                let cd = cd.clone();
                Box::pin(async move {
                    *cd.lock().unwrap() = true;
                    Ok(1)
                })
            };

        let result = delete_community_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            String::from("admin-pass"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, _| Box::pin(async { Ok(Some(creds("byo"))) }),
            &|_, _, _| Box::pin(async { panic!("BYO must not delete the PDS account") }),
            &|_, _, _| Box::pin(async { Ok(String::from("byo-access-jwt")) }),
            &list_records,
            &delete_record,
            &delete_creds,
            &|_, _| Box::pin(async { Ok(0) }),
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:owner");
        assert!(*creds_deleted.lock().unwrap());
    }

    #[tokio::test]
    async fn rejects_when_caller_lacks_permission() {
        let db = mock_db();
        let result = delete_community_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            String::from("admin-pass"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| Box::pin(async { Ok(empty_authz()) }),
            &|_, _| Box::pin(async { panic!("should not load creds without permission") }),
            &|_, _, _| Box::pin(async { panic!("should not delete account") }),
            &|_, _, _| Box::pin(async { panic!("should not open a PDS session") }),
            &|_, _| Box::pin(async { panic!("should not enumerate records") }),
            &|_, _, _, _, _| Box::pin(async { panic!("should not delete records") }),
            &|_, _| Box::pin(async { panic!("should not delete creds") }),
            &|_, _| Box::pin(async { panic!("should not purge") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn returns_not_found_when_no_credentials_stored() {
        let db = mock_db();
        let result = delete_community_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            String::from("admin-pass"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, _| Box::pin(async { Ok(None) }),
            &|_, _, _| Box::pin(async { panic!("should not delete account") }),
            &|_, _, _| Box::pin(async { panic!("should not open a PDS session") }),
            &|_, _| Box::pin(async { panic!("should not enumerate records") }),
            &|_, _, _, _, _| Box::pin(async { panic!("should not delete records") }),
            &|_, _| Box::pin(async { panic!("should not delete creds") }),
            &|_, _| Box::pin(async { panic!("should not purge") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }
}
