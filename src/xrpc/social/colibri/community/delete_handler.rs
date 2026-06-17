//! `social.colibri.community.delete` — tears down a community.
//!
//! Requires the caller to hold the `community.delete` permission. For
//! AppView-managed communities (minted via `community.create`) the entire PDS
//! account is deleted via `com.atproto.admin.deleteAccount`, which removes the
//! repo and every record on it. For BYO communities the AppView does not
//! administer the hosting PDS, so the account is left untouched; the AppView
//! only stops tracking it.
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
                .ok_or_else(|| {
                    not_found_error("No credentials registered for this community.")
                })?;

            // Only AppView-managed accounts can be torn down at the PDS — those
            // live on a PDS the AppView administers. BYO accounts are left
            // alone; we just stop tracking them.
            if creds.source == SOURCE_APPVIEW_MANAGED {
                delete_account_fn(creds.pds_endpoint.clone(), admin_password, did.clone())
                    .await
                    .map_err(|e| {
                        log::error!("deleteCommunity: admin deleteAccount for {did} failed: {e}");
                        upstream_error(format!("deleteAccount failed: {e}"))
                    })?;
            } else {
                log::warn!(
                    "deleteCommunity: {did} is BYO ({}); leaving PDS account intact, untracking only",
                    creds.source
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
    async fn byo_community_skips_account_deletion_but_still_untracks() {
        let db = mock_db();
        let creds_deleted: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let cd = creds_deleted.clone();
        let purged: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let pu = purged.clone();

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
                    Ok(0)
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
            &delete_creds,
            &purge,
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:owner");
        assert!(*creds_deleted.lock().unwrap());
        assert!(*purged.lock().unwrap());
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
            &|_, _| Box::pin(async { panic!("should not delete creds") }),
            &|_, _| Box::pin(async { panic!("should not purge") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }
}
