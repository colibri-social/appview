use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::account_purge::{self, DeletionCounts, SoleOwnedCommunity};
use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::responses::{ErrorCode, ErrorResponse};

const LXM: &str = "social.colibri.actor.getDeletionStatus";

#[derive(Serialize, Debug)]
pub struct DeletionStatusResponse {
    #[serde(rename = "soleOwnedCommunities")]
    pub sole_owned_communities: Vec<SoleOwnedCommunity>,
    pub counts: DeletionCounts,
    #[serde(rename = "pdsAccountPage", skip_serializing_if = "Option::is_none")]
    pub pds_account_page: Option<String>,
}

pub struct DeletionStatus {
    pub sole_owned_communities: Vec<SoleOwnedCommunity>,
    pub counts: DeletionCounts,
}

type LoadStatusFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<DeletionStatus, DbErr>>
    + Send
    + Sync;

type AccountPageFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Option<String>> + Send + Sync;

async fn get_deletion_status_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_status_fn: &LoadStatusFn,
    account_page_fn: &AccountPageFn,
) -> Result<Json<DeletionStatusResponse>, ErrorResponse> {
    with_authenticated(auth, LXM, db, verify_auth_fn, |caller_did, db| async move {
        let status = load_status_fn(db.clone(), caller_did.clone())
            .await
            .map_err(|e| ErrorCode::InternalError.with(e.to_string()))?;

        Ok(Json(DeletionStatusResponse {
            sole_owned_communities: status.sole_owned_communities,
            counts: status.counts,
            pds_account_page: account_page_fn(db, caller_did).await,
        }))
    })
    .await
}

fn load_status_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<DeletionStatus, DbErr>> {
    Box::pin(async move {
        Ok(DeletionStatus {
            sole_owned_communities: account_purge::sole_owned_communities(&db, &did).await?,
            counts: account_purge::deletion_counts(&db, &did).await?,
        })
    })
}

fn account_page_boxed(db: DatabaseConnection, did: String) -> BoxFuture<'static, Option<String>> {
    Box::pin(async move { account_purge::pds_account_page(&db, &did).await })
}

#[get("/xrpc/social.colibri.actor.getDeletionStatus?<auth>")]
pub async fn get_deletion_status(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<DeletionStatusResponse>, ErrorResponse> {
    get_deletion_status_with(
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_status_boxed,
        &account_page_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn verifying(did: &'static str) -> Box<VerifyAuthFn> {
        Box::new(move |_auth, _lxm| Box::pin(async move { Ok(String::from(did)) }))
    }

    fn rejecting() -> Box<VerifyAuthFn> {
        Box::new(|_auth, _lxm| Box::pin(async { Err(ServiceAuthError::InvalidFormat) }))
    }

    fn blocked_status() -> Box<LoadStatusFn> {
        Box::new(|_db, _did| {
            Box::pin(async {
                Ok(DeletionStatus {
                    sole_owned_communities: vec![SoleOwnedCommunity {
                        uri: String::from("at://did:plc:c/social.colibri.community/self"),
                        name: String::from("Birds"),
                        member_count: 12,
                    }],
                    counts: DeletionCounts::default(),
                })
            })
        })
    }

    fn clear_status() -> Box<LoadStatusFn> {
        Box::new(|_db, _did| {
            Box::pin(async {
                Ok(DeletionStatus {
                    sole_owned_communities: Vec::new(),
                    counts: DeletionCounts {
                        records: 42,
                        notifications: 3,
                        push_subscriptions: 1,
                        invitations: 0,
                    },
                })
            })
        })
    }

    fn no_account_page() -> Box<AccountPageFn> {
        Box::new(|_db, _did| Box::pin(async { None }))
    }

    fn some_account_page() -> Box<AccountPageFn> {
        Box::new(|_db, _did| Box::pin(async { Some(String::from("https://pds.example/account")) }))
    }

    #[tokio::test]
    async fn reports_the_communities_that_block_deletion() {
        let result = get_deletion_status_with(
            String::from("token"),
            mock_db(),
            &*verifying("did:plc:user"),
            &*blocked_status(),
            &*no_account_page(),
        )
        .await
        .expect("status");

        assert_eq!(result.sole_owned_communities.len(), 1);
        assert_eq!(result.sole_owned_communities[0].name, "Birds");
        assert!(result.pds_account_page.is_none());
    }

    #[tokio::test]
    async fn reports_counts_and_the_account_page_when_there_is_one() {
        let result = get_deletion_status_with(
            String::from("token"),
            mock_db(),
            &*verifying("did:plc:user"),
            &*clear_status(),
            &*some_account_page(),
        )
        .await
        .expect("status");

        assert!(result.sole_owned_communities.is_empty());
        assert_eq!(result.counts.records, 42);
        assert_eq!(
            result.pds_account_page.as_deref(),
            Some("https://pds.example/account")
        );
    }

    #[tokio::test]
    async fn refuses_an_unverifiable_token() {
        let result = get_deletion_status_with(
            String::from("token"),
            mock_db(),
            &*rejecting(),
            &*clear_status(),
            &*no_account_page(),
        )
        .await;

        assert_eq!(result.err().map(|e| e.code), Some(ErrorCode::AuthRequired));
    }
}
