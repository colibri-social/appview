use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::account_purge::{self, DeletedCounts, SoleOwnedCommunity, TapTeardown};
use crate::lib::handler::{VerifyAuthFn, verify_auth_boxed, with_authenticated};
use crate::lib::responses::{ErrorCode, ErrorResponse};

const LXM: &str = "social.colibri.actor.deleteAccount";

#[derive(Serialize, Debug)]
pub struct DeleteAccountResponse {
    pub deleted: DeletedCounts,
}

type SoleOwnedFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<SoleOwnedCommunity>, DbErr>>
    + Send
    + Sync;

type PurgeFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<DeletedCounts, DbErr>>
    + Send
    + Sync;

async fn delete_account_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    sole_owned_fn: &SoleOwnedFn,
    purge_fn: &PurgeFn,
) -> Result<Json<DeleteAccountResponse>, ErrorResponse> {
    with_authenticated(auth, LXM, db, verify_auth_fn, |caller_did, db| async move {
        let blocked = sole_owned_fn(db.clone(), caller_did.clone())
            .await
            .map_err(|e| ErrorCode::InternalError.with(e.to_string()))?;

        if !blocked.is_empty() {
            let names: Vec<&str> = blocked.iter().map(|c| c.name.as_str()).collect();
            return Err(ErrorCode::InvalidState.with(format!(
                "Transfer or delete these communities first: {}",
                names.join(", ")
            )));
        }

        let deleted = purge_fn(db, caller_did)
            .await
            .map_err(|e| ErrorCode::InternalError.with(e.to_string()))?;

        Ok(Json(DeleteAccountResponse { deleted }))
    })
    .await
}

fn sole_owned_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<Vec<SoleOwnedCommunity>, DbErr>> {
    Box::pin(async move { account_purge::sole_owned_communities(&db, &did).await })
}

fn purge_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<DeletedCounts, DbErr>> {
    Box::pin(async move { account_purge::purge_did(&db, &did, TapTeardown::Remove).await })
}

#[post("/xrpc/social.colibri.actor.deleteAccount?<auth>")]
pub async fn delete_account(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<DeleteAccountResponse>, ErrorResponse> {
    delete_account_with(
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &sole_owned_boxed,
        &purge_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::service_auth::ServiceAuthError;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn verifying(did: &'static str) -> Box<VerifyAuthFn> {
        Box::new(move |_auth, _lxm| Box::pin(async move { Ok(String::from(did)) }))
    }

    fn rejecting() -> Box<VerifyAuthFn> {
        Box::new(|_auth, _lxm| Box::pin(async { Err(ServiceAuthError::InvalidFormat) }))
    }

    fn owns_nothing() -> Box<SoleOwnedFn> {
        Box::new(|_db, _did| Box::pin(async { Ok(Vec::new()) }))
    }

    fn owns_one() -> Box<SoleOwnedFn> {
        Box::new(|_db, _did| {
            Box::pin(async {
                Ok(vec![SoleOwnedCommunity {
                    uri: String::from("at://did:plc:c/social.colibri.community/self"),
                    name: String::from("Birds"),
                    member_count: 12,
                }])
            })
        })
    }

    fn purging(ran: Arc<AtomicBool>) -> Box<PurgeFn> {
        Box::new(move |_db, _did| {
            let ran = ran.clone();
            Box::pin(async move {
                ran.store(true, Ordering::SeqCst);
                Ok(DeletedCounts {
                    record_data: 7,
                    community_records: 3,
                    ..Default::default()
                })
            })
        })
    }

    #[tokio::test]
    async fn purges_and_reports_what_it_removed() {
        let ran = Arc::new(AtomicBool::new(false));
        let result = delete_account_with(
            String::from("token"),
            mock_db(),
            &*verifying("did:plc:user"),
            &*owns_nothing(),
            &*purging(ran.clone()),
        )
        .await
        .expect("deleted");

        assert!(ran.load(Ordering::SeqCst));
        assert_eq!(result.deleted.record_data, 7);
        assert_eq!(result.deleted.community_records, 3);
    }

    #[tokio::test]
    async fn refuses_while_the_caller_is_a_sole_owner() {
        let ran = Arc::new(AtomicBool::new(false));
        let result = delete_account_with(
            String::from("token"),
            mock_db(),
            &*verifying("did:plc:user"),
            &*owns_one(),
            &*purging(ran.clone()),
        )
        .await;

        assert_eq!(result.err().map(|e| e.code), Some(ErrorCode::InvalidState));
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn refuses_an_unverifiable_token() {
        let ran = Arc::new(AtomicBool::new(false));
        let result = delete_account_with(
            String::from("token"),
            mock_db(),
            &*rejecting(),
            &*owns_nothing(),
            &*purging(ran.clone()),
        )
        .await;

        assert_eq!(result.err().map(|e| e.code), Some(ErrorCode::AuthRequired));
        assert!(!ran.load(Ordering::SeqCst));
    }
}
