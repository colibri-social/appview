use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::handler::{
    VerifyAuthFn, invalid_community_uri, verify_auth_boxed, with_authenticated,
};
use crate::lib::moderation::{RevokeMemberFn, revoke_member_boxed};
use crate::lib::responses::ErrorResponse;

#[derive(Serialize, Debug)]
pub struct LeaveResponse {}

async fn leave_with(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    revoke_member_fn: &RevokeMemberFn,
) -> Result<Json<LeaveResponse>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(invalid_community_uri)?;

    with_authenticated(
        auth,
        "social.colibri.community.leave",
        db,
        verify_auth_fn,
        move |caller_did, db| async move {
            // Idempotent: `revoke_community_member` returns Ok(false) when no
            // member record exists (never joined, already left, or a legacy
            // community with no AppView credentials), which we treat as success.
            revoke_member_fn(db, community.authority.clone(), caller_did).await?;
            Ok(Json(LeaveResponse {}))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.community.leave?<community>&<auth>")]
/// Removes the authenticated caller from a community by revoking their
/// community-side `social.colibri.member` record.
pub async fn leave(
    community: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<LeaveResponse>, ErrorResponse> {
    leave_with(
        community.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &revoke_member_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use futures::future::BoxFuture;
    use rocket::tokio;
    use sea_orm::DbErr;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn leave_revokes_callers_member_record() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let revoke_fn = move |_: DatabaseConnection,
                              community_did: String,
                              subject_did: String|
              -> BoxFuture<'static, Result<bool, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((community_did, subject_did));
                Ok(true)
            })
        };

        let result = leave_with(
            String::from("at://did:plc:community/social.colibri.community/self"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &revoke_fn,
        )
        .await;

        assert!(result.is_ok());
        let (community_did, subject_did) = captured.lock().unwrap().take().unwrap();
        assert_eq!(community_did, "did:plc:community");
        assert_eq!(subject_did, "did:plc:me");
    }

    #[tokio::test]
    async fn leave_succeeds_when_no_member_record_exists() {
        let db = mock_db();
        let result = leave_with(
            String::from("at://did:plc:community/social.colibri.community/self"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            &|_, _, _| Box::pin(async { Ok(false) }),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn leave_rejects_invalid_community_uri() {
        let db = mock_db();
        let result = leave_with(
            String::from("not-a-uri"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate on invalid uri") }),
            &|_, _, _| Box::pin(async { panic!("should not revoke on invalid uri") }),
        )
        .await;

        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
