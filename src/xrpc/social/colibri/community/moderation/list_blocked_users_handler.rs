use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::moderation::currently_banned_dids;
use crate::lib::responses::{ErrorBody, ErrorResponse};

#[derive(Serialize, Debug)]
pub struct BlockedUsersResponse {
    pub dids: Vec<String>,
}

type FetchBannedFn =
    fn(DatabaseConnection, AtUri) -> BoxFuture<'static, Result<Vec<String>, DbErr>>;

async fn list_blocked_users_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_fn: FetchBannedFn,
) -> Result<Json<BlockedUsersResponse>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    let dids = fetch_fn(db, community).await?;
    Ok(Json(BlockedUsersResponse { dids }))
}

fn fetch_banned_boxed(
    db: DatabaseConnection,
    community: AtUri,
) -> BoxFuture<'static, Result<Vec<String>, DbErr>> {
    Box::pin(async move { currently_banned_dids(&db, &community).await })
}

#[get("/xrpc/social.colibri.community.listBlockedUsers?<community>")]
/// Lists every DID currently banned in the community (derived from the
/// `social.colibri.moderation` event log on the community repo).
pub async fn list_blocked_users(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BlockedUsersResponse>, ErrorResponse> {
    list_blocked_users_with(
        community.to_string(),
        db.inner().clone(),
        fetch_banned_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_banned_dids_from_helper() {
        let db = mock_db();
        let result = list_blocked_users_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            |_, community| {
                assert_eq!(community.authority, "did:plc:owner");
                Box::pin(async {
                    Ok(vec![
                        String::from("did:plc:alice"),
                        String::from("did:plc:bob"),
                    ])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(
            result.dids,
            vec![String::from("did:plc:alice"), String::from("did:plc:bob")]
        );
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = list_blocked_users_with(String::from("not-a-uri"), db, |_, _| {
            Box::pin(async { panic!("should not fetch when uri is invalid") })
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
