use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::moderation::currently_banned_dids;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::xrpc::social::colibri::actor::get_data_handler::Actor;
use crate::xrpc::social::colibri::community::invitations::hydrate_actors;

#[derive(Serialize, Debug)]
pub struct BannedUsersResponse {
    /// Banned users hydrated into full profiles, ordered by DID. Banned users
    /// no longer hold a `social.colibri.member` record, so — unlike
    /// `listMembers` — these carry no `roles`.
    pub users: Vec<Actor>,
}

type FetchBannedFn =
    fn(DatabaseConnection, AtUri) -> BoxFuture<'static, Result<Vec<String>, DbErr>>;
type HydrateFn = fn(
    DatabaseConnection,
    Vec<String>,
) -> BoxFuture<'static, Result<HashMap<String, Actor>, DbErr>>;

async fn list_banned_users_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_fn: FetchBannedFn,
    hydrate_fn: HydrateFn,
) -> Result<Json<BannedUsersResponse>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    let dids = fetch_fn(db.clone(), community).await?;
    let mut hydrated = hydrate_fn(db, dids.clone()).await?;

    // Preserve the (sorted) order from `currently_banned_dids`. `hydrate_actors`
    // returns an entry for every DID it's handed, so each banned DID resolves;
    // `filter_map` only guards against an unexpected miss.
    let users = dids
        .into_iter()
        .filter_map(|did| hydrated.remove(&did))
        .collect();

    Ok(Json(BannedUsersResponse { users }))
}

fn fetch_banned_boxed(
    db: DatabaseConnection,
    community: AtUri,
) -> BoxFuture<'static, Result<Vec<String>, DbErr>> {
    Box::pin(async move { currently_banned_dids(&db, &community.authority).await })
}

fn hydrate_boxed(
    db: DatabaseConnection,
    dids: Vec<String>,
) -> BoxFuture<'static, Result<HashMap<String, Actor>, DbErr>> {
    Box::pin(async move { hydrate_actors(&db, dids).await })
}

#[get("/xrpc/social.colibri.community.listBannedUsers?<community>")]
/// Lists every user currently banned in the community (derived from the
/// `social.colibri.moderation` event log on the community repo), each hydrated
/// into their full profile.
pub async fn list_banned_users(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BannedUsersResponse>, ErrorResponse> {
    list_banned_users_with(
        community.to_string(),
        db.inner().clone(),
        fetch_banned_boxed,
        hydrate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use crate::xrpc::social::colibri::actor::get_data_handler::{ActorData, ActorStatus};
    use rocket::tokio;

    fn actor(did: &str, handle: &str, display_name: &str) -> Actor {
        Actor {
            did: did.to_string(),
            handle: handle.to_string(),
            data: ActorData {
                display_name: display_name.to_string(),
                avatar: None,
                banner: None,
                description: None,
                online_state: String::from("offline"),
                status: ActorStatus {
                    text: String::new(),
                    emoji: None,
                },
            },
        }
    }

    #[tokio::test]
    async fn returns_hydrated_banned_users_in_order() {
        let db = mock_db();
        let result = list_banned_users_with(
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
            |_, dids| {
                Box::pin(async move {
                    assert_eq!(
                        dids,
                        vec![String::from("did:plc:alice"), String::from("did:plc:bob")]
                    );
                    Ok(HashMap::from([
                        (
                            String::from("did:plc:alice"),
                            actor("did:plc:alice", "alice.test", "Alice"),
                        ),
                        (
                            String::from("did:plc:bob"),
                            actor("did:plc:bob", "bob.test", "Bob"),
                        ),
                    ]))
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.users.len(), 2);
        assert_eq!(result.users[0].did, "did:plc:alice");
        assert_eq!(result.users[0].handle, "alice.test");
        assert_eq!(result.users[0].data.display_name, "Alice");
        assert_eq!(result.users[1].did, "did:plc:bob");
    }

    #[tokio::test]
    async fn hydrates_full_profile_fields() {
        let db = mock_db();
        let result = list_banned_users_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            |_, _| Box::pin(async { Ok(vec![String::from("did:plc:alice")]) }),
            |_, _| {
                Box::pin(async {
                    let mut a = actor("did:plc:alice", "alice.test", "Alice");
                    a.data.description = Some(String::from("bio"));
                    a.data.online_state = String::from("online");
                    a.data.status = ActorStatus {
                        text: String::from("Working"),
                        emoji: Some(String::from("🦜")),
                    };
                    Ok(HashMap::from([(String::from("did:plc:alice"), a)]))
                })
            },
        )
        .await
        .unwrap();

        let alice = &result.users[0];
        assert_eq!(alice.data.description.as_deref(), Some("bio"));
        assert_eq!(alice.data.online_state, "online");
        assert_eq!(alice.data.status.text, "Working");
        assert_eq!(alice.data.status.emoji.as_deref(), Some("🦜"));
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = list_banned_users_with(
            String::from("not-a-uri"),
            db,
            |_, _| Box::pin(async { panic!("should not fetch when uri is invalid") }),
            |_, _| Box::pin(async { panic!("should not hydrate when uri is invalid") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
