use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::ColibriActorData;
use crate::lib::did_document::DidDocument;
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::get_state::get_state;
use crate::lib::responses::ErrorResponse;
use crate::xrpc::com::atproto::identity::resolve_identity;
use crate::xrpc::social::colibri::actor::set_state_handler::UserState;

#[derive(Serialize, Deserialize, Debug)]
pub struct ActorStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ActorData {
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "onlineState")]
    pub online_state: String,
    pub status: ActorStatus,
}

#[derive(Serialize, Deserialize)]
pub struct Actor {
    pub did: String,
    pub handle: String,
    pub data: ActorData,
}

type ResolveIdentityFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Json<DidDocument>, ErrorResponse>> + Send + Sync;
type GetRecordFn = dyn Fn(
        String,
        String,
        String,
        DatabaseConnection,
    ) -> BoxFuture<'static, Result<Value, ErrorResponse>>
    + Send
    + Sync;
type GetStateFn = dyn Fn(String, DatabaseConnection) -> BoxFuture<'static, Result<UserState, ErrorResponse>>
    + Send
    + Sync;

async fn get_data_with(
    identifier: String,
    db: DatabaseConnection,
    resolve_identity_fn: &ResolveIdentityFn,
    get_record_fn: &GetRecordFn,
    get_state_fn: &GetStateFn,
) -> Result<Json<Actor>, ErrorResponse> {
    let identity = resolve_identity_fn(identifier).await?;
    let handle = identity
        .also_known_as
        .as_ref()
        .unwrap()
        .first()
        .unwrap()
        .to_owned();

    let bsky_profile_value = get_record_fn(
        identity.id.clone(),
        String::from("app.bsky.actor.profile"),
        String::from("self"),
        db.clone(),
    )
    .await?;
    let bsky_profile = serde_json::from_value::<ActorProfile>(bsky_profile_value)
        .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;

    let colibri_actor_value = get_record_fn(
        identity.id.clone(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db.clone(),
    )
    .await?;
    let colibri_actor = serde_json::from_value::<ColibriActorData>(colibri_actor_value)
        .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;

    let actor_state = get_state_fn(identity.id.clone(), db).await?;

    Ok(Json(Actor {
        did: identity.id.clone(),
        handle: handle.clone(),
        data: ActorData {
            avatar: bsky_profile.avatar,
            banner: bsky_profile.banner,
            description: bsky_profile.description,
            display_name: bsky_profile.display_name.unwrap_or(handle),
            online_state: actor_state.to_string(),
            status: ActorStatus {
                text: colibri_actor.status,
                emoji: colibri_actor.emoji,
            },
        },
    }))
}

fn resolve_identity_boxed(
    identifier: String,
) -> BoxFuture<'static, Result<Json<DidDocument>, ErrorResponse>> {
    Box::pin(async move { resolve_identity(&identifier).await })
}

fn get_record_boxed(
    did: String,
    nsid: String,
    rkey: String,
    db: DatabaseConnection,
) -> BoxFuture<'static, Result<Value, ErrorResponse>> {
    Box::pin(async move {
        get_atproto_record::<Value>(did, nsid, rkey, &db)
            .await
            .map_err(Into::into)
    })
}

fn get_state_boxed(
    did: String,
    db: DatabaseConnection,
) -> BoxFuture<'static, Result<UserState, ErrorResponse>> {
    Box::pin(async move { get_state(did, &db).await.map_err(Into::into) })
}

#[get("/xrpc/social.colibri.actor.getData?<identifier>")]
/// Returns the actor data for a specified identity.
pub async fn get_data(
    identifier: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<Actor>, ErrorResponse> {
    get_data_with(
        identifier.to_string(),
        db.inner().clone(),
        &resolve_identity_boxed,
        &get_record_boxed,
        &get_state_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn identity_json() -> Json<DidDocument> {
        Json(DidDocument {
            context: vec![String::from("https://www.w3.org/ns/did/v1")],
            id: String::from("did:plc:abc"),
            also_known_as: Some(vec![String::from("alice.test")]),
            verification_method: vec![],
            service: vec![],
        })
    }

    #[tokio::test]
    async fn builds_actor_data_from_resolved_identity_and_records() {
        let db = mock_db();
        let result = get_data_with(
            String::from("alice.test"),
            db,
            &|_| Box::pin(async { Ok(identity_json()) }),
            &|_did, nsid, _rkey, _| {
                Box::pin(async move {
                    if nsid == "app.bsky.actor.profile" {
                        Ok(serde_json::json!({
                            "displayName": "Alice",
                            "description": "Hello",
                            "avatar": { "ref": "blob1" }
                        }))
                    } else {
                        Ok(serde_json::json!({
                            "status": "Working",
                            "emoji": "🦜",
                            "communities": []
                        }))
                    }
                })
            },
            &|_did, _| Box::pin(async { Ok(UserState::Away) }),
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:abc");
        assert_eq!(result.handle, "alice.test");
        assert_eq!(result.data.display_name, "Alice");
        assert_eq!(result.data.online_state, "away");
        assert_eq!(result.data.status.text, "Working");
    }
}
