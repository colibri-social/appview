use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::ColibriActorData;
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::responses::ErrorResponse;
use crate::xrpc::com::atproto::identity::resolve_identity;

#[derive(Serialize, Deserialize)]
pub struct ActorStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize)]
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

#[get("/xrpc/social.colibri.actor.getData?<identifier>")]
/// Returns the actor data for a specified identity.
pub async fn get_data(
    identifier: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<Actor>, ErrorResponse> {
    let identity = resolve_identity(identifier).await?;
    let handle = identity
        .also_known_as
        .as_ref()
        .unwrap()
        .first()
        .unwrap()
        .to_owned();

    let bsky_profile = get_atproto_record::<ActorProfile>(
        identity.id.clone(),
        String::from("app.bsky.actor.profile"),
        String::from("self"),
        db.inner(),
    )
    .await?;
    let colibri_actor = get_atproto_record::<ColibriActorData>(
        identity.id.clone(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db.inner(),
    )
    .await?;

    Ok(Json(Actor {
        did: identity.id.clone(),
        handle: handle.clone(),
        data: ActorData {
            avatar: bsky_profile.avatar,
            banner: bsky_profile.banner,
            description: bsky_profile.description,
            display_name: bsky_profile.display_name.unwrap_or(handle),
            online_state: String::from("online"), // TODO: This needs to be made configurable
            status: ActorStatus {
                text: colibri_actor.status,
                emoji: colibri_actor.emoji,
            },
        },
    }))
}
