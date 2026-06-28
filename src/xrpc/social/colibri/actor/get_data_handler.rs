use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{
    ColibriActorData, ColibriActorProfile, ColibriProfileTheme, EffectiveProfile,
    resolve_effective_profile,
};
use crate::lib::did_document::DidDocument;
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::get_state::get_state;
use crate::lib::responses::ErrorResponse;
use crate::xrpc::com::atproto::identity::resolve_identity;
use crate::xrpc::social::colibri::actor::set_state_handler::UserState;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActorStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActorData {
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "isBot")]
    pub is_bot: bool,
    #[serde(rename = "onlineState")]
    pub online_state: String,
    #[serde(rename = "syncBluesky")]
    pub sync_bluesky: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme: Option<ColibriProfileTheme>,
    pub status: ActorStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Actor {
    pub did: String,
    pub handle: String,
    pub data: ActorData,
}

/// Builds the wire-shape [`ActorData`] from a resolved [`EffectiveProfile`],
/// applying the fallbacks shared by every actor-bearing surface (the profile
/// popover via `getData`, member lists, message authors, applicants, banned
/// users, invitation creators): an absent display name falls back to `handle`.
/// `is_bot` is always derived from the Bluesky profile by the caller — the
/// effective profile never carries it. Centralised here so every surface serves
/// the same Colibri-profile / Bluesky / `syncBluesky` resolution rather than
/// reading `app.bsky.actor.profile` directly and drifting apart.
pub fn actor_data_from_effective(
    effective: EffectiveProfile,
    is_bot: bool,
    handle: &str,
    online_state: String,
    status: ActorStatus,
) -> ActorData {
    ActorData {
        display_name: effective
            .display_name
            .unwrap_or_else(|| handle.to_string()),
        avatar: effective.avatar,
        banner: effective.banner,
        description: effective.description,
        is_bot,
        online_state,
        sync_bluesky: effective.sync_bluesky,
        theme: effective.theme,
        status,
    }
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

    // The Colibri profile and Bluesky profile are both optional: un-onboarded
    // users have no Colibri profile (we fall back to Bluesky), and synced
    // profiles may omit the mirrored fields. A missing record (or a transient
    // fetch error) is treated as "absent" so the response degrades gracefully.
    let colibri_profile = get_record_fn(
        identity.id.clone(),
        String::from("social.colibri.actor.profile"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ColibriActorProfile>(v).ok());

    let bsky_profile = get_record_fn(
        identity.id.clone(),
        String::from("app.bsky.actor.profile"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ActorProfile>(v).ok());

    let colibri_actor = get_record_fn(
        identity.id.clone(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok())
    .unwrap_or_default();

    let effective = resolve_effective_profile(colibri_profile.as_ref(), bsky_profile.as_ref());
    // `is_bot` is always derived from the Bluesky self-label convention.
    let is_bot = bsky_profile.as_ref().is_some_and(ActorProfile::is_bot);

    let actor_state = get_state_fn(identity.id.clone(), db).await?;

    let data = actor_data_from_effective(
        effective,
        is_bot,
        &handle,
        actor_state.to_string(),
        ActorStatus {
            text: colibri_actor.status.unwrap_or(String::from("")),
            emoji: colibri_actor.emoji,
        },
    );

    Ok(Json(Actor {
        did: identity.id.clone(),
        handle,
        data,
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

    /// Simulates a record absent from the index (e.g. an un-onboarded user with
    /// no Colibri profile). `get_data_with` treats this as "no record".
    fn not_found() -> ErrorResponse {
        sea_orm::DbErr::RecordNotFound(String::from("not found")).into()
    }

    #[tokio::test]
    async fn unsynced_profile_uses_colibri_fields() {
        let db = mock_db();
        let result = get_data_with(
            String::from("alice.test"),
            db,
            &|_| Box::pin(async { Ok(identity_json()) }),
            &|_did, nsid, _rkey, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(serde_json::json!({
                            "displayName": "Bsky Alice",
                            "description": "bsky bio",
                            "avatar": { "ref": "bsky-blob" }
                        })),
                        "social.colibri.actor.profile" => Ok(serde_json::json!({
                            "displayName": "Colibri Alice",
                            "description": "colibri bio",
                            "syncBluesky": false,
                            "theme": { "accentColor": "#ff0000" }
                        })),
                        _ => Ok(serde_json::json!({
                            "status": "Working",
                            "emoji": "🦜",
                            "communities": []
                        })),
                    }
                })
            },
            &|_did, _| Box::pin(async { Ok(UserState::Away) }),
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:abc");
        assert_eq!(result.handle, "alice.test");
        assert_eq!(result.data.display_name, "Colibri Alice");
        assert_eq!(result.data.description.as_deref(), Some("colibri bio"));
        assert!(!result.data.sync_bluesky);
        assert_eq!(
            result.data.theme.clone().unwrap().accent_color.as_deref(),
            Some("#ff0000")
        );
        assert_eq!(result.data.online_state, "away");
        assert_eq!(result.data.status.text, "Working");
        assert!(!result.data.is_bot);
    }

    #[tokio::test]
    async fn synced_profile_uses_bsky_fields_but_colibri_theme() {
        let db = mock_db();
        let result = get_data_with(
            String::from("alice.test"),
            db,
            &|_| Box::pin(async { Ok(identity_json()) }),
            &|_did, nsid, _rkey, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(serde_json::json!({
                            "displayName": "Bsky Alice",
                            "description": "bsky bio"
                        })),
                        "social.colibri.actor.profile" => Ok(serde_json::json!({
                            "syncBluesky": true,
                            "theme": { "bannerColor": "#00ff00" }
                        })),
                        _ => Ok(serde_json::json!({ "communities": [] })),
                    }
                })
            },
            &|_did, _| Box::pin(async { Ok(UserState::Online) }),
        )
        .await
        .unwrap();

        assert_eq!(result.data.display_name, "Bsky Alice");
        assert!(result.data.sync_bluesky);
        assert_eq!(
            result.data.theme.clone().unwrap().banner_color.as_deref(),
            Some("#00ff00")
        );
    }

    #[tokio::test]
    async fn falls_back_to_bsky_when_no_colibri_profile() {
        let db = mock_db();
        let result = get_data_with(
            String::from("alice.test"),
            db,
            &|_| Box::pin(async { Ok(identity_json()) }),
            &|_did, nsid, _rkey, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(serde_json::json!({
                            "displayName": "Bsky Alice",
                            "labels": {
                                "$type": "com.atproto.label.defs#selfLabels",
                                "values": [{ "val": "bot" }]
                            }
                        })),
                        // No Colibri profile and no actor.data — un-onboarded.
                        _ => Err(not_found()),
                    }
                })
            },
            &|_did, _| Box::pin(async { Ok(UserState::Away) }),
        )
        .await
        .unwrap();

        assert_eq!(result.data.display_name, "Bsky Alice");
        assert!(!result.data.sync_bluesky);
        assert!(result.data.theme.is_none());
        assert!(result.data.is_bot);
    }
}
