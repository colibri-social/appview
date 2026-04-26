use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use crate::lib::validate_state::validate_state_str;
use crate::models::user_states::{self, ActiveModel as UserStatesModel, Entity as UserStates};
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, sea_query};
use serde::Serialize;

#[derive(Serialize)]
pub struct SetStateResponse {
    #[serde(rename = "onlineState")]
    pub online_state: String,
}

pub enum UserState {
    Online,
    Away,
    Dnd,
    Offline,
}

impl UserState {
    pub fn as_str(&self) -> &'static str {
        match self {
            UserState::Online => "online",
            UserState::Away => "away",
            UserState::Dnd => "dnd",
            UserState::Offline => "offline",
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            UserState::Online => String::from("online"),
            UserState::Away => String::from("away"),
            UserState::Dnd => String::from("dnd"),
            UserState::Offline => String::from("offline"),
        }
    }

    pub fn from_string(string: String) -> UserState {
        match string.as_str() {
            "online" => UserState::Online,
            "away" => UserState::Away,
            "dnd" => UserState::Dnd,
            "offline" => UserState::Offline,
            _ => UserState::Online,
        }
    }
}

pub async fn save_state(db: &DatabaseConnection, did: String, state: String) {
    let _ = UserStates::insert(UserStatesModel {
        did: ActiveValue::Set(did),
        state: ActiveValue::Set(state),
        ..Default::default()
    })
    .on_conflict(
        sea_query::OnConflict::columns([user_states::Column::Did])
            .update_column(user_states::Column::State)
            .to_owned(),
    )
    .exec(db)
    .await;
}

#[post("/xrpc/social.colibri.actor.setState?<state>&<auth>")]
/// Returns the actor data for a specified identity.
pub async fn set_state(
    state: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<SetStateResponse>, ErrorResponse> {
    let did = service_auth::verify_service_auth(auth, "social.colibri.actor.setState")
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let maybe_state = validate_state_str(state);

    if maybe_state.is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidState"),
                message: String::from(
                    "Given state is invalid. State must be one of 'online', 'away', 'dnd', or 'offline'.",
                ),
            }),
        });
    }

    let verified_state = maybe_state.unwrap().to_string();

    save_state(db.inner(), did, verified_state.clone()).await;

    Ok(Json(SetStateResponse {
        online_state: verified_state,
    }))
}
