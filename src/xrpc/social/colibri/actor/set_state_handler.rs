use std::fmt;

use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use crate::lib::validate_state::validate_state_str;
use crate::models::user_states::{self, ActiveModel as UserStatesModel, Entity as UserStates};
use futures::future::BoxFuture;
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

impl fmt::Display for UserState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
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

async fn set_state_with<VA, SV>(
    state: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: VA,
    save_state_fn: SV,
) -> Result<Json<SetStateResponse>, ErrorResponse>
where
    VA: Fn(String, String) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>>,
    SV: Fn(DatabaseConnection, String, String) -> BoxFuture<'static, ()>,
{
    let did = verify_auth_fn(auth, String::from("social.colibri.actor.setState"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let maybe_state = validate_state_str(&state);

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

    save_state_fn(db, did, verified_state.clone()).await;

    Ok(Json(SetStateResponse {
        online_state: verified_state,
    }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn save_state_boxed(db: DatabaseConnection, did: String, state: String) -> BoxFuture<'static, ()> {
    Box::pin(async move { save_state(&db, did, state).await })
}

#[post("/xrpc/social.colibri.actor.setState?<state>&<auth>")]
/// Returns the actor data for a specified identity.
pub async fn set_state(
    state: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<SetStateResponse>, ErrorResponse> {
    set_state_with(
        state.to_string(),
        auth.to_string(),
        db.inner().clone(),
        verify_auth_boxed,
        save_state_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

    #[test]
    fn user_state_round_trip_works() {
        assert_eq!(UserState::Online.as_str(), "online");
        assert_eq!(UserState::Away.to_string(), "away");
        assert_eq!(
            UserState::from_string(String::from("dnd")).to_string(),
            "dnd"
        );
        assert_eq!(
            UserState::from_string(String::from("unknown")).to_string(),
            "online"
        );
    }

    #[tokio::test]
    async fn set_state_with_saves_valid_state() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let captured = Arc::new(Mutex::new(None::<(String, String)>));
        let captured_clone = captured.clone();

        let result = set_state_with(
            String::from("away"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:abc")) }),
            move |_, did, state| {
                let captured_clone = captured_clone.clone();
                Box::pin(async move {
                    *captured_clone.lock().unwrap() = Some((did, state));
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.online_state, "away");
        assert_eq!(
            *captured.lock().unwrap(),
            Some((String::from("did:plc:abc"), String::from("away")))
        );
    }

    #[tokio::test]
    async fn set_state_with_rejects_invalid_state() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = set_state_with(
            String::from("invalid"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:abc")) }),
            |_, _, _| Box::pin(async {}),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidState"
        );
    }
}
