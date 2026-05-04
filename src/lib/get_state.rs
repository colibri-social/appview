use sea_orm::{Condition, DatabaseConnection, DbErr};

use crate::models::user_states::{self, Entity as UserStates, Model as UserStatesModel};
use crate::xrpc::social::colibri::actor::set_state_handler::UserState;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

pub async fn get_state(did: String, db: &DatabaseConnection) -> Result<UserState, DbErr> {
    let record = UserStates::find()
        .filter(Condition::all().add(user_states::Column::Did.eq(&did)))
        .one(db)
        .await?;

    if record.is_none() {
        return Ok(UserState::Offline);
    }

    Ok(UserState::from_string(record.unwrap().state))
}

pub async fn get_did_states(
    did: String,
    db: &DatabaseConnection,
) -> Result<UserStatesModel, DbErr> {
    let record = UserStates::find()
        .filter(Condition::all().add(user_states::Column::Did.eq(&did)))
        .one(db)
        .await?;

    if record.is_none() {
        return Err(DbErr::RecordNotFound(String::from(
            "Unable to find record.",
        )));
    }

    Ok(record.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::user_states;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn returns_offline_when_state_is_missing() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<user_states::Model>::new()])
            .into_connection();

        let state = get_state(String::from("did:plc:none"), &db).await.unwrap();
        assert_eq!(state.to_string(), "offline");
    }

    #[tokio::test]
    async fn returns_saved_state_when_record_exists() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![user_states::Model {
                did: String::from("did:plc:abc"),
                state: String::from("dnd"),
                channel: String::from(""),
                vc: None,
                vc_community: None,
            }]])
            .into_connection();

        let state = get_state(String::from("did:plc:abc"), &db).await.unwrap();
        assert_eq!(state.to_string(), "dnd");
    }
}
