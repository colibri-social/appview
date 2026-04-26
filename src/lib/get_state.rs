use sea_orm::{Condition, DatabaseConnection, DbErr};

use crate::models::user_states::{self, Entity as UserStates};
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

    return Ok(UserState::from_string(record.unwrap().state));
}
