use crate::models::user_states::{self, ActiveModel as UserStatesModel, Entity as UserStates};
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, sea_query};

pub async fn join_vc(did: String, vc: String, community: String, db: &DatabaseConnection) {
    let _ = UserStates::insert(UserStatesModel {
        did: ActiveValue::Set(did),
        vc: ActiveValue::Set(Some(vc)),
        vc_community: ActiveValue::Set(Some(community)),
        ..Default::default()
    })
    .on_conflict(
        sea_query::OnConflict::columns([user_states::Column::Did])
            .update_column(user_states::Column::Vc)
            .to_owned(),
    )
    .exec(db)
    .await;
}

pub async fn leave_vc(did: String, db: &DatabaseConnection) {
    let _ = UserStates::insert(UserStatesModel {
        did: ActiveValue::Set(did),
        vc: ActiveValue::Set(None),
        ..Default::default()
    })
    .on_conflict(
        sea_query::OnConflict::columns([user_states::Column::Did])
            .update_column(user_states::Column::Vc)
            .to_owned(),
    )
    .exec(db)
    .await;
}

pub async fn view_channel(did: String, channel: String, db: &DatabaseConnection) {
    let _ = UserStates::insert(UserStatesModel {
        did: ActiveValue::Set(did),
        channel: ActiveValue::Set(Some(channel)),
        ..Default::default()
    })
    .on_conflict(
        sea_query::OnConflict::columns([user_states::Column::Did])
            .update_column(user_states::Column::Channel)
            .to_owned(),
    )
    .exec(db)
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

    #[tokio::test]
    async fn join_vc_executes_without_failing() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection();

        join_vc(
            String::from("did:plc:abc"),
            String::from("voice-1"),
            String::from("community-1"),
            &db,
        )
        .await;
    }

    #[tokio::test]
    async fn leave_vc_executes_without_failing() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection();

        leave_vc(String::from("did:plc:abc"), &db).await;
    }

    #[tokio::test]
    async fn view_channel_executes_without_failing() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection();

        view_channel(
            String::from("did:plc:abc"),
            String::from("community-1/channel-1"),
            &db,
        )
        .await;
    }
}
