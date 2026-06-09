use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::tap::TapMessageRecord;
use crate::models::user_states::{self, ActiveModel as UserStatesModel, Entity as UserStates};
use rocket::tokio::sync::broadcast::Sender;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, sea_query};
use serde_json::Value;

/// Broadcasts the user's current online state to every connected client by
/// synthesizing an `actor.data` update and pushing it onto the shared tap
/// fan-out. Each subscriber maps it through `map_tap_event`, which enriches it
/// with the state we just persisted, and forwards a `user_event` to the clients
/// that know the user — i.e. those viewing a community the user is part of. This
/// is the same path real profile/status updates travel, so the user themselves
/// and everyone who shares a community with them stay in sync after a state
/// change (connect/disconnect or an explicit `setState`).
pub async fn broadcast_state_change(
    broadcast: &Sender<TapMessageRecord>,
    did: &str,
    db: &DatabaseConnection,
) {
    let actor_data = get_atproto_record::<Value>(
        did.to_string(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db,
    )
    .await;

    let actor_data = match actor_data {
        Ok(record) => record,
        Err(e) => {
            log::error!("Unable to load actor data for {did} on state change: {e}");
            return;
        }
    };

    let record = TapMessageRecord {
        live: true,
        did: did.to_string(),
        rev: String::new(),
        collection: String::from("social.colibri.actor.data"),
        rkey: String::from("self"),
        action: String::from("update"),
        record: Some(actor_data),
        cid: None,
    };

    let _ = broadcast.send(record);
}

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
