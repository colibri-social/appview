use crate::lib::author_cache::AuthorCache;
use crate::lib::event_scope::{CommunityResolver, ScopedEvent, SharedScopedEvent};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::hum_client::{self, OutboundHum};
use crate::lib::map_tap_event::map_tap_event;
use crate::lib::presence;
use crate::lib::tap::TapMessageRecord;
use crate::models::user_states::{
    self, ActiveModel as UserStatesActiveModel, Entity as UserStates,
};
use crate::xrpc::social::colibri::actor::set_state_handler::UserState;
use rocket::tokio::sync::broadcast::Sender;
use rocket::tokio::sync::mpsc;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter,
    sea_query::{self, Expr},
};
use serde_json::Value;
use std::sync::Arc;

/// Broadcasts the user's current online state to every connected client by
/// synthesizing an `actor.data` update, mapping it once through
/// `map_tap_event` (which enriches it with the state we just persisted), and
/// pushing the resulting `user_event` onto the shared fan-out. `user_event`s
/// are `Global`-scoped, so the user themselves and everyone else stay in sync
/// after a state change (connect/disconnect or an explicit `setState`). This is
/// the same mapping path real profile/status updates travel.
pub async fn broadcast_state_change(
    broadcast: &Sender<SharedScopedEvent>,
    did: &str,
    db: &DatabaseConnection,
    hum_outbox: &mpsc::Sender<OutboundHum>,
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

    // The actor.data arm yields a single `Global` `user_event` and consults
    // neither the resolver nor the author cache, so throwaway empty ones are
    // fine here.
    let resolver = CommunityResolver::new();
    let author_cache = AuthorCache::new();
    match map_tap_event(&record, db, &resolver, &author_cache).await {
        Ok(events) => {
            for (event, scope) in events {
                let scoped = Arc::new(ScopedEvent {
                    scope,
                    payload: event.serialize(),
                });
                let _ = broadcast.send(scoped);
            }
        }
        Err(e) => {
            log::error!("Unable to map state change for {did}: {e}");
        }
    }

    // Propagate the presence change to the user's communities hosted on other
    // AppViews (no-op unless Humming is enabled).
    hum_client::enqueue(
        hum_outbox,
        OutboundHum::Presence {
            did: did.to_string(),
        },
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveState {
    pub state: String,
    pub changed: bool,
}

pub async fn refresh_effective_state(db: &DatabaseConnection, did: &str) -> Option<EffectiveState> {
    let existing = match UserStates::find_by_id(did).one(db).await {
        Ok(record) => record,
        Err(e) => {
            log::error!("Unable to load user state for {did}: {e}");
            return None;
        }
    };

    let effective = if presence::connection_count(did) == 0 {
        UserState::Offline
    } else {
        existing
            .as_ref()
            .and_then(|record| record.manual_state.clone())
            .map_or(UserState::Online, UserState::from_string)
    };
    let effective = effective.as_str().to_string();

    let unchanged = match &existing {
        Some(record) => record.state == effective,
        None => effective == UserState::Offline.as_str(),
    };

    if unchanged {
        return Some(EffectiveState {
            state: effective,
            changed: false,
        });
    }

    let _ = UserStates::insert(UserStatesActiveModel {
        did: ActiveValue::Set(did.to_string()),
        state: ActiveValue::Set(effective.clone()),
        ..Default::default()
    })
    .on_conflict(
        sea_query::OnConflict::columns([user_states::Column::Did])
            .update_column(user_states::Column::State)
            .to_owned(),
    )
    .exec(db)
    .await;

    Some(EffectiveState {
        state: effective,
        changed: true,
    })
}

pub async fn join_vc(did: String, vc: String, community: String, db: &DatabaseConnection) {
    let _ = UserStates::update_many()
        .col_expr(user_states::Column::Vc, Expr::value(vc))
        .col_expr(user_states::Column::VcCommunity, Expr::value(community))
        .filter(user_states::Column::Did.eq(did))
        .exec(db)
        .await;
}

pub async fn leave_vc(did: String, db: &DatabaseConnection) {
    let _ = UserStates::update_many()
        .col_expr(user_states::Column::Vc, Expr::value(Option::<String>::None))
        .col_expr(
            user_states::Column::VcCommunity,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            user_states::Column::VcMuted,
            Expr::value(Option::<bool>::None),
        )
        .col_expr(
            user_states::Column::VcDeafened,
            Expr::value(Option::<bool>::None),
        )
        .filter(user_states::Column::Did.eq(did))
        .exec(db)
        .await;
}

pub async fn reset_all_presence(db: &DatabaseConnection) {
    let _ = UserStates::update_many()
        .col_expr(user_states::Column::State, Expr::value("offline"))
        .col_expr(user_states::Column::Vc, Expr::value(Option::<String>::None))
        .col_expr(
            user_states::Column::VcCommunity,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            user_states::Column::VcMuted,
            Expr::value(Option::<bool>::None),
        )
        .col_expr(
            user_states::Column::VcDeafened,
            Expr::value(Option::<bool>::None),
        )
        .exec(db)
        .await;
}

pub async fn set_vc_state(did: String, muted: bool, deafened: bool, db: &DatabaseConnection) {
    let _ = UserStates::update_many()
        .col_expr(user_states::Column::VcMuted, Expr::value(muted))
        .col_expr(user_states::Column::VcDeafened, Expr::value(deafened))
        .filter(user_states::Column::Did.eq(did))
        .exec(db)
        .await;
}

pub async fn view_channel(did: String, channel: String, db: &DatabaseConnection) {
    let _ = UserStates::update_many()
        .col_expr(user_states::Column::Channel, Expr::value(channel))
        .filter(user_states::Column::Did.eq(did))
        .exec(db)
        .await;
}

/// Clears the channel `view_channel` last recorded, so a disconnected user
/// doesn't keep suppressing notifications for whatever they had open (see
/// `notifications::currently_viewing_dids`) after they've actually left.
/// Called on the last WS disconnect.
pub async fn clear_viewed_channel(did: String, db: &DatabaseConnection) {
    let _ = UserStates::update_many()
        .col_expr(
            user_states::Column::Channel,
            Expr::value(Option::<String>::None),
        )
        .filter(user_states::Column::Did.eq(did))
        .exec(db)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

    fn row(did: &str, state: &str, manual_state: Option<&str>) -> user_states::Model {
        user_states::Model {
            did: String::from(did),
            state: String::from(state),
            manual_state: manual_state.map(String::from),
            vc: None,
            vc_community: None,
            channel: None,
            vc_muted: None,
            vc_deafened: None,
        }
    }

    fn db_with(row: user_states::Model) -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![row]])
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection()
    }

    #[tokio::test]
    async fn a_chosen_state_survives_other_devices_coming_and_going() {
        let did = "did:plc:state-two-devices";

        presence::add_connection(did);
        assert_eq!(
            refresh_effective_state(&db_with(row(did, "offline", None)), did)
                .await
                .unwrap(),
            EffectiveState {
                state: String::from("online"),
                changed: true,
            }
        );

        assert_eq!(
            refresh_effective_state(&db_with(row(did, "online", Some("dnd"))), did)
                .await
                .unwrap(),
            EffectiveState {
                state: String::from("dnd"),
                changed: true,
            }
        );

        presence::add_connection(did);
        assert_eq!(
            refresh_effective_state(&db_with(row(did, "dnd", Some("dnd"))), did)
                .await
                .unwrap(),
            EffectiveState {
                state: String::from("dnd"),
                changed: false,
            }
        );

        assert_eq!(presence::remove_connection(did), 1);
        assert_eq!(presence::remove_connection(did), 0);

        assert_eq!(
            refresh_effective_state(&db_with(row(did, "dnd", Some("dnd"))), did)
                .await
                .unwrap(),
            EffectiveState {
                state: String::from("offline"),
                changed: true,
            }
        );

        presence::add_connection(did);
        assert_eq!(
            refresh_effective_state(&db_with(row(did, "offline", Some("dnd"))), did)
                .await
                .unwrap(),
            EffectiveState {
                state: String::from("dnd"),
                changed: true,
            }
        );
    }

    #[tokio::test]
    async fn goes_offline_when_no_device_is_connected() {
        let did = "did:plc:state-none-connected";
        let db = db_with(row(did, "dnd", Some("dnd")));

        assert_eq!(
            refresh_effective_state(&db, did).await,
            Some(EffectiveState {
                state: String::from("offline"),
                changed: true,
            })
        );
    }

    #[tokio::test]
    async fn defaults_to_online_when_no_state_was_chosen() {
        let did = "did:plc:state-no-choice";
        presence::add_connection(did);

        let db = db_with(row(did, "offline", None));

        assert_eq!(
            refresh_effective_state(&db, did).await,
            Some(EffectiveState {
                state: String::from("online"),
                changed: true,
            })
        );
    }

    #[tokio::test]
    async fn keeps_the_chosen_state_while_a_device_is_connected() {
        let did = "did:plc:state-chosen";
        presence::add_connection(did);

        let db = db_with(row(did, "offline", Some("dnd")));

        assert_eq!(
            refresh_effective_state(&db, did).await,
            Some(EffectiveState {
                state: String::from("dnd"),
                changed: true,
            })
        );
    }

    #[tokio::test]
    async fn reports_no_change_when_a_second_device_connects() {
        let did = "did:plc:state-second-device";
        presence::add_connection(did);
        presence::add_connection(did);

        let db = db_with(row(did, "dnd", Some("dnd")));

        assert_eq!(
            refresh_effective_state(&db, did).await,
            Some(EffectiveState {
                state: String::from("dnd"),
                changed: false,
            })
        );
    }

    #[tokio::test]
    async fn stays_offline_when_one_of_several_devices_leaves() {
        let did = "did:plc:state-one-leaves";
        presence::add_connection(did);
        presence::add_connection(did);
        assert_eq!(presence::remove_connection(did), 1);

        let db = db_with(row(did, "away", Some("away")));

        assert_eq!(
            refresh_effective_state(&db, did).await,
            Some(EffectiveState {
                state: String::from("away"),
                changed: false,
            })
        );
    }

    #[tokio::test]
    async fn writes_nothing_for_a_missing_row_with_no_connections() {
        let did = "did:plc:state-missing-row";
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<user_states::Model>::new()])
            .into_connection();

        assert_eq!(
            refresh_effective_state(&db, did).await,
            Some(EffectiveState {
                state: String::from("offline"),
                changed: false,
            })
        );
    }

    #[tokio::test]
    async fn returns_none_when_the_read_fails() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();

        assert_eq!(
            refresh_effective_state(&db, "did:plc:state-read-error").await,
            None
        );
    }

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

    #[tokio::test]
    async fn clear_viewed_channel_executes_without_failing() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection();

        clear_viewed_channel(String::from("did:plc:abc"), &db).await;
    }
}
