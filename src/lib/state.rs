use crate::lib::author_cache::AuthorCache;
use crate::lib::event_scope::{CommunityResolver, ScopedEvent, SharedScopedEvent};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::hum_client::{self, OutboundHum};
use crate::lib::map_tap_event::map_tap_event;
use crate::lib::tap::TapMessageRecord;
use crate::models::user_states::{self, Entity as UserStates};
use rocket::tokio::sync::broadcast::Sender;
use rocket::tokio::sync::mpsc;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr};
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
/// Called on WS disconnect.
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
