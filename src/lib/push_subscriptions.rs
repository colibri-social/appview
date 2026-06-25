//! Persistence for Web Push subscriptions.
//!
//! Each row is one browser/device push endpoint owned by an actor. Endpoints
//! are globally unique (the push service mints them), so the table dedupes on
//! `endpoint`: re-registering an endpoint refreshes its keys and owning actor
//! rather than inserting a duplicate. The AppView reads these when a
//! mention/reply notification is indexed (see `push_send`) to fan a Web Push
//! out to every device the recipient has registered.

use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};

use crate::lib::time::current_iso8601_utc;
use crate::models::push_subscriptions;

/// Stores (or refreshes) a push subscription. Idempotent on `endpoint`: a
/// repeat registration updates the keys, platform, and owning actor and leaves
/// `created_at` untouched.
pub async fn upsert(
    db: &DatabaseConnection,
    actor_did: &str,
    endpoint: &str,
    p256dh: &str,
    auth: &str,
    platform: &str,
) -> Result<(), DbErr> {
    let active = push_subscriptions::ActiveModel {
        actor_did: ActiveValue::Set(actor_did.to_string()),
        endpoint: ActiveValue::Set(endpoint.to_string()),
        p256dh: ActiveValue::Set(p256dh.to_string()),
        auth: ActiveValue::Set(auth.to_string()),
        platform: ActiveValue::Set(platform.to_string()),
        created_at: ActiveValue::Set(current_iso8601_utc()),
        ..Default::default()
    };

    push_subscriptions::Entity::insert(active)
        .on_conflict(
            sea_query::OnConflict::column(push_subscriptions::Column::Endpoint)
                .update_columns([
                    push_subscriptions::Column::ActorDid,
                    push_subscriptions::Column::P256dh,
                    push_subscriptions::Column::Auth,
                    push_subscriptions::Column::Platform,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

/// Drops a subscription by its endpoint. Idempotent — removing an endpoint that
/// isn't stored is a no-op. Used both by `unregisterPush` and to prune
/// subscriptions the push service reports as gone (HTTP 404/410).
pub async fn delete_by_endpoint(db: &DatabaseConnection, endpoint: &str) -> Result<(), DbErr> {
    push_subscriptions::Entity::delete_many()
        .filter(push_subscriptions::Column::Endpoint.eq(endpoint))
        .exec(db)
        .await?;
    Ok(())
}

/// Returns every push subscription owned by `actor_did`.
pub async fn list_for_actor(
    db: &DatabaseConnection,
    actor_did: &str,
) -> Result<Vec<push_subscriptions::Model>, DbErr> {
    push_subscriptions::Entity::find()
        .filter(push_subscriptions::Column::ActorDid.eq(actor_did))
        .all(db)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

    #[tokio::test]
    async fn list_for_actor_collects_rows() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                push_subscriptions::Model {
                    id: 1,
                    actor_did: String::from("did:plc:me"),
                    endpoint: String::from("https://push.example/a"),
                    p256dh: String::from("p1"),
                    auth: String::from("a1"),
                    platform: String::from("web"),
                    created_at: String::from("2026-06-25T00:00:00.000Z"),
                },
                push_subscriptions::Model {
                    id: 2,
                    actor_did: String::from("did:plc:me"),
                    endpoint: String::from("https://push.example/b"),
                    p256dh: String::from("p2"),
                    auth: String::from("a2"),
                    platform: String::from("web"),
                    created_at: String::from("2026-06-25T00:00:01.000Z"),
                },
            ]])
            .into_connection();

        let rows = list_for_actor(&db, "did:plc:me").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].endpoint, "https://push.example/a");
    }

    #[tokio::test]
    async fn delete_by_endpoint_is_a_no_op_without_panicking() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 0,
                rows_affected: 0,
            }])
            .into_connection();
        delete_by_endpoint(&db, "https://push.example/gone")
            .await
            .unwrap();
    }
}
