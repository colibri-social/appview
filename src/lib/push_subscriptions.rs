//! Persistence for push subscriptions (Web Push and FCM).
//!
//! Each row is one device/endpoint owned by an actor for a given `provider`
//! (`"web"` or `"fcm"`). `endpoint` holds the provider-specific unique
//! identifier — a Web Push endpoint URL for `provider = "web"`, an FCM
//! registration token for `provider = "fcm"` — so the table dedupes on
//! `(actor_did, provider, endpoint)`: re-registering refreshes the keys/owning
//! actor rather than inserting a duplicate. The AppView reads these when a
//! mention/reply/message notification is indexed (see `push_send`) to fan a
//! push out to every device the recipient has registered.

use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait, QueryFilter,
    sea_query,
};

use crate::lib::time::current_iso8601_utc;
use crate::models::push_subscriptions;

/// Hard ceiling on how many push subscriptions a single actor may register.
/// Generous enough to cover every device/browser someone reasonably owns,
/// while bounding the per-notification fan-out (and outbound-request
/// amplification) a single account can cause.
pub const MAX_SUBSCRIPTIONS_PER_ACTOR: u64 = 25;

/// Stores (or refreshes) a push subscription. Idempotent on `(actor_did,
/// provider, endpoint)`: a repeat registration updates the keys/platform and
/// leaves `created_at` untouched. Rejects a genuinely new endpoint once the
/// actor already has [`MAX_SUBSCRIPTIONS_PER_ACTOR`] registered, across all
/// providers combined.
#[allow(clippy::too_many_arguments)]
pub async fn upsert(
    db: &DatabaseConnection,
    actor_did: &str,
    provider: &str,
    endpoint: &str,
    p256dh: Option<&str>,
    auth: Option<&str>,
    platform: &str,
) -> Result<(), DbErr> {
    let already_registered = push_subscriptions::Entity::find()
        .filter(push_subscriptions::Column::ActorDid.eq(actor_did))
        .filter(push_subscriptions::Column::Provider.eq(provider))
        .filter(push_subscriptions::Column::Endpoint.eq(endpoint))
        .one(db)
        .await?
        .is_some();

    if !already_registered {
        let count = push_subscriptions::Entity::find()
            .filter(push_subscriptions::Column::ActorDid.eq(actor_did))
            .count(db)
            .await?;
        if count >= MAX_SUBSCRIPTIONS_PER_ACTOR {
            return Err(DbErr::Custom(format!(
                "actor already has the maximum of {MAX_SUBSCRIPTIONS_PER_ACTOR} push subscriptions"
            )));
        }
    }

    let active = push_subscriptions::ActiveModel {
        actor_did: ActiveValue::Set(actor_did.to_string()),
        provider: ActiveValue::Set(provider.to_string()),
        endpoint: ActiveValue::Set(endpoint.to_string()),
        p256dh: ActiveValue::Set(p256dh.map(str::to_string)),
        auth: ActiveValue::Set(auth.map(str::to_string)),
        platform: ActiveValue::Set(platform.to_string()),
        created_at: ActiveValue::Set(current_iso8601_utc()),
        ..Default::default()
    };

    push_subscriptions::Entity::insert(active)
        .on_conflict(
            sea_query::OnConflict::columns([
                push_subscriptions::Column::ActorDid,
                push_subscriptions::Column::Provider,
                push_subscriptions::Column::Endpoint,
            ])
            .update_columns([
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

/// Drops a subscription by its `(provider, endpoint)`, regardless of owner.
/// Used to prune subscriptions the push service reports as gone (e.g. HTTP
/// 404/410 for Web Push, `UNREGISTERED`/`NOT_FOUND` for FCM) — the caller
/// there isn't a user request, so there's no actor to scope to.
pub async fn delete_by_endpoint(
    db: &DatabaseConnection,
    provider: &str,
    endpoint: &str,
) -> Result<(), DbErr> {
    push_subscriptions::Entity::delete_many()
        .filter(push_subscriptions::Column::Provider.eq(provider))
        .filter(push_subscriptions::Column::Endpoint.eq(endpoint))
        .exec(db)
        .await?;
    Ok(())
}

/// Drops a subscription by its `(provider, endpoint)`, scoped to its owning
/// actor — used by `unregisterPush` so a caller can only ever remove their
/// own subscriptions, never one they merely learned the endpoint of.
pub async fn delete_by_endpoint_for_actor(
    db: &DatabaseConnection,
    actor_did: &str,
    provider: &str,
    endpoint: &str,
) -> Result<(), DbErr> {
    push_subscriptions::Entity::delete_many()
        .filter(push_subscriptions::Column::ActorDid.eq(actor_did))
        .filter(push_subscriptions::Column::Provider.eq(provider))
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
                    p256dh: Some(String::from("p1")),
                    auth: Some(String::from("a1")),
                    platform: String::from("web"),
                    provider: String::from("web"),
                    created_at: String::from("2026-06-25T00:00:00.000Z"),
                },
                push_subscriptions::Model {
                    id: 2,
                    actor_did: String::from("did:plc:me"),
                    endpoint: String::from("https://push.example/b"),
                    p256dh: Some(String::from("p2")),
                    auth: Some(String::from("a2")),
                    platform: String::from("web"),
                    provider: String::from("web"),
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
        delete_by_endpoint(&db, "web", "https://push.example/gone")
            .await
            .unwrap();
    }
}
