use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("push_subscriptions")
                    .if_not_exists()
                    .col(big_integer("id").primary_key().auto_increment())
                    .col(string("actor_did"))
                    .col(string("endpoint"))
                    .col(string("p256dh"))
                    .col(string("auth"))
                    .col(string("platform"))
                    .col(string("created_at"))
                    .to_owned(),
            )
            .await?;

        // An endpoint uniquely identifies a push subscription, so dedupe on it:
        // re-registering the same endpoint updates the owning actor + keys
        // rather than creating a duplicate row.
        manager
            .create_index(
                Index::create()
                    .name("idx_push_subscriptions_endpoint")
                    .table("push_subscriptions")
                    .col("endpoint")
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Sending pushes for a notification looks up every subscription owned
        // by the recipient, so index the actor.
        manager
            .create_index(
                Index::create()
                    .name("idx_push_subscriptions_actor_did")
                    .table("push_subscriptions")
                    .col("actor_did")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table("push_subscriptions").to_owned())
            .await
    }
}
