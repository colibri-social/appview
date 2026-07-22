use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_push_subscriptions_actor_did_endpoint")
                    .table("push_subscriptions")
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("push_subscriptions"))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("provider"))
                            .text()
                            .not_null()
                            .default("web"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("push_subscriptions"))
                    .modify_column(ColumnDef::new(Alias::new("p256dh")).text().null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("push_subscriptions"))
                    .modify_column(ColumnDef::new(Alias::new("auth")).text().null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_push_subscriptions_actor_did_provider_endpoint")
                    .table("push_subscriptions")
                    .col("actor_did")
                    .col("provider")
                    .col("endpoint")
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_push_subscriptions_actor_did_provider_endpoint")
                    .table("push_subscriptions")
                    .to_owned(),
            )
            .await?;

        // Only safe to run if no fcm/apns rows with null p256dh/auth have
        // accumulated yet — this mirrors the rest of this crate's migrations
        // in not special-casing lossy downgrades.
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("push_subscriptions"))
                    .modify_column(ColumnDef::new(Alias::new("p256dh")).text().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("push_subscriptions"))
                    .modify_column(ColumnDef::new(Alias::new("auth")).text().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("push_subscriptions"))
                    .drop_column(Alias::new("provider"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_push_subscriptions_actor_did_endpoint")
                    .table("push_subscriptions")
                    .col("actor_did")
                    .col("endpoint")
                    .unique()
                    .to_owned(),
            )
            .await
    }
}
