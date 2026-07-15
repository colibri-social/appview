use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_push_subscriptions_endpoint")
                    .table("push_subscriptions")
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

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_push_subscriptions_actor_did_endpoint")
                    .table("push_subscriptions")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_push_subscriptions_endpoint")
                    .table("push_subscriptions")
                    .col("endpoint")
                    .unique()
                    .to_owned(),
            )
            .await
    }
}
