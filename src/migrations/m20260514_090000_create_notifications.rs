use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("notifications")
                    .if_not_exists()
                    .col(big_integer("id").primary_key().auto_increment())
                    .col(string("recipient_did"))
                    .col(string("kind"))
                    .col(string("message_uri"))
                    .col(string("author_did"))
                    .col(string("channel_rkey"))
                    .col(string("indexed_at"))
                    .col(string_null("seen_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_notifications_recipient_did_id")
                    .table("notifications")
                    .col("recipient_did")
                    .col("id")
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_notifications_dedupe")
                    .table("notifications")
                    .col("recipient_did")
                    .col("message_uri")
                    .col("kind")
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table("notifications").to_owned())
            .await
    }
}
