use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("community_invitations")
                    .if_not_exists()
                    .col(string("code").primary_key())
                    .col(string("community_uri"))
                    .col(string("created_by"))
                    .col(boolean("active").default(true))
                    .col(string("created_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_community_invitations_community_uri")
                    .table("community_invitations")
                    .col("community_uri")
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table("community_invitations").to_owned())
            .await
    }
}
