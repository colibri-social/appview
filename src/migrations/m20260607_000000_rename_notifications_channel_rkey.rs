use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("notifications"))
                    .rename_column(Alias::new("channel_rkey"), Alias::new("channel_uri"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("notifications"))
                    .rename_column(Alias::new("channel_uri"), Alias::new("channel_rkey"))
                    .to_owned(),
            )
            .await
    }
}
