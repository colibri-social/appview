use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("user_states")
                    .add_column_if_not_exists(string("vc"))
                    .add_column_if_not_exists(string("vc_community"))
                    .add_column_if_not_exists(string("channel"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("user_states")
                    .drop_column_if_exists("vc")
                    .drop_column_if_exists("vc_community")
                    .drop_column_if_exists("channel")
                    .to_owned(),
            )
            .await
    }
}
