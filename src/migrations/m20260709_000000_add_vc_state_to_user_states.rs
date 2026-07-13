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
                    .add_column_if_not_exists(boolean_null("vc_muted"))
                    .add_column_if_not_exists(boolean_null("vc_deafened"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table("user_states")
                    .drop_column_if_exists("vc_muted")
                    .drop_column_if_exists("vc_deafened")
                    .to_owned(),
            )
            .await
    }
}
