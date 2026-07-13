use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("dismissed_applications")
                    .if_not_exists()
                    .col(big_integer("id").primary_key().auto_increment())
                    .col(string("community_did"))
                    .col(string("applicant_did"))
                    .col(string("dismissed_at"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_dismissed_applications_community_applicant")
                    .table("dismissed_applications")
                    .col("community_did")
                    .col("applicant_did")
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table("dismissed_applications").to_owned())
            .await
    }
}
