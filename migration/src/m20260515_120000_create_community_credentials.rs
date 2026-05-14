use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table("community_credentials")
                    .if_not_exists()
                    .col(string("community_did").primary_key())
                    .col(string("pds_endpoint"))
                    .col(string("identifier"))
                    .col(string("password_ciphertext_b64"))
                    .col(string("password_nonce_b64"))
                    .col(string("source"))
                    .col(string("created_at"))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table("community_credentials").to_owned())
            .await
    }
}
