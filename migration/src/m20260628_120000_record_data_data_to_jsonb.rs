use sea_orm::ConnectionTrait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // `record_data.data` was a `String`/TEXT column until the model switched
        // to `JsonBinary`/`Value` (commit daafc54). Databases created before
        // that still hold TEXT: `init_db` builds the table with
        // `create_table_from_entity(...).if_not_exists()`, which never alters an
        // existing table, so the column type never updated and rows fail to
        // decode as JSONB (`mismatched types ... JSONB ... is not compatible
        // with SQL type TEXT`). Convert in place — existing values are
        // serialized JSON, so the `::jsonb` cast parses cleanly. On a fresh DB
        // the column is already `jsonb`, making this a no-op.
        manager
            .get_connection()
            .execute_unprepared(
                r#"ALTER TABLE "record_data" ALTER COLUMN "data" TYPE jsonb USING "data"::jsonb"#,
            )
            .await
            .map(|_| ())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"ALTER TABLE "record_data" ALTER COLUMN "data" TYPE text USING "data"::text"#,
            )
            .await
            .map(|_| ())
    }
}
