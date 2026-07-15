use sea_orm::ConnectionTrait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// `record_data` is a single polymorphic table holding every record kind
/// across every community/DID. Its only index is the composite unique key
/// `(did, nsid, rkey)`. Several hot-path reads have no `did` to filter by at
/// all (reactions are authored by many different reactors' repos; messages
/// by many different authors' repos) and instead filter by `nsid` plus a
/// JSONB field extraction (`data->>'subject'/'channel'/'community'/'parent'`)
/// — a query shape the existing index can't serve, so it falls back to a
/// full-table scan that gets slower as the table grows. These four
/// expression indexes cover the field names actually queried this way
/// (`reactions.rs`, `notifications.rs`, `channel_unread.rs`,
/// `community_authz.rs`, and the community read handlers), each scoped by
/// `nsid` first so a plain `nsid = ...` filter can also use them.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for stmt in [
            r#"CREATE INDEX IF NOT EXISTS "idx_record_data_nsid_subject" ON "record_data" ("nsid", ("data"->>'subject'))"#,
            r#"CREATE INDEX IF NOT EXISTS "idx_record_data_nsid_channel" ON "record_data" ("nsid", ("data"->>'channel'))"#,
            r#"CREATE INDEX IF NOT EXISTS "idx_record_data_nsid_community" ON "record_data" ("nsid", ("data"->>'community'))"#,
            r#"CREATE INDEX IF NOT EXISTS "idx_record_data_nsid_parent" ON "record_data" ("nsid", ("data"->>'parent'))"#,
        ] {
            db.execute_unprepared(stmt).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for stmt in [
            r#"DROP INDEX IF EXISTS "idx_record_data_nsid_subject""#,
            r#"DROP INDEX IF EXISTS "idx_record_data_nsid_channel""#,
            r#"DROP INDEX IF EXISTS "idx_record_data_nsid_community""#,
            r#"DROP INDEX IF EXISTS "idx_record_data_nsid_parent""#,
        ] {
            db.execute_unprepared(stmt).await?;
        }
        Ok(())
    }
}
