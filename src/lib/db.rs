use log::info;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbErr, Schema};
use std::time::Duration;

use crate::models::record_data;

/// Reads a positive `usize` from `name`, falling back to `default` when unset,
/// empty, unparseable, or zero.
fn env_usize(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Initializes a SeaORM PostgreSQL connection from `DATABASE_URL`.
///
/// Pool sizing is configurable via `DATABASE_MAX_CONNECTIONS` (default 20) and
/// `DATABASE_MIN_CONNECTIONS` (default 5). Keep `max` comfortably above
/// `TAP_WORKERS` (each tap worker holds a connection during its DB calls) plus
/// headroom for concurrent HTTP handlers, or requests will block on
/// `acquire_timeout`.
pub async fn init_db() -> Result<DatabaseConnection, DbErr> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL env var is required to initialize SeaORM");

    let max_connections = env_usize("DATABASE_MAX_CONNECTIONS", 20);
    // Clamp min to max so a misconfigured min can never exceed max.
    let min_connections = env_usize("DATABASE_MIN_CONNECTIONS", 5).min(max_connections);

    let mut options = ConnectOptions::new(database_url);
    options
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(Duration::from_secs(8))
        .idle_timeout(Duration::from_secs(8 * 60))
        .max_lifetime(Duration::from_secs(30 * 60))
        .sqlx_logging(false);

    let db = Database::connect(options).await?;

    let builder = db.get_database_backend();
    let schema = Schema::new(builder);

    let table_stmt = schema
        .create_table_from_entity(record_data::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(&table_stmt).await?;

    for mut stmt in schema.create_index_from_entity(record_data::Entity) {
        db.execute(stmt.if_not_exists()).await?;
    }

    info!("SeaORM initialized (pool: {min_connections}-{max_connections} connections).");
    Ok(db)
}
