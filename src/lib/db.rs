use log::info;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbErr, Schema};
use std::time::Duration;

use crate::models::record_data;

/// Initializes a SeaORM PostgreSQL connection from `DATABASE_URL`
pub async fn init_db() -> Result<DatabaseConnection, DbErr> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL env var is required to initialize SeaORM");

    let mut options = ConnectOptions::new(database_url);
    options
        .max_connections(20)
        .min_connections(5)
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

    info!("SeaORM initialized.");
    Ok(db)
}
