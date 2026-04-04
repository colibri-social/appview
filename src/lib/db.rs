use log::info;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbErr};
use std::time::Duration;

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
    // synchronizes database schema with entity definitions
    db.get_schema_registry(module_path!().split("::").next().unwrap());

    info!("SeaORM initialized.");
    Ok(db)
}
