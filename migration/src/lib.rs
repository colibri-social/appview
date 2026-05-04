pub use sea_orm_migration::prelude::*;

mod m20260426_145024_create_user_states;
mod m20260504_071943_complete_user_stats;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260426_145024_create_user_states::Migration),
            Box::new(m20260504_071943_complete_user_stats::Migration),
        ]
    }
}
