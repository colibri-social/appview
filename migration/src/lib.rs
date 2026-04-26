pub use sea_orm_migration::prelude::*;

mod m20260426_145024_create_user_states;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20260426_145024_create_user_states::Migration)]
    }
}
