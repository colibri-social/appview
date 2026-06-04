pub use sea_orm_migration::prelude::*;

mod m20260426_145024_create_user_states;
mod m20260504_071943_complete_user_stats;
mod m20260513_120000_create_community_invitations;
mod m20260514_090000_create_notifications;
mod m20260515_120000_create_community_credentials;
mod m20260526_000000_add_indexed_at_to_record_data;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260426_145024_create_user_states::Migration),
            Box::new(m20260504_071943_complete_user_stats::Migration),
            Box::new(m20260513_120000_create_community_invitations::Migration),
            Box::new(m20260514_090000_create_notifications::Migration),
            Box::new(m20260515_120000_create_community_credentials::Migration),
            Box::new(m20260526_000000_add_indexed_at_to_record_data::Migration),
        ]
    }
}
