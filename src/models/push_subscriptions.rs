use sea_orm::entity::prelude::*;
use serde::Serialize;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize)]
#[sea_orm(table_name = "push_subscriptions")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Text")]
    pub actor_did: String,
    #[sea_orm(column_type = "Text")]
    pub endpoint: String,
    #[sea_orm(column_type = "Text")]
    pub p256dh: String,
    #[sea_orm(column_type = "Text")]
    pub auth: String,
    #[sea_orm(column_type = "Text")]
    pub platform: String,
    #[sea_orm(column_type = "Text")]
    pub created_at: String,
}

impl ActiveModelBehavior for ActiveModel {}
