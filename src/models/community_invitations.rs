use sea_orm::entity::prelude::*;
use serde::Serialize;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize)]
#[sea_orm(table_name = "community_invitations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub code: String,
    #[sea_orm(column_type = "Text")]
    pub community_uri: String,
    #[sea_orm(column_type = "Text")]
    pub created_by: String,
    pub active: bool,
    #[sea_orm(column_type = "Text")]
    pub created_at: String,
}

impl ActiveModelBehavior for ActiveModel {}
