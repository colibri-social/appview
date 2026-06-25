use sea_orm::entity::prelude::*;
use serde::Serialize;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize)]
#[sea_orm(table_name = "dismissed_applications")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Text")]
    pub community_did: String,
    #[sea_orm(column_type = "Text")]
    pub applicant_did: String,
    #[sea_orm(column_type = "Text")]
    pub dismissed_at: String,
}

impl ActiveModelBehavior for ActiveModel {}
