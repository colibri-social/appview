use sea_orm::entity::prelude::*;
use serde::Serialize;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize)]
#[sea_orm(table_name = "notifications")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Text")]
    pub recipient_did: String,
    #[sea_orm(column_type = "Text")]
    pub kind: String,
    #[sea_orm(column_type = "Text")]
    pub message_uri: String,
    #[sea_orm(column_type = "Text")]
    pub author_did: String,
    #[sea_orm(column_type = "Text")]
    pub channel_uri: String,
    #[sea_orm(column_type = "Text")]
    pub indexed_at: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub seen_at: Option<String>,
}

impl ActiveModelBehavior for ActiveModel {}
