use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "record_data")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Text", unique_key = "idx-record-did-nsid-rkey")]
    pub did: String,
    #[sea_orm(column_type = "Text", unique_key = "idx-record-did-nsid-rkey")]
    pub nsid: String,
    #[sea_orm(column_type = "Text", unique_key = "idx-record-did-nsid-rkey")]
    pub rkey: String,
    #[sea_orm(column_type = "Text")]
    pub data: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
