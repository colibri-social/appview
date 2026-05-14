use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "community_credentials")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub community_did: String,
    #[sea_orm(column_type = "Text")]
    pub pds_endpoint: String,
    #[sea_orm(column_type = "Text")]
    pub identifier: String,
    /// AES-256-GCM ciphertext, base64-encoded.
    #[sea_orm(column_type = "Text")]
    pub password_ciphertext_b64: String,
    /// 12-byte nonce, base64-encoded.
    #[sea_orm(column_type = "Text")]
    pub password_nonce_b64: String,
    /// `appview_managed` | `byo`
    #[sea_orm(column_type = "Text")]
    pub source: String,
    #[sea_orm(column_type = "Text")]
    pub created_at: String,
}

impl ActiveModelBehavior for ActiveModel {}
