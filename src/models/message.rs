use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Message {
    pub id: Uuid,
    pub rkey: String,
    pub author_did: String,
    pub text: String,
    pub channel: String,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
}
