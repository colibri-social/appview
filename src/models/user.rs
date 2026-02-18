use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct UserStatus {
    pub did: String,
    pub status: String,
    pub updated_at: DateTime<Utc>,
}
