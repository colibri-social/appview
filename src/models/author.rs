use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AuthorProfile {
    pub did: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub banner_url: Option<String>,
    pub description: Option<String>,
    pub handle: Option<String>,
    pub status: Option<String>,
    pub emoji: Option<String>,
    pub updated_at: DateTime<Utc>,
}
