use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Reaction {
    pub id: Uuid,
    pub rkey: String,
    pub author_did: String,
    pub emoji: String,
    pub target_rkey: String,
    pub created_at: DateTime<Utc>,
}

/// Grouped reaction summary for API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactionSummary {
    pub emoji: String,
    pub count: i64,
    /// DIDs of all users who reacted with this emoji.
    pub authors: Vec<String>,
    /// Record keys of each reaction, parallel to `authors`.
    pub rkeys: Vec<String>,
}
