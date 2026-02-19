use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::author::AuthorProfile;

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

/// A message with the author's cached profile fields flattened in.
/// Used for all API responses and WebSocket broadcasts.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MessageWithAuthor {
    pub id: Uuid,
    pub rkey: String,
    pub author_did: String,
    pub text: String,
    pub channel: String,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

impl From<(Message, Option<AuthorProfile>)> for MessageWithAuthor {
    fn from((msg, profile): (Message, Option<AuthorProfile>)) -> Self {
        Self {
            id: msg.id,
            rkey: msg.rkey,
            author_did: msg.author_did,
            text: msg.text,
            channel: msg.channel,
            created_at: msg.created_at,
            indexed_at: msg.indexed_at,
            display_name: profile.as_ref().and_then(|p| p.display_name.clone()),
            avatar_url: profile.as_ref().and_then(|p| p.avatar_url.clone()),
        }
    }
}
