use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::author::AuthorProfile;
use super::reaction::ReactionSummary;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Message {
    pub id: Uuid,
    pub rkey: String,
    pub author_did: String,
    pub text: String,
    pub channel: String,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub edited: bool,
    pub parent: Option<String>,
    pub facets: Option<sqlx::types::Json<serde_json::Value>>,
    pub attachments: Option<sqlx::types::Json<serde_json::Value>>,
}

/// A message with the author's cached profile fields flattened in, mapped from DB rows.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MessageWithAuthor {
    pub id: Uuid,
    pub rkey: String,
    pub author_did: String,
    pub text: String,
    pub channel: String,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub edited: bool,
    pub parent: Option<String>,
    pub facets: Option<sqlx::types::Json<serde_json::Value>>,
    pub attachments: Option<sqlx::types::Json<serde_json::Value>>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

/// Full API / WebSocket response: message + author profile + parent + reactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageResponse {
    #[serde(flatten)]
    pub message: MessageWithAuthor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_message: Option<Box<MessageWithAuthor>>,
    pub reactions: Vec<ReactionSummary>,
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
            edited: msg.edited,
            parent: msg.parent,
            facets: msg.facets,
            attachments: msg.attachments,
            display_name: profile.as_ref().and_then(|p| p.display_name.clone()),
            avatar_url: profile.as_ref().and_then(|p| p.avatar_url.clone()),
        }
    }
}
