use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::models::message::MessageResponse;

/// All events that can be broadcast to connected clients.
/// Add new variants here to extend the event system.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppEvent {
    // ── Message events (filtered by channel name) ─────────────────────────
    Message(MessageResponse),
    MessageDeleted {
        id: Uuid,
        rkey: String,
        author_did: String,
        channel: String,
    },
    ReactionAdded {
        rkey: String,
        author_did: String,
        emoji: String,
        target_rkey: String,
        target_author_did: String,
        channel: String,
    },
    ReactionRemoved {
        rkey: String,
        author_did: String,
        emoji: String,
        target_rkey: String,
        target_author_did: String,
        channel: String,
    },
    // ── Community events (filtered by community_uri) ──────────────────────
    ChannelCreated {
        community_uri: String,
        uri: String,
        rkey: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        channel_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        category_rkey: Option<String>,
    },
    ChannelDeleted {
        community_uri: String,
        uri: String,
        rkey: String,
    },
    CategoryCreated {
        community_uri: String,
        uri: String,
        rkey: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_order: Option<Vec<String>>,
    },
    CategoryDeleted {
        community_uri: String,
        uri: String,
        rkey: String,
    },
    /// Fired when a membership record is created (status = pending).
    MemberPending {
        community_uri: String,
        member_did: String,
        membership_uri: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        avatar_url: Option<String>,
    },
    /// Fired when an approval record is created (status = approved).
    MemberJoined {
        community_uri: String,
        member_did: String,
        membership_uri: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        avatar_url: Option<String>,
    },
    MemberLeft {
        community_uri: String,
        member_did: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        avatar_url: Option<String>,
    },
    // ── Owner-scoped community list events (filtered by community_uri) ───────
    /// A community was created or fully updated.
    CommunityUpserted {
        community_uri: String,
        owner_did: String,
        rkey: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        picture: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        category_order: Option<serde_json::Value>,
    },
    CommunityDeleted {
        community_uri: String,
        owner_did: String,
        rkey: String,
    },
    // ── User status events (filtered by DID) ─────────────────────────────
    /// Fired when a user updates their social.colibri.actor.data record.
    UserStatusChanged {
        did: String,
        status: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        emoji: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        avatar_url: Option<String>,
    },
    UserProfileUpdated {
        did: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        avatar_url: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        banner_url: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        handle: Option<String>,
    },
}

pub type EventBus = broadcast::Sender<AppEvent>;

pub fn create_event_bus(capacity: usize) -> EventBus {
    let (tx, _) = broadcast::channel(capacity);
    tx
}
