use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::models::message::MessageResponse;

/// All events that can be broadcast to connected clients.
/// Add new variants here to extend the event system.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppEvent {
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
        channel: String,
    },
    ReactionRemoved {
        rkey: String,
        author_did: String,
        emoji: String,
        target_rkey: String,
        channel: String,
    },
}

pub type EventBus = broadcast::Sender<AppEvent>;

pub fn create_event_bus(capacity: usize) -> EventBus {
    let (tx, _) = broadcast::channel(capacity);
    tx
}
