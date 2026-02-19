use serde::Serialize;
use tokio::sync::broadcast;

use crate::models::message::MessageWithAuthor;

/// All events that can be broadcast to connected clients.
/// Add new variants here to extend the event system.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppEvent {
    Message(MessageWithAuthor),
}

pub type EventBus = broadcast::Sender<AppEvent>;

pub fn create_event_bus(capacity: usize) -> EventBus {
    let (tx, _) = broadcast::channel(capacity);
    tx
}
