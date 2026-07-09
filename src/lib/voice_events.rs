use std::sync::Arc;

use rocket::tokio::sync::broadcast::Sender;

use crate::lib::event_scope::{EventScope, ScopedEvent, SharedScopedEvent};
use crate::lib::events::{
    ColibriServerEvent, ColibriServerEventData, VoicePresenceEventData, VoiceStateEventData,
};

#[allow(clippy::too_many_arguments)]
pub fn broadcast_voice_state(
    broadcast: &Sender<SharedScopedEvent>,
    community_did: &str,
    channel: &str,
    did: &str,
    muted: Option<bool>,
    deafened: Option<bool>,
    server_muted: Option<bool>,
    server_deafened: Option<bool>,
) {
    let server_event = ColibriServerEvent {
        event_type: String::from("voice_state_event"),
        data: Some(ColibriServerEventData::VoiceState(VoiceStateEventData {
            channel: channel.to_string(),
            did: did.to_string(),
            muted,
            deafened,
            server_muted,
            server_deafened,
        })),
    };
    let scoped = Arc::new(ScopedEvent {
        scope: EventScope::Community(community_did.to_string()),
        payload: server_event.serialize(),
    });
    let _ = broadcast.send(scoped);
}

pub fn broadcast_voice_presence(
    broadcast: &Sender<SharedScopedEvent>,
    community_did: &str,
    channel: &str,
    did: &str,
    event: &str,
) {
    let server_event = ColibriServerEvent {
        event_type: String::from("voice_presence_event"),
        data: Some(ColibriServerEventData::VoicePresence(
            VoicePresenceEventData {
                event: event.to_string(),
                channel: channel.to_string(),
                did: did.to_string(),
            },
        )),
    };
    let scoped = Arc::new(ScopedEvent {
        scope: EventScope::Community(community_did.to_string()),
        payload: server_event.serialize(),
    });
    let _ = broadcast.send(scoped);
}
