use std::collections::HashSet;

use futures_util::{SinkExt, StreamExt};
use rocket::State;
use rocket_ws as ws;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

use crate::events::{AppEvent, EventBus};

// ── Client → server ───────────────────────────────────────────────────────────

/// A subscription request sent by the client over the WebSocket.
///
/// Examples:
/// ```json
/// {"action":"subscribe",  "event_type":"message",     "channel":"general"}
/// {"action":"subscribe",  "event_type":"message"}        // all channels
/// {"action":"subscribe",  "event_type":"user_status", "did":"did:plc:…"}
/// {"action":"subscribe",  "event_type":"user_status"}    // all users
/// {"action":"unsubscribe","event_type":"message",     "channel":"general"}
/// ```
#[derive(Debug, Deserialize)]
struct ClientRequest {
    action: String,
    event_type: String,
    /// Filter parameter: channel name for "message", DID for "user_status".
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    did: Option<String>,
}

// ── Server → client ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Ack { message: String },
    Error { message: String },
}

// ── Subscription state ────────────────────────────────────────────────────────

/// Tracks which events a connected client wants to receive.
/// Extend this struct to add new filterable event types.
#[derive(Debug, Default)]
struct Subscriptions {
    /// `None`         = not subscribed to messages at all.
    /// `Some(None)`   = subscribed to messages in *all* channels.
    /// `Some(Some(s))`= subscribed to messages only in the listed channels.
    messages: Option<Option<HashSet<String>>>,
}

impl Subscriptions {
    fn subscribe(&mut self, event_type: &str, param: Option<String>) {
        match event_type {
            "message" => match param {
                Some(ch) => {
                    let set = self.messages.get_or_insert_with(|| Some(HashSet::new()));
                    if let Some(inner) = set {
                        inner.insert(ch);
                    }
                    // If already Some(None) (all channels), keep it.
                }
                None => self.messages = Some(None),
            },
            other => debug!("Unknown event_type in subscribe: {other}"),
        }
    }

    fn unsubscribe(&mut self, event_type: &str, param: Option<String>) {
        match event_type {
            "message" => match param {
                Some(ch) => {
                    if let Some(Some(set)) = &mut self.messages {
                        set.remove(&ch);
                    }
                }
                None => self.messages = None,
            },
            other => debug!("Unknown event_type in unsubscribe: {other}"),
        }
    }

    fn matches(&self, event: &AppEvent) -> bool {
        match event {
            AppEvent::Message(msg) => match &self.messages {
                None => false,
                Some(None) => true,
                Some(Some(channels)) => channels.contains(&msg.channel),
            },
        }
    }
}

// ── Rocket route ──────────────────────────────────────────────────────────────

#[rocket::get("/api/subscribe")]
pub fn subscribe(ws: ws::WebSocket, bus: &State<EventBus>) -> ws::Channel<'static> {
    let mut rx = bus.subscribe();

    ws.channel(move |stream| {
        Box::pin(async move {
            let (mut sink, mut stream) = stream.split();
            let mut subs = Subscriptions::default();

            loop {
                tokio::select! {
                    // ── Incoming message from this client ────────────────────
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(ws::Message::Text(text))) => {
                                match serde_json::from_str::<ClientRequest>(&text) {
                                    Ok(req) => {
                                        let param = req.channel.or(req.did);
                                        let reply = match req.action.as_str() {
                                            "subscribe" => {
                                                subs.subscribe(&req.event_type, param);
                                                ServerMessage::Ack {
                                                    message: format!("Subscribed to {}", req.event_type),
                                                }
                                            }
                                            "unsubscribe" => {
                                                subs.unsubscribe(&req.event_type, param);
                                                ServerMessage::Ack {
                                                    message: format!("Unsubscribed from {}", req.event_type),
                                                }
                                            }
                                            "heartbeat" => {
                                                ServerMessage::Ack {
                                                    message: format!("", req.event_type),
                                                }
                                            }
                                            other => ServerMessage::Error {
                                                message: format!("Unknown action: {other}"),
                                            },
                                        };
                                        let json = serde_json::to_string(&reply).unwrap_or_default();
                                        if sink.send(ws::Message::Text(json)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(_) => {
                                        let err = serde_json::to_string(&ServerMessage::Error {
                                            message: "Invalid JSON".to_string(),
                                        })
                                        .unwrap_or_default();
                                        let _ = sink.send(ws::Message::Text(err)).await;
                                    }
                                }
                            }
                            Some(Ok(ws::Message::Close(_))) | None => break,
                            _ => {}
                        }
                    }

                    // ── Broadcast event from the event bus ───────────────────
                    event = rx.recv() => {
                        match event {
                            Ok(event) if subs.matches(&event) => {
                                match serde_json::to_string(&event) {
                                    Ok(json) => {
                                        if sink.send(ws::Message::Text(json)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => error!("Serialization error: {e}"),
                                }
                            }
                            Ok(_) => {} // filtered out
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                debug!("Subscriber lagged by {n} events");
                            }
                            Err(_) => break,
                        }
                    }
                }
            }

            Ok(())
        })
    })
}
