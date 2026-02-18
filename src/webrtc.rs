use std::sync::Arc;

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use rocket::State;
use rocket_ws as ws;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::{debug, info};

// ── Room state ────────────────────────────────────────────────────────────────

type PeerTx = UnboundedSender<String>;

/// Shared map: room_id → (peer_id → channel to that peer's WebSocket).
#[derive(Clone, Default)]
pub struct RoomState(Arc<DashMap<String, DashMap<String, PeerTx>>>);

impl RoomState {
    pub fn new() -> Self {
        Self::default()
    }
}

// ── Signaling wire types ──────────────────────────────────────────────────────

/// Messages the client sends to the signaling server.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Signal {
    /// Join a voice/video room.
    Join { room: String, peer_id: String },
    /// Forward a WebRTC SDP offer to another peer in the same room.
    Offer { room: String, target_peer_id: String, sdp: String },
    /// Forward a WebRTC SDP answer to another peer in the same room.
    Answer { room: String, target_peer_id: String, sdp: String },
    /// Forward an ICE candidate to another peer in the same room.
    Ice { room: String, target_peer_id: String, candidate: String },
    /// Leave the room cleanly.
    Leave { room: String },
}

/// Messages the server sends to a client.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SignalResponse {
    /// Confirmation that this peer has joined; includes the list of other peers
    /// currently in the room so the new peer can initiate offers.
    Joined { room: String, peer_id: String, peers: Vec<String> },
    PeerJoined { room: String, peer_id: String },
    PeerLeft { room: String, peer_id: String },
    /// Relayed SDP offer (from another peer).
    Offer { from_peer_id: String, sdp: String },
    /// Relayed SDP answer (from another peer).
    Answer { from_peer_id: String, sdp: String },
    /// Relayed ICE candidate (from another peer).
    Ice { from_peer_id: String, candidate: String },
    Error { message: String },
}

// ── Rocket route ──────────────────────────────────────────────────────────────

#[rocket::get("/api/webrtc/signal")]
pub fn signal(ws: ws::WebSocket, rooms: &State<RoomState>) -> ws::Channel<'static> {
    let rooms = rooms.inner().clone();

    ws.channel(move |stream| {
        Box::pin(async move {
            let (mut sink, mut stream) = stream.split();
            // Per-connection outbound channel; other peers write here to reach us.
            let (self_tx, mut self_rx) = mpsc::unbounded_channel::<String>();

            let mut current_room: Option<String> = None;
            let mut current_peer_id: Option<String> = None;

            loop {
                tokio::select! {
                    // ── Routed messages from other peers ─────────────────────
                    Some(msg) = self_rx.recv() => {
                        if sink.send(ws::Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }

                    // ── Incoming messages from this client ───────────────────
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(ws::Message::Text(text))) => {
                                match serde_json::from_str::<Signal>(&text) {
                                    Ok(signal) => {
                                        process_signal(
                                            signal,
                                            &self_tx,
                                            &rooms,
                                            &mut current_room,
                                            &mut current_peer_id,
                                        );
                                    }
                                    Err(e) => {
                                        let err = serde_json::to_string(&SignalResponse::Error {
                                            message: format!("Invalid signal: {e}"),
                                        })
                                        .unwrap_or_default();
                                        let _ = self_tx.send(err);
                                    }
                                }
                            }
                            Some(Ok(ws::Message::Close(_))) | None => break,
                            _ => {}
                        }
                    }
                }
            }

            // Clean up when the connection closes.
            if let (Some(room), Some(peer_id)) = (current_room, current_peer_id) {
                cleanup_peer(&rooms, &room, &peer_id);
            }

            Ok(())
        })
    })
}

// ── Signal processing (sync – DashMap ops need no await) ─────────────────────

fn process_signal(
    signal: Signal,
    self_tx: &PeerTx,
    rooms: &RoomState,
    current_room: &mut Option<String>,
    current_peer_id: &mut Option<String>,
) {
    match signal {
        Signal::Join { room, peer_id } => {
            // Snapshot existing peers before we insert ourselves.
            let existing: Vec<String> = rooms
                .0
                .entry(room.clone())
                .or_default()
                .iter()
                .map(|e| e.key().clone())
                .collect();

            // Notify every existing peer that we joined.
            if let Some(room_map) = rooms.0.get(&room) {
                for entry in room_map.iter() {
                    let note = encode(&SignalResponse::PeerJoined {
                        room: room.clone(),
                        peer_id: peer_id.clone(),
                    });
                    let _ = entry.value().send(note);
                }
            }

            // Register ourselves.
            rooms
                .0
                .entry(room.clone())
                .or_default()
                .insert(peer_id.clone(), self_tx.clone());

            *current_room = Some(room.clone());
            *current_peer_id = Some(peer_id.clone());

            info!("Peer {peer_id} joined room {room} ({} peers total)", existing.len() + 1);

            let _ = self_tx.send(encode(&SignalResponse::Joined {
                room,
                peer_id,
                peers: existing,
            }));
        }

        Signal::Offer { room, target_peer_id, sdp } => {
            let from = peer_id_or_return!(current_peer_id);
            relay_to(&rooms, &room, &target_peer_id, &SignalResponse::Offer {
                from_peer_id: from,
                sdp,
            });
        }

        Signal::Answer { room, target_peer_id, sdp } => {
            let from = peer_id_or_return!(current_peer_id);
            relay_to(&rooms, &room, &target_peer_id, &SignalResponse::Answer {
                from_peer_id: from,
                sdp,
            });
        }

        Signal::Ice { room, target_peer_id, candidate } => {
            let from = peer_id_or_return!(current_peer_id);
            relay_to(&rooms, &room, &target_peer_id, &SignalResponse::Ice {
                from_peer_id: from,
                candidate,
            });
        }

        Signal::Leave { room } => {
            if let Some(peer_id) = current_peer_id.take() {
                cleanup_peer(rooms, &room, &peer_id);
            }
            *current_room = None;
        }
    }
}

/// Send a signaling message to one specific peer in a room.
fn relay_to(rooms: &RoomState, room: &str, target: &str, response: &SignalResponse) {
    if let Some(room_map) = rooms.0.get(room) {
        if let Some(tx) = room_map.get(target) {
            let _ = tx.send(encode(response));
        } else {
            debug!("relay_to: target peer {target} not found in room {room}");
        }
    }
}

/// Remove a peer from its room and notify the remaining peers.
fn cleanup_peer(rooms: &RoomState, room: &str, peer_id: &str) {
    if let Some(room_map) = rooms.0.get(room) {
        room_map.remove(peer_id);
        for entry in room_map.iter() {
            let _ = entry.value().send(encode(&SignalResponse::PeerLeft {
                room: room.to_string(),
                peer_id: peer_id.to_string(),
            }));
        }
        let is_empty = room_map.is_empty();
        drop(room_map);
        if is_empty {
            rooms.0.remove(room);
            info!("Room {room} is now empty and has been removed");
        }
    }
}

fn encode(r: &SignalResponse) -> String {
    serde_json::to_string(r).unwrap_or_default()
}

/// Return the peer ID string, or return from the calling function if not set.
macro_rules! peer_id_or_return {
    ($opt:expr) => {
        match $opt.as_deref() {
            Some(id) => id.to_string(),
            None => return,
        }
    };
}
use peer_id_or_return;
