use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use rocket::State;
use rocket_ws as ws;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::events::{AppEvent, EventBus};

// ── Shared presence map ───────────────────────────────────────────────────────

/// Per-DID connection state, shared across all WebSocket connections for a user.
#[derive(Debug)]
pub struct DIDConnectionState {
    /// Number of currently-open WebSocket connections for this DID.
    pub count: usize,
    /// Most recent non-heartbeat activity across *all* connections for this DID.
    pub last_active: tokio::time::Instant,
    /// Whether the appview has set this DID to `away` due to inactivity.
    /// Reset to `false` as soon as any connection sends a non-heartbeat action.
    pub is_away: bool,
}

/// Application-wide presence map.  Registered as Rocket managed state.
pub type PresenceMap = Arc<Mutex<HashMap<String, DIDConnectionState>>>;

// ── Client → server ───────────────────────────────────────────────────────────

/// A subscription request sent by the client over the WebSocket.
///
/// Examples:
/// ```json
/// {"action":"subscribe",  "event_type":"message",   "channel":"general"}
/// {"action":"subscribe",  "event_type":"message"}     // all channels
/// {"action":"subscribe",  "event_type":"community", "community_uri":"at://did:plc:…/social.colibri.community/rkey"}
/// {"action":"subscribe",  "event_type":"owner",     "did":"did:plc:…"}
/// {"action":"unsubscribe","event_type":"message",   "channel":"general"}
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
    /// Community AT-URI filter for "community" event type.
    #[serde(default)]
    community_uri: Option<String>,
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
    /// Same semantics, filtered by community AT-URI.
    community: Option<Option<HashSet<String>>>,
    /// Filtered by DID. `Some(None)` = all users.
    user_status: Option<Option<HashSet<String>>>,
    /// DIDs of all members across all subscribed communities.
    /// Status and profile update events for these DIDs are delivered automatically.
    community_member_dids: HashSet<String>,
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
                }
                None => self.messages = Some(None),
            },
            "community" => match param {
                Some(uri) => {
                    let set = self.community.get_or_insert_with(|| Some(HashSet::new()));
                    if let Some(inner) = set {
                        inner.insert(uri);
                    }
                }
                None => self.community = Some(None),
            },
            "user_status" => match param {
                Some(did) => {
                    let set = self.user_status.get_or_insert_with(|| Some(HashSet::new()));
                    if let Some(inner) = set {
                        inner.insert(did);
                    }
                }
                None => self.user_status = Some(None),
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
            "community" => match param {
                Some(uri) => {
                    if let Some(Some(set)) = &mut self.community {
                        set.remove(&uri);
                    }
                }
                None => self.community = None,
            },
            "user_status" => match param {
                Some(did) => {
                    if let Some(Some(set)) = &mut self.user_status {
                        set.remove(&did);
                    }
                }
                None => self.user_status = None,
            },
            other => debug!("Unknown event_type in unsubscribe: {other}"),
        }
    }

    fn matches(&self, event: &AppEvent) -> bool {
        match event {
            AppEvent::Message(resp) => match &self.messages {
                None => false,
                Some(None) => true,
                Some(Some(channels)) => channels.contains(&resp.message.channel),
            },
            AppEvent::MessageDeleted { channel, .. }
            | AppEvent::ReactionAdded { channel, .. }
            | AppEvent::ReactionRemoved { channel, .. } => match &self.messages {
                None => false,
                Some(None) => true,
                Some(Some(channels)) => channels.contains(channel),
            },
            AppEvent::ChannelCreated { community_uri, .. }
            | AppEvent::ChannelDeleted { community_uri, .. }
            | AppEvent::CategoryCreated { community_uri, .. }
            | AppEvent::CategoryDeleted { community_uri, .. }
            | AppEvent::MemberPending { community_uri, .. }
            | AppEvent::MemberJoined { community_uri, .. }
            | AppEvent::MemberLeft { community_uri, .. }
            | AppEvent::CommunityUpserted { community_uri, .. }
            | AppEvent::CommunityDeleted { community_uri, .. } => match &self.community {
                None => false,
                Some(None) => true,
                Some(Some(uris)) => uris.contains(community_uri),
            },
            AppEvent::UserStatusChanged { did, .. }
            | AppEvent::UserProfileUpdated { did, .. } => {
                // Deliver if explicitly watching this DID via user_status subscription…
                let explicit = match &self.user_status {
                    None => false,
                    Some(None) => true,
                    Some(Some(dids)) => dids.contains(did),
                };
                // …or if the DID is a member of any subscribed community.
                explicit || self.community_member_dids.contains(did)
            }
        }
    }
}

// ── Rocket route ──────────────────────────────────────────────────────────────

const AWAY_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(300);

#[rocket::get("/api/subscribe?<did>")]
pub fn subscribe(
    ws: ws::WebSocket,
    bus: &State<EventBus>,
    pool: &State<PgPool>,
    presence_map: &State<PresenceMap>,
    did: Option<String>,
) -> ws::Channel<'static> {
    let mut rx = bus.subscribe();
    let bus = bus.inner().clone();
    let pool = pool.inner().clone();
    let presence_map = presence_map.inner().clone();
    let did = did.filter(|d| !d.is_empty());

    ws.channel(move |stream| {
        Box::pin(async move {
            let (mut sink, mut stream) = stream.split();
            let mut subs = Subscriptions::default();

            // ── On connect: register in presence map ─────────────────────────
            let mut preferred_state = "online".to_string();
            if let Some(ref d) = did {
                // Read preferred_state from DB.
                if let Ok(Some(profile)) = crate::db::get_author_profile(&pool, d).await {
                    preferred_state = profile
                        .preferred_state
                        .as_deref()
                        .unwrap_or("online")
                        .to_string();
                }

                // Increment the shared connection counter for this DID.
                // If this is the first connection, or all previous connections had gone
                // away, broadcast a status update — but only once.
                let (first_connection, was_away) = {
                    let mut map = presence_map.lock().await;
                    let entry = map.entry(d.clone()).or_insert(DIDConnectionState {
                        count: 0,
                        last_active: tokio::time::Instant::now(),
                        is_away: false,
                    });
                    entry.count += 1;
                    let first = entry.count == 1;
                    let was_away = entry.is_away;
                    // A new connection counts as activity — clear any pending away flag.
                    if was_away {
                        entry.is_away = false;
                        entry.last_active = tokio::time::Instant::now();
                    }
                    (first, was_away)
                };

                if first_connection || was_away {
                    if let Ok(updated) =
                        crate::db::set_user_state(&pool, d, &preferred_state, false).await
                    {
                        let _ = bus.send(AppEvent::UserStatusChanged {
                            did: d.clone(),
                            status: updated.status.unwrap_or_default(),
                            emoji: updated.emoji,
                            state: updated.state,
                            display_name: updated.display_name,
                            avatar_url: updated.avatar_url,
                        });
                    }
                }
            }

            let mut away_check = tokio::time::interval(tokio::time::Duration::from_secs(30));
            away_check.tick().await; // consume the immediate first tick

            loop {
                tokio::select! {
                    // ── Away timer check ─────────────────────────────────────
                    _ = away_check.tick() => {
                        if let Some(ref d) = did {
                            // Only broadcast once: whichever connection first observes the
                            // timeout sets is_away=true in the shared map; subsequent
                            // timer ticks from other connections are no-ops.
                            let should_set_away = {
                                let mut map = presence_map.lock().await;
                                if let Some(entry) = map.get_mut(d) {
                                    if !entry.is_away && entry.last_active.elapsed() >= AWAY_TIMEOUT {
                                        entry.is_away = true;
                                        true
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            };
                            if should_set_away {
                                if let Ok(updated) =
                                    crate::db::set_user_state(&pool, d, "away", false).await
                                {
                                    let _ = bus.send(AppEvent::UserStatusChanged {
                                        did: d.clone(),
                                        status: updated.status.unwrap_or_default(),
                                        emoji: updated.emoji,
                                        state: updated.state,
                                        display_name: updated.display_name,
                                        avatar_url: updated.avatar_url,
                                    });
                                }
                            }
                        }
                    }

                    // ── Incoming message from this client ────────────────────
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(ws::Message::Text(text))) => {
                                match serde_json::from_str::<ClientRequest>(&text) {
                                    Ok(req) => {
                                        let param = req.community_uri.clone()
                                            .or(req.channel)
                                            .or(req.did);

                                        let reply = match req.action.as_str() {
                                            "heartbeat" => {
                                                // Heartbeat does NOT update shared last_active.
                                                ServerMessage::Ack { message: String::new() }
                                            }
                                            action => {
                                                // Any non-heartbeat action updates the shared
                                                // last_active and clears is_away (if set), but
                                                // only broadcasts once if transitioning from away.
                                                let was_away = if let Some(ref d) = did {
                                                    let mut map = presence_map.lock().await;
                                                    if let Some(entry) = map.get_mut(d) {
                                                        entry.last_active = tokio::time::Instant::now();
                                                        let away = entry.is_away;
                                                        if away {
                                                            entry.is_away = false;
                                                        }
                                                        away
                                                    } else {
                                                        false
                                                    }
                                                } else {
                                                    false
                                                };

                                                if was_away {
                                                    if let Some(ref d) = did {
                                                        if let Ok(updated) = crate::db::set_user_state(
                                                            &pool, d, &preferred_state, false,
                                                        ).await {
                                                            let _ = bus.send(AppEvent::UserStatusChanged {
                                                                did: d.clone(),
                                                                status: updated.status.unwrap_or_default(),
                                                                emoji: updated.emoji,
                                                                state: updated.state,
                                                                display_name: updated.display_name,
                                                                avatar_url: updated.avatar_url,
                                                            });
                                                        }
                                                    }
                                                }

                                                match action {
                                                    "subscribe" | "unsubscribe" => {
                                                        if action == "subscribe" {
                                                            if req.event_type == "community" {
                                                                if let Some(uri) = &req.community_uri {
                                                                    match crate::db::get_member_dids_for_community(&pool, uri).await {
                                                                        Ok(dids) => { subs.community_member_dids.extend(dids); }
                                                                        Err(e) => error!("Failed to fetch member DIDs for {uri}: {e}"),
                                                                    }
                                                                }
                                                            }
                                                            subs.subscribe(&req.event_type, param);
                                                            ServerMessage::Ack { message: format!("Subscribed to {}", req.event_type) }
                                                        } else {
                                                            subs.unsubscribe(&req.event_type, param);
                                                            ServerMessage::Ack { message: format!("Unsubscribed from {}", req.event_type) }
                                                        }
                                                    }
                                                    // Generic activity ping — client sends this after
                                                    // any user action (e.g. sending a message via REST).
                                                    "activity" => ServerMessage::Ack { message: String::new() },
                                                    other => ServerMessage::Error {
                                                        message: format!("Unknown action: {other}"),
                                                    },
                                                }
                                            }
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
                                // When a new member joins a subscribed community, add their
                                // DID to the watch-list so future status/profile events
                                // are delivered automatically.
                                if let AppEvent::MemberJoined { member_did, community_uri, .. } = &event {
                                    if let Some(Some(uris)) = &subs.community {
                                        if uris.contains(community_uri) {
                                            subs.community_member_dids.insert(member_did.clone());
                                        }
                                    } else if subs.community == Some(None) {
                                        subs.community_member_dids.insert(member_did.clone());
                                    }
                                }
                                // When a member leaves, rebuild the watch-list so we stop
                                // delivering their events (unless they're in another subscribed community).
                                if let AppEvent::MemberLeft { community_uri, .. } = &event {
                                    let rebuild = match &subs.community {
                                        Some(Some(uris)) => uris.contains(community_uri),
                                        Some(None) => true,
                                        None => false,
                                    };
                                    if rebuild {
                                        let subscribed_uris: Vec<String> = match &subs.community {
                                            Some(Some(uris)) => uris.iter().cloned().collect(),
                                            Some(None) => vec![community_uri.clone()],
                                            None => vec![],
                                        };
                                        let mut new_dids = HashSet::new();
                                        for uri in &subscribed_uris {
                                            if let Ok(dids) = crate::db::get_member_dids_for_community(&pool, uri).await {
                                                new_dids.extend(dids);
                                            }
                                        }
                                        subs.community_member_dids = new_dids;
                                    }
                                }
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

            // ── On disconnect: only set offline when the last connection closes ──
            if let Some(ref d) = did {
                let last_connection = {
                    let mut map = presence_map.lock().await;
                    if let Some(entry) = map.get_mut(d) {
                        entry.count = entry.count.saturating_sub(1);
                        if entry.count == 0 {
                            map.remove(d);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if last_connection {
                    if let Ok(updated) =
                        crate::db::set_user_state(&pool, d, "offline", false).await
                    {
                        let _ = bus.send(AppEvent::UserStatusChanged {
                            did: d.clone(),
                            status: updated.status.unwrap_or_default(),
                            emoji: updated.emoji,
                            state: updated.state,
                            display_name: updated.display_name,
                            avatar_url: updated.avatar_url,
                        });
                    }
                }
            }

            Ok(())
        })
    })
}
