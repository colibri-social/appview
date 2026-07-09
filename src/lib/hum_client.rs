//! Humming outbound side: the hub-connection manager.
//!
//! This AppView is a **leaf** for communities hubbed elsewhere and a **hub** for
//! communities it administers. The manager covers both directions:
//!
//! - **Ingress-out.** When a local user's presence/typing/voice changes, the
//!   relevant handlers enqueue an [`OutboundHum`]. The manager resolves each of
//!   the user's affected communities to its hub ([`community_hub_did`]); for a
//!   community this AppView hubs it publishes the Hum onto `bridge.hums` (so its
//!   `subscribeHums` peers forward it), and for a remote hub it `sendHum`s it.
//! - **Egress-in.** For every remote hub that administers a community a local
//!   online user belongs to, the manager holds a supervised `subscribeHums`
//!   connection and injects received Hums locally (never re-relaying — leaves
//!   don't forward).
//!
//! All of this is inert unless [`humming_enabled`] returns true; the whole
//! manager task is only spawned when it does.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use rocket::tokio::sync::{broadcast, mpsc};
use rocket::tokio::time::{interval, sleep};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message as TungMessage};

use crate::EventNotification;
use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriActorData, ColibriCommunity, community_hub_did};
use crate::lib::event_scope::SharedScopedEvent;
use crate::lib::events::{
    HumEnvelope, HumEvent, TypingEventData, UserEventData, UserEventProfile, UserEventStatus,
    VoicePresenceEventData, VoiceStateEventData,
};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::get_state::get_state;
use crate::lib::http::HTTP;
use crate::lib::service_auth::{appview_did, mint_appview_auth};
use crate::models::user_states;
use crate::xrpc::social::colibri::actor::list_communities_handler::get_authorized_communities;
use crate::xrpc::social::colibri::sync::send_hum_handler::ingest_trusted_hum;
use crate::xrpc::social::colibri::sync::subscribe_events_handler::AUTH_SUBPROTOCOL;

const COMMUNITY_NSID: &str = "social.colibri.community";

/// How often the egress-in manager recomputes which remote hubs to connect to.
const RECONCILE_SECS: u64 = 30;
/// Backoff between `subscribeHums` reconnect attempts to a single hub.
const RECONNECT_BACKOFF_SECS: u64 = 5;
/// How often an idle hub connection wakes to check whether it should stop.
const CONN_STOP_POLL_SECS: u64 = 10;

/// Per-process Hum id counter. Combined with the origin DID it yields an id
/// unique to this AppView, which receivers use to dedup across relay paths.
static HUM_SEQ: AtomicU64 = AtomicU64::new(0);

/// Whether Humming (cross-AppView off-protocol presence relay) is enabled.
/// **On by default** — a deployment opts out with `HUMMING_ENABLED=false` (also
/// accepts `0`/`no`). When disabled the inbound endpoints reject, the manager
/// task is never spawned, and the local hooks never enqueue.
pub fn humming_enabled() -> bool {
    std::env::var("HUMMING_ENABLED")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true)
}

/// A local off-protocol change the manager should propagate to the relevant
/// community hubs.
#[derive(Debug, Clone)]
pub enum OutboundHum {
    /// The user's online-state/status changed — propagate to all their
    /// communities.
    Presence { did: String },
    /// The user started typing in a channel (an at-uri).
    Typing { did: String, channel: String },
    /// The user joined or left a voice channel (an at-uri); `event` is
    /// `join`/`leave`.
    Voice {
        did: String,
        channel: String,
        event: String,
    },
    VoiceState {
        did: String,
        channel: String,
        muted: bool,
        deafened: bool,
    },
}

/// Enqueues a local change for cross-AppView propagation. No-op when Humming is
/// disabled or the outbox is full (presence is best-effort — a dropped update is
/// superseded by the next one).
pub fn enqueue(outbox: &mpsc::Sender<OutboundHum>, item: OutboundHum) {
    if !humming_enabled() {
        return;
    }
    if let Err(e) = outbox.try_send(item) {
        log::debug!("hum outbox drop: {e}");
    }
}

fn next_id(origin: &str) -> String {
    format!("{origin}:{}", HUM_SEQ.fetch_add(1, Ordering::Relaxed))
}

/// Host portion of a `did:web` (with any percent-encoded port decoded), or
/// `None` for other DID methods.
fn did_web_host(did: &str) -> Option<String> {
    did.strip_prefix("did:web:").map(|h| h.replace("%3A", ":"))
}

/// Runs the outbound Humming manager: spawns the egress-in reconciler and drains
/// the ingress-out queue. Returns only if the outbox is closed.
pub async fn run_hum_manager(
    db: DatabaseConnection,
    broadcast_tx: broadcast::Sender<SharedScopedEvent>,
    c2c_tx: broadcast::Sender<EventNotification>,
    hums_tx: broadcast::Sender<HumEnvelope>,
    mut outbox_rx: mpsc::Receiver<OutboundHum>,
) {
    rocket::tokio::spawn(run_egress_reconciler(
        db.clone(),
        broadcast_tx.clone(),
        c2c_tx.clone(),
    ));

    log::info!("Humming manager started (HUMMING_ENABLED)");

    while let Some(item) = outbox_rx.recv().await {
        dispatch_outbound(&db, &hums_tx, item).await;
    }
}

// -- Ingress-out: local change -> the community's hub -------------------------

async fn dispatch_outbound(
    db: &DatabaseConnection,
    hums_tx: &broadcast::Sender<HumEnvelope>,
    item: OutboundHum,
) {
    let (subject, event, targets) = match resolve_outbound(db, item).await {
        Some(v) => v,
        None => return,
    };

    let me = appview_did();
    for (community_uri, hub) in targets {
        let envelope = HumEnvelope {
            origin: me.clone(),
            id: next_id(&me),
            ttl: 1,
            subject: subject.clone(),
            community: community_uri,
            event: event.clone(),
        };

        if hub == me {
            // We hub this community — hand straight to our own relay so
            // `subscribeHums` peers pick it up. Local clients already saw it via
            // the normal broadcast path.
            let _ = hums_tx.send(envelope);
        } else {
            post_hum(&hub, &envelope).await;
        }
    }
}

/// Resolves a queued change into `(subject, event, [(community_uri, hub_did)])`.
async fn resolve_outbound(
    db: &DatabaseConnection,
    item: OutboundHum,
) -> Option<(String, HumEvent, Vec<(String, String)>)> {
    match item {
        OutboundHum::Presence { did } => {
            let event = HumEvent::User(Box::new(presence_payload(db, &did).await));
            let targets = presence_targets(db, &did).await;
            Some((did, event, targets))
        }
        OutboundHum::Typing { did, channel } => {
            let (community_uri, hub) = channel_target(db, &channel).await?;
            let event = HumEvent::Typing(TypingEventData {
                event: String::from("start"),
                channel,
                did: did.clone(),
            });
            Some((did, event, vec![(community_uri, hub)]))
        }
        OutboundHum::Voice {
            did,
            channel,
            event,
        } => {
            let (community_uri, hub) = channel_target(db, &channel).await?;
            let payload = HumEvent::VoicePresence(VoicePresenceEventData {
                event,
                channel,
                did: did.clone(),
            });
            Some((did, payload, vec![(community_uri, hub)]))
        }
        OutboundHum::VoiceState {
            did,
            channel,
            muted,
            deafened,
        } => {
            let (community_uri, hub) = channel_target(db, &channel).await?;
            let payload = HumEvent::VoiceState(VoiceStateEventData {
                channel,
                did: did.clone(),
                muted,
                deafened,
            });
            Some((did, payload, vec![(community_uri, hub)]))
        }
    }
}

/// Builds the presence `user_event` payload. Identity fields are left minimal on
/// purpose — the receiver re-derives them from its own view of the subject's
/// repo, so only the ephemeral status matters here.
async fn presence_payload(db: &DatabaseConnection, did: &str) -> UserEventData {
    let state = get_state(did.to_string(), db)
        .await
        .map(|s| s.to_string())
        .unwrap_or_else(|_| String::from("online"));

    let actor_data = get_atproto_record::<ColibriActorData>(
        did.to_string(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db,
    )
    .await
    .ok();

    UserEventData {
        did: did.to_string(),
        status: Some(UserEventStatus {
            state,
            text: actor_data
                .as_ref()
                .and_then(|d| d.status.clone())
                .unwrap_or_default(),
            emoji: actor_data.and_then(|d| d.emoji),
        }),
        profile: UserEventProfile {
            display_name: None,
            avatar: None,
            banner: None,
            description: None,
            is_bot: false,
            handle: did.to_string(),
            theme: None,
        },
    }
}

/// The `(community_uri, hub_did)` pairs for all communities the user belongs to.
async fn presence_targets(db: &DatabaseConnection, did: &str) -> Vec<(String, String)> {
    let communities = match get_authorized_communities(db, did).await {
        Ok(c) => c,
        Err(e) => {
            log::warn!("presence targets for {did}: {e}");
            return Vec::new();
        }
    };

    communities
        .into_iter()
        .filter_map(|c| {
            let community_did = c.community.did.clone();
            let record = serde_json::from_value::<ColibriCommunity>(c.community.data).ok()?;
            let uri = format!("at://{community_did}/{COMMUNITY_NSID}/self");
            Some((uri, community_hub_did(&record)))
        })
        .collect()
}

/// The `(community_uri, hub_did)` for a channel at-uri: the channel's authority
/// is the community DID, whose record yields the hub.
async fn channel_target(db: &DatabaseConnection, channel: &str) -> Option<(String, String)> {
    let community_did = AtUri::parse(channel)?.authority;
    let community_uri = format!("at://{community_did}/{COMMUNITY_NSID}/self");

    let hub = get_atproto_record::<ColibriCommunity>(
        community_did,
        String::from(COMMUNITY_NSID),
        String::from("self"),
        db,
    )
    .await
    .map(|record| community_hub_did(&record))
    .unwrap_or_else(|_| String::from("did:web:api.colibri.social"));

    Some((community_uri, hub))
}

async fn post_hum(hub_did: &str, envelope: &HumEnvelope) {
    let Some(host) = did_web_host(hub_did) else {
        log::warn!("cannot resolve host for hub {hub_did}");
        return;
    };
    let token = match mint_appview_auth(hub_did, "social.colibri.sync.sendHum") {
        Ok(t) => t,
        Err(e) => {
            log::warn!("failed to mint sendHum token for {hub_did}: {e}");
            return;
        }
    };

    let url = format!("https://{host}/xrpc/social.colibri.sync.sendHum");
    match HTTP
        .post(&url)
        .bearer_auth(&token)
        .json(envelope)
        .send()
        .await
    {
        Ok(res) if !res.status().is_success() => {
            log::warn!("sendHum to {hub_did} returned {}", res.status());
        }
        Ok(_) => {}
        Err(e) => log::warn!("sendHum to {hub_did} failed: {e}"),
    }
}

// -- Egress-in: subscribe to remote hubs --------------------------------------

/// Reconciles the set of `subscribeHums` connections to remote hubs against the
/// communities local online users belong to, spawning connections for new hubs,
/// signalling stale ones to stop, and reconnecting when a hub's community set
/// changes (so the declared-interest query stays accurate).
async fn run_egress_reconciler(
    db: DatabaseConnection,
    broadcast_tx: broadcast::Sender<SharedScopedEvent>,
    c2c_tx: broadcast::Sender<EventNotification>,
) {
    // hub DID -> (stop flag, the community set we connected with).
    let mut connections: HashMap<String, (Arc<AtomicBool>, HashSet<String>)> = HashMap::new();
    let mut ticker = interval(Duration::from_secs(RECONCILE_SECS));

    loop {
        ticker.tick().await;

        let mut desired = match desired_remote_hubs(&db).await {
            Ok(map) => map,
            Err(e) => {
                log::warn!("hub reconcile query failed: {e}");
                continue;
            }
        };

        cap_remote_hubs(&mut desired, crate::lib::hum_guard::max_peers());

        // Drop connections whose hub is gone, or whose community set changed —
        // the latter reconnect below with the updated declared-interest list.
        connections.retain(|hub, (stop, communities)| {
            let keep = desired.get(hub).is_some_and(|set| set == communities);
            if !keep {
                stop.store(true, Ordering::SeqCst);
            }
            keep
        });

        for (hub, communities) in desired {
            if connections.contains_key(&hub) {
                continue;
            }
            let stop = Arc::new(AtomicBool::new(false));
            connections.insert(hub.clone(), (stop.clone(), communities.clone()));
            rocket::tokio::spawn(run_hub_connection(
                hub,
                communities,
                db.clone(),
                broadcast_tx.clone(),
                c2c_tx.clone(),
                stop,
            ));
        }
    }
}

/// Caps the desired remote-hub set at `max` hubs (`HUM_MAX_PEERS`), dropping the
/// excess deterministically (hubs sorted by DID, keep the first `max`) so the
/// kept set is stable across reconciles and doesn't flap. Presence for a dropped
/// hub's communities simply doesn't cross instances — a graceful degrade. `0`
/// means unbounded.
fn cap_remote_hubs(hubs: &mut HashMap<String, HashSet<String>>, max: usize) {
    if max == 0 || hubs.len() <= max {
        return;
    }
    let mut names: Vec<String> = hubs.keys().cloned().collect();
    names.sort();
    for hub in names.into_iter().skip(max) {
        hubs.remove(&hub);
        log::warn!("HUM_MAX_PEERS ({max}) reached; not connecting to hub {hub}");
    }
}

/// Maps each remote hub (i.e. not this AppView) to the community DIDs a
/// currently-online local user belongs to there. The community set is sent to
/// the hub as the declared-interest filter (Option A), so the hub streams only
/// those communities' Hums instead of everything it relays.
async fn desired_remote_hubs(
    db: &DatabaseConnection,
) -> Result<HashMap<String, HashSet<String>>, DbErr> {
    let me = appview_did();
    let online = user_states::Entity::find()
        .filter(user_states::Column::State.ne("offline"))
        .all(db)
        .await?;

    let mut hubs: HashMap<String, HashSet<String>> = HashMap::new();
    for user in online {
        let communities = match get_authorized_communities(db, &user.did).await {
            Ok(c) => c,
            Err(e) => {
                log::warn!("hub reconcile: communities for {}: {e}", user.did);
                continue;
            }
        };
        for c in communities {
            let community_did = c.community.did.clone();
            if let Ok(record) = serde_json::from_value::<ColibriCommunity>(c.community.data) {
                let hub = community_hub_did(&record);
                if hub != me {
                    hubs.entry(hub).or_default().insert(community_did);
                }
            }
        }
    }
    Ok(hubs)
}

/// Supervises one hub's `subscribeHums` connection: (re)connect with backoff and
/// inject received Hums locally until `stop` is set. `communities` is the
/// declared-interest set sent to the hub.
async fn run_hub_connection(
    hub_did: String,
    communities: HashSet<String>,
    db: DatabaseConnection,
    broadcast_tx: broadcast::Sender<SharedScopedEvent>,
    c2c_tx: broadcast::Sender<EventNotification>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        match connect_subscribe_hums(&hub_did, &communities).await {
            Ok(mut ws) => {
                log::info!("Humming egress connected to hub {hub_did}");
                loop {
                    let poll = sleep(Duration::from_secs(CONN_STOP_POLL_SECS));
                    rocket::tokio::select! {
                        msg = ws.next() => match msg {
                            Some(Ok(TungMessage::Text(text))) => {
                                match serde_json::from_str::<HumEnvelope>(&text) {
                                    Ok(envelope) => {
                                        let _ = ingest_trusted_hum(
                                            envelope,
                                            db.clone(),
                                            &broadcast_tx,
                                            &c2c_tx,
                                        )
                                        .await;
                                    }
                                    Err(e) => log::warn!("unparseable Hum from {hub_did}: {e}"),
                                }
                            }
                            Some(Ok(TungMessage::Close(_))) | None => break,
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                log::warn!("Hum stream from {hub_did} errored: {e}");
                                break;
                            }
                        },
                        _ = poll => {
                            if stop.load(Ordering::SeqCst) {
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => log::warn!("Humming egress connect to {hub_did} failed: {e}"),
        }

        if stop.load(Ordering::SeqCst) {
            return;
        }
        sleep(Duration::from_secs(RECONNECT_BACKOFF_SECS)).await;
    }
}

type HumWs = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<rocket::tokio::net::TcpStream>,
>;

/// Builds the `subscribeHums` URL, appending each declared community as a
/// repeated `communities=` query param (sorted for a stable, comparable URL). No
/// query when the set is empty. DIDs are query-safe (`:` and `.` are permitted in
/// the query component), so no escaping is needed.
fn build_subscribe_url(host: &str, communities: &HashSet<String>) -> String {
    let base = format!("wss://{host}/xrpc/social.colibri.sync.subscribeHums");
    if communities.is_empty() {
        return base;
    }
    let mut list: Vec<&String> = communities.iter().collect();
    list.sort();
    let query: Vec<String> = list.iter().map(|c| format!("communities={c}")).collect();
    format!("{base}?{}", query.join("&"))
}

/// Opens an authenticated `subscribeHums` WebSocket to a peer hub, offering a
/// freshly-minted peer token via the auth subprotocol and declaring the
/// communities of interest so the hub narrows what it streams.
async fn connect_subscribe_hums(
    hub_did: &str,
    communities: &HashSet<String>,
) -> Result<HumWs, String> {
    let host = did_web_host(hub_did).ok_or_else(|| format!("no host for {hub_did}"))?;
    let token = mint_appview_auth(hub_did, "social.colibri.sync.subscribeHums")
        .map_err(|e| e.to_string())?;

    let url = build_subscribe_url(&host, communities);
    let mut request = url.into_client_request().map_err(|e| e.to_string())?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        format!("{AUTH_SUBPROTOCOL}, {token}")
            .parse()
            .map_err(|_| String::from("invalid subprotocol header"))?,
    );

    let (ws, _response) = connect_async(request).await.map_err(|e| e.to_string())?;
    Ok(ws)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_web_host_decodes_port() {
        assert_eq!(
            did_web_host("did:web:api.colibri.social").as_deref(),
            Some("api.colibri.social")
        );
        assert_eq!(
            did_web_host("did:web:localhost%3A8080").as_deref(),
            Some("localhost:8080")
        );
        assert_eq!(did_web_host("did:plc:abc"), None);
    }

    #[test]
    fn build_subscribe_url_appends_sorted_communities() {
        let set: HashSet<String> = ["did:plc:b".to_string(), "did:plc:a".to_string()]
            .into_iter()
            .collect();
        assert_eq!(
            build_subscribe_url("h", &set),
            "wss://h/xrpc/social.colibri.sync.subscribeHums?communities=did:plc:a&communities=did:plc:b"
        );
        assert_eq!(
            build_subscribe_url("h", &HashSet::new()),
            "wss://h/xrpc/social.colibri.sync.subscribeHums"
        );
    }

    #[test]
    fn cap_remote_hubs_keeps_sorted_prefix() {
        let mut hubs: HashMap<String, HashSet<String>> = ["did:web:c", "did:web:a", "did:web:b"]
            .into_iter()
            .map(|h| (h.to_string(), HashSet::new()))
            .collect();

        cap_remote_hubs(&mut hubs, 2);
        assert_eq!(hubs.len(), 2);
        // Deterministic: the two lowest-sorted hubs survive.
        assert!(hubs.contains_key("did:web:a"));
        assert!(hubs.contains_key("did:web:b"));
        assert!(!hubs.contains_key("did:web:c"));
    }

    #[test]
    fn cap_remote_hubs_zero_is_unbounded() {
        let mut hubs: HashMap<String, HashSet<String>> = ["did:web:a", "did:web:b"]
            .into_iter()
            .map(|h| (h.to_string(), HashSet::new()))
            .collect();
        cap_remote_hubs(&mut hubs, 0);
        assert_eq!(hubs.len(), 2);
    }

    #[test]
    fn next_id_is_unique_and_prefixed() {
        let a = next_id("did:web:me");
        let b = next_id("did:web:me");
        assert_ne!(a, b);
        assert!(a.starts_with("did:web:me:"));
    }
}
