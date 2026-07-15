use crate::lib::at_uri::AtUri;
use crate::lib::author_cache::AuthorCache;
use crate::lib::channel_authz;
use crate::lib::colibri::{
    ColibriChannel, ColibriMembership, ColibriMessage, ColibriModeration, ColibriModerationSubject,
};
use crate::lib::community_authz;
use crate::lib::community_record::fetch_community_record;
use crate::lib::community_write;
use crate::lib::event_scope::{CommunityResolver, ScopedEvent, SharedScopedEvent};
use crate::lib::events::{
    CommunityCreationProgressEvent, HumEnvelope, MuteEvent, MuteEventData, SeenEvent, SeenEventData,
};
use crate::lib::hum_client::OutboundHum;
use crate::lib::map_tap_event::map_tap_event;
use crate::lib::moderation::{self, ACTION_BLOCKED_JOIN, MODERATION_NSID};
use crate::lib::notifications::{IndexedNotification, index_message_notifications};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data::{self, ActiveModel as RecordDataModel, Entity as RecordData};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use rocket::tokio::sync::{Notify, broadcast, mpsc};
use rocket::tokio::{net::TcpStream, sync::mpsc::Sender};
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::io::Error;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::client::IntoClientRequest,
};

const MEMBERSHIP_NSID: &str = "social.colibri.membership";

use crate::xrpc::social::colibri::sync::subscribe_events_handler::DidStruct;

pub type TapStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize, Debug, Clone)]
pub struct TapMessageRecord {
    #[allow(dead_code)]
    pub live: bool,
    pub did: String,
    #[allow(dead_code)]
    pub rev: String,
    pub collection: String,
    pub rkey: String,
    pub action: String,
    pub record: Option<Value>,
    #[allow(dead_code)]
    pub cid: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TapMessage {
    pub id: Number,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    pub message_type: String,
    pub record: Option<TapMessageRecord>,
}

#[derive(Serialize)]
pub struct TapAck {
    #[serde(rename = "type")]
    pub tap_type: String,
    pub id: Number,
}

/// Opens a new connection to the tap instance.
pub async fn connect_to_tap() -> Result<TapStream, Error> {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let credentials =
        base64::engine::general_purpose::STANDARD.encode(format!("admin:{tap_password}"));

    let mut request = format!("ws://{tap_hostname}/channel")
        .into_client_request()
        .expect("Failed to build request");

    request.headers_mut().insert(
        "Authorization",
        format!("Basic {credentials}").parse().unwrap(),
    );

    let (ws_stream, _response) = connect_async(request).await.map_err(Error::other)?;

    log::info!("Connected to Tap!");

    Ok(ws_stream)
}

/// Sends DIDs to Tap to register for backfilling.
pub async fn register_dids(dids: Vec<String>) {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let client = reqwest::Client::new();

    let did_struct = DidStruct { dids };

    let res = client
        .post(format!("http://{tap_hostname}/repos/add"))
        .basic_auth(String::from("admin"), Some(tap_password))
        .json(&did_struct)
        .send()
        .await;

    if let Err(res_err) = res {
        log::error!("Unable to add DIDs: {}", res_err);
    } else {
        log::info!("Now tracking DIDs: {}", &did_struct.dids.join(", "));
    }
}

/// Acknowledges a message from Tap and saves the data in the database.
///
/// For `delete` events the local `record_data` row is removed for any NSID,
/// keeping the cache consistent with the source repo.
///
/// Returns `true` only when an ack was actually enqueued to the writer. A
/// database persist failure returns `false` *without* acking, so the event
/// stays in tap's outbox and is redelivered for another attempt (tap uses
/// at-least-once delivery — an acked event is deleted upstream). Callers use
/// this to decide whether the event is genuinely done or must be retried.
pub async fn ack_tap_msg(
    db: &DatabaseConnection,
    to_tap: &mut Sender<String>,
    text: String,
    save_in_db: bool,
) -> bool {
    let msg_data: TapMessage = serde_json::from_str(text.as_str()).unwrap();

    if let Some(safe_record) = msg_data.record
        && save_in_db
    {
        if safe_record.action == "delete" {
            let delete_result = RecordData::delete_many()
                .filter(record_data::Column::Did.eq(&safe_record.did))
                .filter(record_data::Column::Nsid.eq(&safe_record.collection))
                .filter(record_data::Column::Rkey.eq(&safe_record.rkey))
                .exec(db)
                .await;

            if let Err(res) = delete_result {
                log::error!("Unable to delete record from database: {}", res);
                return false;
            }
        } else {
            let json_data: Value = serde_json::to_value(&safe_record.record).unwrap();

            let insert_result = RecordData::insert(RecordDataModel {
                data: ActiveValue::Set(json_data),
                did: ActiveValue::Set(safe_record.did),
                nsid: ActiveValue::Set(safe_record.collection),
                rkey: ActiveValue::Set(safe_record.rkey),
                indexed_at: ActiveValue::Set(current_iso8601_utc()),
                ..Default::default()
            })
            .on_conflict(
                sea_query::OnConflict::columns([
                    record_data::Column::Did,
                    record_data::Column::Nsid,
                    record_data::Column::Rkey,
                ])
                .update_column(record_data::Column::Data)
                .to_owned(),
            )
            .exec(db)
            .await;

            if let Err(res) = insert_result {
                log::error!("Unable to save record in database: {}", res);

                return false;
            }
        }
    }

    let ack = TapAck {
        tap_type: String::from("ack"),
        id: msg_data.id.clone(),
    };

    let serialized_ack = serde_json::to_string(&ack).unwrap();

    let ack_res = to_tap.send(serialized_ack).await;

    if let Err(res_err) = ack_res {
        // The writer task's receiver is gone — the connection is tearing down.
        // Report failure so the caller doesn't record the event as done; tap
        // will redeliver it on the next connection.
        log::error!(
            "Unable to acknowledge event with ID {}: {}",
            msg_data.id,
            res_err
        );
        false
    } else {
        // NB: this only means the ack was queued to the writer task, not that it
        // reached tap. The writer transmits it; if that fails the writer tears
        // down the connection (see `run_connection`).
        log::debug!("Queued ack for tap event with ID {}", msg_data.id);
        true
    }
}

/// Bundles the cross-task channels the AppView uses for tap-derived events.
/// Stored in Rocket state so handlers can subscribe to the broadcasts.
pub struct CommsBridge {
    /// Fan-out of pre-mapped, scope-tagged server events derived from tap
    /// records. Each record is mapped and enriched exactly once in
    /// `run_connection`; every WS subscriber holds its own `Receiver` and
    /// forwards only the events whose [`EventScope`] matches its user.
    ///
    /// [`EventScope`]: crate::lib::event_scope::EventScope
    pub broadcast: broadcast::Sender<SharedScopedEvent>,
    /// Fan-out of plain-data voice-moderation commands
    pub voice_control: broadcast::Sender<crate::lib::voice_control::VoiceControlCommand>,
    /// Fan-out of notifications indexed inside the tap pipeline. Each WS
    /// subscriber filters by recipient DID.
    pub notifications: broadcast::Sender<IndexedNotification>,
    /// Fan-out of `application_event`s emitted directly by REST handlers
    /// (`approveMembership`, `kickUser`/`kick`, `dismissApplication`,
    /// `undismissApplication`) rather than derived from a single tap record.
    /// Bypasses `map_tap_event` entirely — the handler already has full
    /// context (and, for `approveMembership`, has just performed the write),
    /// so there's no need to round-trip through the firehose to know what
    /// happened. Every WS subscriber forwards these as-is; clients scope by
    /// `community`.
    pub applications: broadcast::Sender<crate::lib::events::ColibriServerEvent>,
    /// Fan-out of `seen_event`s — per-user read-state changes pushed to the
    /// originating user's other connected clients (each subscriber filters by
    /// `recipient_did`). `channel_read` events are emitted from the tap loop
    /// when a `social.colibri.channel.read` record is indexed; `message_seen`
    /// events are emitted by the `updateSeenForMessage` handler.
    pub seen: broadcast::Sender<SeenEvent>,
    /// Fan-out of `mute_event`s — per-user mute/unmute changes pushed to the
    /// originating user's other connected clients (each subscriber filters by
    /// `recipient_did`). Emitted from the tap loop when a
    /// `social.colibri.actor.mute` record is created or deleted.
    pub mute: broadcast::Sender<MuteEvent>,
    /// Fan-out of `community_creation_progress` events — per-user progress
    /// hints pushed to the creating user's own connections (each subscriber
    /// filters by `recipient_did`). Emitted by `community.create` while
    /// bootstrapping a BYO community on a (possibly slow) external PDS.
    pub progress: broadcast::Sender<CommunityCreationProgressEvent>,
    /// Fan-out of off-protocol Hums this AppView relays in its hub role. The
    /// `sendHum` handler injects an inbound Hum locally and, when its `ttl`
    /// permits, republishes it here; every connected `subscribeHums` peer stream
    /// forwards it onward. Not derived from tap — Hums are the AppView-to-AppView
    /// presence channel.
    pub hums: broadcast::Sender<HumEnvelope>,
    /// Outbound queue for local off-protocol changes (presence/typing/voice) the
    /// Humming manager propagates to each community's hub. When Humming is
    /// disabled the manager task is never spawned, so nothing drains this and
    /// `hum_client::enqueue` no-ops.
    pub hum_outbox: mpsc::Sender<OutboundHum>,
}

/// Runs the tap-connection event loop:
///
/// 1. Forwards outbound text from `rx_outbound` upstream over the WS socket.
/// 2. Reads inbound records from the WS socket; for `social.colibri.message`
///    records that aren't deletes, indexes notifications centrally (so each
///    record produces notification rows exactly once regardless of how many
///    subscribers are connected).
/// 3. Broadcasts every inbound record on `tx_inbound` for the
///    `subscribeEvents` WS handler to consume.
/// 4. Acknowledges the message back to tap.
///
/// This is the only place the AppView consumes tap data; subscriber WS
/// connections read from `tx_inbound` and `notifications_tx` exclusively.
///
/// ## Concurrency model
///
/// Tap delivers historical/backfill events (`live: false`) concurrently across
/// repos, while live events (`live: true`) are strict one-in-flight barriers.
/// To exploit the backfill concurrency without reordering anything tap considers
/// ordered, the connection is structured as:
///
/// * a **dispatcher** (this function's main loop) that reads the socket and
///   routes each record to a shard worker keyed by its full identity
///   (`did` + `collection` + `rkey`),
/// * `N` **shard workers**, each draining its own bounded queue in FIFO order
///   and running the full per-event pipeline ([`process_event`]) — so every
///   event for a given `(did, collection, rkey)` is processed in order on a
///   single worker, while distinct records (even within one repo's backfill)
///   process in parallel,
/// * a **writer task** that owns the socket's send half and forwards acks
///   (produced by the workers via `to_tap`) upstream, independently of the
///   read/route path so a full shard queue can never deadlock the ack path.
///
/// Returns when the socket closes or errors; the caller is responsible for
/// reconnecting.
pub async fn run_connection(
    db: DatabaseConnection,
    socket: TapStream,
    tx_inbound: broadcast::Sender<SharedScopedEvent>,
    notifications_tx: broadcast::Sender<IndexedNotification>,
    seen_tx: broadcast::Sender<SeenEvent>,
    mute_tx: broadcast::Sender<MuteEvent>,
) {
    // Caches channel/message -> community DID across the connection's lifetime
    // so per-event community resolution rarely touches the database. Shared
    // across all workers (its internal maps are Mutex-guarded and append-only).
    let resolver = std::sync::Arc::new(CommunityResolver::new());

    // Caches per-author profile enrichment so a single repo's backfill (every
    // message by the same author) does the profile/actor.data lookups once
    // instead of per message. Invalidated when a profile/actor.data record flows
    // through (see `process_event`). Shared across all workers.
    let author_cache = std::sync::Arc::new(AuthorCache::new());

    // Dedup against tap's at-least-once redelivery. Tap re-sends any event it
    // hasn't seen an ack for within `TAP_RETRY_TIMEOUT` (default 60s), reusing
    // the same event id; without this a redelivery re-runs the whole pipeline
    // (re-indexing notifications, re-broadcasting, re-writing member records)
    // and, because it re-hashes to the same shard, piles onto an already-backed-
    // up queue during backfill and amplifies the backlog. Shared across workers
    // and the dispatcher. Per-connection: a reconnect starts fresh (tap resends
    // unacked events and we process them once), which is fine.
    let dedup = std::sync::Arc::new(TapDedup::new());

    let worker_count = tap_worker_count();

    // Outbound ack channel. Acks are produced inside the workers and drained to
    // the socket by the writer task below. Created per connection so reconnects
    // start with a clean channel.
    let (to_tap, mut rx_outbound) = mpsc::channel::<String>(OUTBOUND_QUEUE_CAP);

    let (mut sink, mut stream) = socket.split();

    // Signalled when the writer task can no longer transmit. Acks flow through
    // the writer, so if it dies the dispatcher must stop too — otherwise it
    // keeps reading and "acking" into a channel nothing drains, black-holing
    // every ack while tap redelivers forever. The dispatcher selects on this.
    let writer_dead = std::sync::Arc::new(Notify::new());

    // Writer task: drains acks to the socket independently of socket reads.
    let writer = {
        let writer_dead = writer_dead.clone();
        rocket::tokio::spawn(async move {
            while let Some(msg) = rx_outbound.recv().await {
                if let Err(e) = sink.send(TungMessage::Text(msg.into())).await {
                    // A failed send means the socket's write half is gone;
                    // signal the dispatcher to tear down so the supervisor
                    // reconnects (and tap resends the unacked backlog) instead
                    // of silently dropping every subsequent ack.
                    log::error!("tap ack writer send failed; tearing down connection: {e}");
                    writer_dead.notify_one();
                    break;
                }
            }
        })
    };

    // Spawn the shard workers. Each owns a bounded queue and processes its
    // events sequentially; the dispatcher routes by `hash(did) % worker_count`.
    let mut shard_txs: Vec<mpsc::Sender<WorkItem>> = Vec::with_capacity(worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, mut rx) = mpsc::channel::<WorkItem>(WORKER_QUEUE_CAP);
        shard_txs.push(tx);

        let db = db.clone();
        let resolver = resolver.clone();
        let author_cache = author_cache.clone();
        let mut to_tap = to_tap.clone();
        let tx_inbound = tx_inbound.clone();
        let notifications_tx = notifications_tx.clone();
        let seen_tx = seen_tx.clone();
        let mute_tx = mute_tx.clone();
        let dedup = dedup.clone();

        worker_handles.push(rocket::tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                let acked = process_event(
                    item.record,
                    item.text,
                    &db,
                    &resolver,
                    &author_cache,
                    &mut to_tap,
                    &tx_inbound,
                    &notifications_tx,
                    &seen_tx,
                    &mute_tx,
                )
                .await;

                // Update dedup state now the event is done. On success mark it
                // completed so a redelivery is re-acked without reprocessing; on
                // failure (no ack sent) just release the in-flight mark so tap's
                // redelivery is treated as new and retried from scratch.
                if let Some(id) = item.id {
                    if acked {
                        dedup.complete(id);
                    } else {
                        dedup.release(id);
                    }
                }
            }
        }));
    }

    // The dispatcher keeps one ack sender for record-less events, which need an
    // ack but no per-shard processing.
    let mut dispatcher_to_tap = to_tap.clone();
    // Drop our extra reference so the workers' clones are the only senders left;
    // once the dispatcher loop ends and the workers finish, the writer's
    // `rx_outbound.recv()` resolves to `None` and it exits cleanly.
    drop(to_tap);

    // Dispatcher: read the socket and route records to shard workers. Stops if
    // the writer dies (acks would otherwise be black-holed) or the socket ends.
    loop {
        let msg = rocket::tokio::select! {
            biased;
            _ = writer_dead.notified() => {
                log::warn!("tap ack writer ended; ending dispatcher to reconnect");
                break;
            }
            msg = stream.next() => msg,
        };

        let Some(msg) = msg else { break };

        let text = match msg {
            Ok(TungMessage::Text(text)) => text.to_string(),
            // Ping/pong/binary/close frames carry no records; ignore non-text
            // and stop on close.
            Ok(TungMessage::Close(_)) => break,
            Ok(_) => continue,
            Err(e) => {
                log::warn!("tap socket read error: {e}");
                break;
            }
        };

        let tap_msg = match serde_json::from_str::<TapMessage>(&text) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("Received unparseable event from tap: {e}; payload={text}");
                continue;
            }
        };

        // Tap event ids are monotonic `uint`s reused across redeliveries. Track
        // by id to drop/handle duplicates below; if it somehow isn't an integer
        // we skip dedup and process it (correctness over dedup).
        let event_id = tap_msg.id.as_u64();

        let Some(record) = tap_msg.record else {
            // Event does not carry a record — ack immediately, no DB work.
            ack_tap_msg(&db, &mut dispatcher_to_tap, text, false).await;
            continue;
        };

        // Deduplicate tap's at-least-once redelivery before doing any work.
        if let Some(id) = event_id {
            match dedup.admit(id) {
                Admit::New => {}
                Admit::InFlight => {
                    // The original delivery is still queued/processing and will
                    // ack it; drop this copy so it doesn't re-enter the shard
                    // queue and amplify the backlog.
                    log::debug!("dropping redelivered in-flight tap event ID {id}");
                    continue;
                }
                Admit::AlreadyDone => {
                    // Already processed and acked once, but tap redelivered
                    // (our earlier ack was lost or slow). Re-ack without
                    // reprocessing or re-writing the cache.
                    log::debug!("re-acking already-processed tap event ID {id}");
                    ack_tap_msg(&db, &mut dispatcher_to_tap, text, false).await;
                    continue;
                }
            }
        }

        let shard = shard_for(&record.did, &record.collection, &record.rkey, worker_count);

        log::debug!("Processing Tap event ID {} on shard {}", tap_msg.id, shard);

        if shard_txs[shard]
            .send(WorkItem {
                record,
                text,
                id: event_id,
            })
            .await
            .is_err()
        {
            log::error!("tap shard worker {shard} stopped; ending dispatcher");
            break;
        }
    }

    // Socket closed or errored: drop the shard senders so each worker drains
    // its remaining queue and exits, then wait for them and the writer to wind
    // down before returning (so the caller can reconnect cleanly).
    drop(shard_txs);
    drop(dispatcher_to_tap);
    for handle in worker_handles {
        let _ = handle.await;
    }
    let _ = writer.await;
}

/// A single tap record routed to a shard worker. Carries both the parsed
/// `record` (for enrichment) and the original `text` (which [`ack_tap_msg`]
/// re-serializes for the cache write + ack). `id` is the tap event id (when it
/// parsed as an integer) so the worker can update [`TapDedup`] once done.
struct WorkItem {
    record: TapMessageRecord,
    text: String,
    id: Option<u64>,
}

/// Outcome of admitting a tap event id into [`TapDedup`].
enum Admit {
    /// First time we've seen this id (now marked in-flight) — process it.
    New,
    /// A redelivery of an event still queued/processing — drop it; the original
    /// delivery will ack it.
    InFlight,
    /// A redelivery of an event already processed and acked — re-ack it without
    /// reprocessing.
    AlreadyDone,
}

/// Upper bound on remembered completed event ids. Redeliveries arrive roughly
/// `TAP_RETRY_TIMEOUT` apart, so this only needs to cover the ids seen within a
/// backfill burst window; ids are monotonic, so one old enough to fall out of
/// the window is old enough to forget. Overridable via `TAP_DEDUP_CAPACITY`.
const DEFAULT_DEDUP_CAPACITY: usize = 16384;

fn dedup_capacity() -> usize {
    std::env::var("TAP_DEDUP_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_DEDUP_CAPACITY)
}

/// Guards against tap's at-least-once redelivery. Tracks the ids currently
/// in-flight (queued or processing) and a capacity-bounded FIFO of recently
/// completed ids. Shared across the dispatcher and all shard workers; its short
/// critical sections make a plain `Mutex` the right fit.
struct TapDedup {
    inner: std::sync::Mutex<TapDedupInner>,
}

struct TapDedupInner {
    capacity: usize,
    /// Ids handed to a worker but not yet finished.
    inflight: std::collections::HashSet<u64>,
    /// Recently completed ids, in insertion order for eviction.
    done_order: std::collections::VecDeque<u64>,
    done_ids: std::collections::HashSet<u64>,
}

impl TapDedup {
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(TapDedupInner {
                capacity: dedup_capacity(),
                inflight: std::collections::HashSet::new(),
                done_order: std::collections::VecDeque::new(),
                done_ids: std::collections::HashSet::new(),
            }),
        }
    }

    /// Decides how to handle an incoming event id, marking it in-flight when new.
    fn admit(&self, id: u64) -> Admit {
        let mut g = self.inner.lock().unwrap();
        if g.done_ids.contains(&id) {
            Admit::AlreadyDone
        } else if !g.inflight.insert(id) {
            Admit::InFlight
        } else {
            Admit::New
        }
    }

    /// Marks an in-flight id as fully processed and acked: moves it into the
    /// bounded completed set so redeliveries are re-acked rather than reprocessed.
    fn complete(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        g.inflight.remove(&id);
        if g.done_ids.insert(id) {
            g.done_order.push_back(id);
            while g.done_order.len() > g.capacity
                && let Some(old) = g.done_order.pop_front()
            {
                g.done_ids.remove(&old);
            }
        }
    }

    /// Releases an in-flight id that failed to process (no ack sent) without
    /// recording it as done, so tap's redelivery is retried from scratch.
    fn release(&self, id: u64) {
        self.inner.lock().unwrap().inflight.remove(&id);
    }
}

/// Outbound ack channel capacity. Acks are small and drained promptly by the
/// writer task; this only needs to absorb bursts from all workers at once.
const OUTBOUND_QUEUE_CAP: usize = 256;

/// Per-shard work queue capacity. A full queue back-pressures the dispatcher
/// (which back-pressures tap), bounding memory under a backlog.
const WORKER_QUEUE_CAP: usize = 64;

/// Number of shard workers. Configurable via `TAP_WORKERS`; defaults to 8 and
/// is clamped to at least 1. Keep it comfortably under the DB pool size so
/// workers don't starve each other on connections.
fn tap_worker_count() -> usize {
    std::env::var("TAP_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(8)
}

/// Maps a record's full identity (`did` + `collection` + `rkey`) to a shard
/// index in `[0, worker_count)`. Deterministic for a given record, so every
/// event for the same `(did, collection, rkey)` — i.e. a record's create →
/// update → delete lifecycle — lands on the same worker and is processed in
/// order. That per-record ordering is the only invariant the cache relies on
/// (delete reads the row a prior create wrote); records with different rkeys are
/// independent and may process concurrently.
///
/// Hashing the full identity rather than just the DID lets a *single* repo's
/// backfill fan out across all workers (a backfill is dominated by one DID, so
/// DID-only sharding would pin it to one worker and leave the rest idle). The
/// trade-off: events within a repo may now be processed/acked out of order. We
/// only ever required per-record order, not per-repo, so this is safe; clients
/// already reconcile by URI and sort by `createdAt`.
fn shard_for(did: &str, collection: &str, rkey: &str, worker_count: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    did.hash(&mut hasher);
    collection.hash(&mut hasher);
    rkey.hash(&mut hasher);
    (hasher.finish() % worker_count.max(1) as u64) as usize
}

/// Runs the full per-event pipeline for a single tap record and acks it. This
/// is the body each shard worker runs in FIFO order; because a repo's events
/// all route to one worker, the read-before-delete ordering the cache relies on
/// (member/reaction/membership/mute deletes read the cached row before
/// `ack_tap_msg` removes it) holds exactly as in the original serial loop.
///
/// Steps, in order: channel-post authz gate → durable notification indexing
/// (+ background Web Push) → membership/seen/mute side-effects → `map_tap_event`
/// broadcast → `ack_tap_msg` (cache write + ack). Notifications are indexed
/// before the ack so a crash never drops a durable notification for an acked
/// event.
///
/// Returns whether the event was acked (see [`ack_tap_msg`]): `true` when it's
/// genuinely done, `false` when a persist failure left it for tap to redeliver.
fn channel_rkey(channel: &str) -> String {
    AtUri::parse(channel)
        .map(|u| u.rkey)
        .unwrap_or_else(|| channel.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn process_event(
    record: TapMessageRecord,
    text: String,
    db: &DatabaseConnection,
    resolver: &CommunityResolver,
    author_cache: &AuthorCache,
    to_tap: &mut mpsc::Sender<String>,
    tx_inbound: &broadcast::Sender<SharedScopedEvent>,
    notifications_tx: &broadcast::Sender<IndexedNotification>,
    seen_tx: &broadcast::Sender<SeenEvent>,
    mute_tx: &broadcast::Sender<MuteEvent>,
) -> bool {
    // Author enrichment invalidation: a change to the author's Bluesky profile,
    // Colibri profile, or actor.data must drop their cached enrichment so
    // subsequent messages pick up the new values. The Colibri profile feeds the
    // effective-profile resolution (display name / avatar / theme / syncBluesky),
    // so it invalidates here too. The record's `did` is the author who changed.
    if record.collection == "app.bsky.actor.profile"
        || record.collection == "social.colibri.actor.profile"
        || record.collection == "social.colibri.actor.data"
    {
        author_cache.invalidate(&record.did);
    }

    // Channel post restrictions: reject (don't index, don't notify, don't
    // broadcast) messages from authors not permitted to post in the target
    // channel. Fails open on any inconclusive lookup (channel not yet cached,
    // DB error, malformed JSON) — only a successfully loaded channel + authz
    // pair that conclusively disallows the author causes a reject.
    //
    // The per-actor authz load (`load_actor_authz`) is the expensive part — a
    // member-table query plus a roles query per message — so it runs ONLY when
    // it can actually change the outcome: the channel is genuinely restricted
    // AND the author isn't the community owner. An unrestricted channel allows
    // everyone and the owner can always post (see `channel_authz::can_post`), so
    // for those cases — the hot path for the firehose — we skip the authz
    // queries entirely and only pay the cheap indexed channel lookup.
    if record.collection == "social.colibri.message"
        && record.action != "delete"
        && let Some(payload) = record.record.as_ref()
        && let Ok(message) = serde_json::from_value::<ColibriMessage>(payload.clone())
        && let Some(community_did) =
            resolver.community_for_channel(db, &channel_rkey(&message.channel)).await
        // Owner may always post; skip the channel + authz reads.
        && record.did != community_did
        && let Ok(Some(chan_json)) = community_write::read_cached(
            db,
            &community_did,
            "social.colibri.channel",
            &channel_rkey(&message.channel),
        )
        .await
        && let Ok(channel) = serde_json::from_value::<ColibriChannel>(chan_json)
        // Unrestricted channels allow everyone — don't load authz.
        && (channel.owner_only == Some(true)
            || !channel.allowed_roles.is_empty()
            || !channel.allowed_members.is_empty())
    {
        let community_uri = format!("at://{community_did}/social.colibri.community/self");
        match community_authz::load_actor_authz(db, &community_uri, &record.did).await {
            Ok(authz) if !channel_authz::can_post(&channel, &authz, &record.did) => {
                log::info!(
                    "rejecting message {}/{} into restricted channel {}: author not permitted",
                    record.did,
                    record.rkey,
                    message.channel
                );
                return ack_tap_msg(db, to_tap, text, false).await;
            }
            Ok(_) => {}
            Err(e) => log::warn!(
                "authz lookup failed for {}: {e}; allowing message through",
                record.did
            ),
        }
    }

    // Centralized notification indexing for newly authored Colibri messages.
    // Persists rows + broadcasts to live WS subscribers, so each notification is
    // written exactly once regardless of subscriber count.
    if record.collection == "social.colibri.message"
        && record.action != "delete"
        && let Some(payload) = record.record.clone()
        && let Ok(message) = serde_json::from_value::<ColibriMessage>(payload)
    {
        let message_uri = format!("at://{}/{}/{}", record.did, record.collection, record.rkey);
        match index_message_notifications(db, &record.did, &message_uri, &message).await {
            Ok(rows) => {
                for row in rows {
                    // Background Web Push (closed-app delivery) runs off-thread
                    // so its network I/O never stalls the worker. It applies its
                    // own DND + mute filtering. The live WS broadcast below is
                    // the foreground path.
                    let push_db = db.clone();
                    let push_row = row.clone();
                    rocket::tokio::spawn(async move {
                        crate::lib::push_send::deliver(push_db, push_row).await;
                    });

                    let _ = notifications_tx.send(row);
                }
            }
            Err(e) => {
                log::error!("notification indexing failed for {message_uri}: {e}");
            }
        }
    }

    // Self-leave detection: when a user deletes their own
    // `social.colibri.membership`, drop their community-side
    // `social.colibri.member` record. Runs before `ack_tap_msg` so the cached
    // membership row (which holds the community URI) is still present to read.
    if record.collection == MEMBERSHIP_NSID
        && record.action == "delete"
        && let Err(e) = revoke_member_for_leave(db, &record).await
    {
        log::error!(
            "member revoke on leave failed for {}/{}: {}",
            record.did,
            record.rkey,
            e
        );
    }

    // Auto-join on membership-create. For open communities the AppView writes
    // the matching `social.colibri.member` record on the community's PDS. Closed
    // communities sit pending until a moderator hits `approveMembership`. Banned
    // users get a `blockedJoin` moderation entry for the audit trail and no
    // member record. Legacy communities (rkey != "self" on the community record)
    // are not auto-joinable — we have no community-side credentials for them.
    if record.collection == MEMBERSHIP_NSID
        && record.action != "delete"
        && let Err(e) = process_membership_create(db, &record).await
    {
        log::error!(
            "membership-create processing failed for {}/{}: {}",
            record.did,
            record.rkey,
            e
        );
    }

    // Cross-device read-state sync: when a user's read cursor advances (a
    // `social.colibri.channel.read` record is indexed), tell their other
    // connected clients to clear that channel's white "unread messages" dot.
    if record.collection == "social.colibri.channel.read"
        && record.action != "delete"
        && let Some(payload) = record.record.as_ref()
        && let Some(channel) = payload.get("channel").and_then(|c| c.as_str())
    {
        let _ = seen_tx.send(SeenEvent {
            recipient_did: record.did.clone(),
            data: SeenEventData {
                event: String::from("channel_read"),
                channel_uri: channel.to_string(),
                message_uri: None,
                cleared: None,
            },
        });
    }

    // Cross-device mute sync: when a user mutes or unmutes a channel/community
    // (a `social.colibri.actor.mute` record is created or deleted), tell their
    // other connected clients so their mute set stays current without a reload.
    // On delete the record body is gone, so read the `subject` from the cached
    // row before `ack_tap_msg` removes it.
    if record.collection == "social.colibri.actor.mute" {
        let subject = match record.action.as_str() {
            "delete" => mute_subject_from_cache(db, &record).await,
            _ => record
                .record
                .as_ref()
                .and_then(|p| p.get("subject"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string()),
        };

        if let Some(subject) = subject {
            let event = if record.action == "delete" {
                "unmuted"
            } else {
                "muted"
            };
            let _ = mute_tx.send(MuteEvent {
                recipient_did: record.did.clone(),
                data: MuteEventData {
                    event: String::from(event),
                    subject,
                },
            });
        }
    }

    // Map the record into scope-tagged events and fan them out exactly once —
    // every connected subscriber then only filters by scope rather than
    // re-mapping/enriching. This runs BEFORE `ack_tap_msg`, which deletes the
    // cached row for delete actions that the mapper still needs to read
    // (member/reaction deletes, message community resolution).
    match map_tap_event(&record, db, resolver, author_cache).await {
        Ok(events) => {
            for (event, scope) in events {
                let scoped = std::sync::Arc::new(ScopedEvent {
                    scope,
                    payload: event.serialize(),
                });
                let _ = tx_inbound.send(scoped);
            }
        }
        Err(e) => {
            let msg = e.to_string();
            // `Facet` is an expected, intentionally-unhandled collection; don't
            // spam the log for it.
            if msg != "Facet" {
                log::error!("Unable to map tap record {:?}: {}", record, msg);
            }
        }
    }

    ack_tap_msg(db, to_tap, text, true).await
}

/// Processes a `social.colibri.membership` create event. Parses the payload,
/// resolves the community record (cache → PDS fallback), refuses legacy
/// communities, records a `blockedJoin` audit entry for banned users, and
/// auto-writes a `social.colibri.member` record on the community's PDS for
/// open communities. Closed communities are a no-op — moderators advance them
/// via the `approveMembership` endpoint.
async fn process_membership_create(
    db: &DatabaseConnection,
    record: &TapMessageRecord,
) -> Result<(), DbErr> {
    let payload = match record.record.as_ref() {
        Some(v) => v,
        None => {
            log::warn!(
                "membership-create event from {}/{} carried no payload; skipping",
                record.did,
                record.rkey
            );
            return Ok(());
        }
    };
    let membership = match serde_json::from_value::<ColibriMembership>(payload.clone()) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "membership-create payload from {}/{} failed to parse: {e}",
                record.did,
                record.rkey
            );
            return Ok(());
        }
    };

    let Some(community_uri) = AtUri::parse(&membership.community) else {
        log::warn!(
            "membership from {}/{} has malformed community URI {:?}; skipping",
            record.did,
            record.rkey,
            membership.community
        );
        return Ok(());
    };
    let community_did = community_uri.authority.clone();
    let community_rkey = community_uri.rkey.clone();

    // Legacy communities live as one of many records on a user repo with a
    // TID rkey; AppView holds no credentials for those PDSes, so it can't
    // write the community-side member record. By design — these communities
    // are read-only as of the Variant A cutover.
    if community_rkey != "self" {
        log::info!(
            "ignoring membership from {} to legacy community {}/{} (rkey != \"self\")",
            record.did,
            community_did,
            community_rkey
        );
        return Ok(());
    }

    let community = match fetch_community_record(db, &community_did, &community_rkey).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            log::warn!(
                "membership from {} references unknown community {}/{}; skipping",
                record.did,
                community_did,
                community_rkey
            );
            return Ok(());
        }
        Err(e) => {
            log::error!(
                "community fetch failed for membership {}/{}: {e}",
                record.did,
                record.rkey
            );
            return Ok(());
        }
    };

    if moderation::is_user_banned(db, &community_did, &record.did).await? {
        log::info!(
            "refusing membership-create for banned user {} in {}; writing blockedJoin audit",
            record.did,
            community_did
        );
        let audit = ColibriModeration {
            record_type: Some(MODERATION_NSID.to_string()),
            action: ACTION_BLOCKED_JOIN.to_string(),
            subject: ColibriModerationSubject {
                did: Some(record.did.clone()),
                uri: None,
            },
            reason: None,
            created_by: community_did.clone(),
            created_at: current_iso8601_utc(),
        };
        if let Err(e) = moderation::write_moderation_record(db, &community_uri, &audit).await {
            log::error!(
                "blockedJoin audit write failed for {} in {}: {e}",
                record.did,
                community_did
            );
        }
        return Ok(());
    }

    if community.requires_approval_to_join {
        log::debug!(
            "membership from {} is pending approval for closed community {}",
            record.did,
            community_did
        );
        return Ok(());
    }

    let membership_uri = format!("at://{}/{}/{}", record.did, record.collection, record.rkey);
    match moderation::write_member_record(
        db,
        &community_did,
        &record.did,
        vec![],
        Some(membership_uri),
    )
    .await
    {
        Ok(Some(_)) => {
            log::info!(
                "auto-admitted {} to open community {}",
                record.did,
                community_did
            );
        }
        Ok(None) => {
            log::debug!(
                "membership-create from {} for {} hit existing member record (idempotent skip)",
                record.did,
                community_did
            );
        }
        Err(e) => {
            log::error!(
                "auto-admit write_member_record failed for {} in {}: {e}",
                record.did,
                community_did
            );
        }
    }

    Ok(())
}

/// Looks up the cached `social.colibri.membership` row for the deleted record,
/// resolves the community DID from its payload, and revokes the community-side
/// `social.colibri.member` record. Returns `Ok(())` when the cached record is
/// missing or malformed (best-effort — nothing to revoke).
async fn revoke_member_for_leave(
    db: &DatabaseConnection,
    record: &TapMessageRecord,
) -> Result<(), DbErr> {
    let old = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&record.did))
        .filter(record_data::Column::Nsid.eq(MEMBERSHIP_NSID))
        .filter(record_data::Column::Rkey.eq(&record.rkey))
        .one(db)
        .await?;
    let Some(row) = old else {
        return Ok(());
    };
    let Ok(membership) = serde_json::from_value::<ColibriMembership>(row.data) else {
        return Ok(());
    };
    let Some(community) = AtUri::parse(&membership.community) else {
        return Ok(());
    };

    moderation::revoke_community_member(db, &community.authority, &record.did).await?;
    Ok(())
}

/// Reads the `subject` of a just-deleted `social.colibri.actor.mute` record from
/// the local cache. The delete event carries no body, so the cached row (still
/// present until `ack_tap_msg` removes it) is the only source of the subject.
/// Returns `None` when the row is missing or malformed.
async fn mute_subject_from_cache(
    db: &DatabaseConnection,
    record: &TapMessageRecord,
) -> Option<String> {
    let row = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&record.did))
        .filter(record_data::Column::Nsid.eq("social.colibri.actor.mute"))
        .filter(record_data::Column::Rkey.eq(&record.rkey))
        .one(db)
        .await
        .ok()??;
    row.data
        .get("subject")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use rocket::tokio::sync::{broadcast, mpsc};
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

    #[tokio::test]
    async fn sends_ack_for_non_record_messages() {
        let db = mock_db();
        let (mut tx, mut rx) = mpsc::channel::<String>(1);

        ack_tap_msg(
            &db,
            &mut tx,
            String::from(r#"{"id":1,"type":"heartbeat","record":null}"#),
            true,
        )
        .await;

        let ack = rx.recv().await.unwrap();
        let ack_json: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack_json["type"], "ack");
        assert_eq!(ack_json["id"], 1);
    }

    #[test]
    fn channel_rkey_extracts_rkey_from_full_at_uri() {
        assert_eq!(
            channel_rkey("at://did:plc:owner/social.colibri.channel/chan-a"),
            "chan-a"
        );
    }

    #[test]
    fn channel_rkey_passes_through_bare_rkey_unchanged() {
        assert_eq!(channel_rkey("chan-a"), "chan-a");
    }

    #[test]
    fn shard_for_is_deterministic_and_in_range() {
        for n in [1usize, 2, 8, 16] {
            let first = shard_for("did:plc:alice", "social.colibri.message", "r1", n);
            let second = shard_for("did:plc:alice", "social.colibri.message", "r1", n);
            assert_eq!(
                first, second,
                "same record identity must always map to the same shard"
            );
            assert!(first < n, "shard index {first} must be < worker_count {n}");
        }
        // A single worker collapses everything onto shard 0.
        assert_eq!(
            shard_for("did:plc:whoever", "social.colibri.message", "r1", 1),
            0
        );
    }

    #[test]
    fn shard_for_spreads_one_repos_records() {
        // The point of hashing the full record identity: a single repo's
        // backfill (one DID, many rkeys) must fan out across workers rather
        // than pinning to a single shard.
        let n = 8;
        let mut seen = std::collections::HashSet::new();
        for i in 0..200 {
            seen.insert(shard_for(
                "did:plc:one-repo",
                "social.colibri.message",
                &format!("r{i}"),
                n,
            ));
        }
        assert!(
            seen.len() >= 4,
            "expected one repo's records to spread across shards, only hit {}",
            seen.len()
        );
    }

    #[test]
    fn dedup_admits_new_then_dedups_in_flight_and_done() {
        let dedup = TapDedup::new();

        // First sighting: process it.
        assert!(matches!(dedup.admit(5), Admit::New));
        // Redelivery while still in flight: drop it.
        assert!(matches!(dedup.admit(5), Admit::InFlight));

        // Once completed, a redelivery is re-acked, not reprocessed.
        dedup.complete(5);
        assert!(matches!(dedup.admit(5), Admit::AlreadyDone));

        // A distinct id is unaffected.
        assert!(matches!(dedup.admit(8), Admit::New));
    }

    #[test]
    fn dedup_release_allows_reprocessing() {
        let dedup = TapDedup::new();

        assert!(matches!(dedup.admit(5), Admit::New));
        // Processing failed (no ack): release so tap's redelivery is retried.
        dedup.release(5);
        assert!(matches!(dedup.admit(5), Admit::New));
    }

    #[test]
    fn dedup_evicts_oldest_completed_beyond_capacity() {
        let dedup = TapDedup::new();
        {
            // Shrink capacity for the test without depending on the env default.
            let mut g = dedup.inner.lock().unwrap();
            g.capacity = 2;
        }

        for id in [1u64, 2, 3] {
            assert!(matches!(dedup.admit(id), Admit::New));
            dedup.complete(id);
        }

        // id 1 was evicted (capacity 2), so it's treated as new again; 2 and 3
        // are still remembered as done.
        assert!(matches!(dedup.admit(1), Admit::New));
        assert!(matches!(dedup.admit(2), Admit::AlreadyDone));
        assert!(matches!(dedup.admit(3), Admit::AlreadyDone));
    }

    /// `process_event` runs the full pipeline for a record and acks it back to
    /// tap after the cache write. Uses a `channel.read` delete, which maps to no
    /// downstream events and exercises the delete branch of `ack_tap_msg`
    /// (cache row removed, then ack sent).
    #[tokio::test]
    async fn process_event_runs_pipeline_and_acks() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 0,
                rows_affected: 1,
            }])
            .into_connection();

        let resolver = CommunityResolver::new();
        let author_cache = AuthorCache::new();
        let (mut to_tap, mut rx) = mpsc::channel::<String>(4);
        let (tx_inbound, _) = broadcast::channel::<SharedScopedEvent>(4);
        let (notifications_tx, _) = broadcast::channel::<IndexedNotification>(4);
        let (seen_tx, _) = broadcast::channel::<SeenEvent>(4);
        let (mute_tx, _) = broadcast::channel::<MuteEvent>(4);

        let text = String::from(
            r#"{"id":7,"type":"record","record":{"live":true,"did":"did:plc:alice","rev":"r","collection":"social.colibri.channel.read","rkey":"k1","action":"delete","record":null,"cid":null}}"#,
        );
        let record = serde_json::from_str::<TapMessage>(&text)
            .unwrap()
            .record
            .unwrap();

        process_event(
            record,
            text,
            &db,
            &resolver,
            &author_cache,
            &mut to_tap,
            &tx_inbound,
            &notifications_tx,
            &seen_tx,
            &mute_tx,
        )
        .await;

        let ack = rx.recv().await.unwrap();
        let ack_json: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack_json["type"], "ack");
        assert_eq!(ack_json["id"], 7);
    }
}
