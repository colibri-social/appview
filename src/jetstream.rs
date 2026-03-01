use anyhow::Result;
use chrono::DateTime;
use futures_util::StreamExt;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info, trace, warn};

use crate::{
    atproto, db, emoji,
    events::{AppEvent, EventBus},
    models::message::{MessageResponse, MessageWithAuthor},
};

const DEFAULT_JETSTREAM_URL: &str = "wss://jetstream2.us-east.bsky.network/subscribe\
     ?wantedCollections=social.colibri.message\
     &wantedCollections=social.colibri.reaction\
     &wantedCollections=social.colibri.community\
     &wantedCollections=social.colibri.channel\
     &wantedCollections=social.colibri.membership\
     &wantedCollections=social.colibri.approval\
     &wantedCollections=app.bsky.actor.profile";

// ── Jetstream wire types ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct JetstreamEvent {
    did: String,
    kind: String,
    commit: Option<CommitData>,
}

#[derive(Debug, Deserialize)]
struct CommitData {
    operation: String,
    collection: String,
    rkey: String,
    record: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriMessage {
    text: String,
    created_at: String,
    channel: String,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    facets: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriReaction {
    emoji: String,
    parent: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriCommunity {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    image: Option<serde_json::Value>,
    #[serde(default)]
    category_order: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriChannel {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(rename = "type")]
    channel_type: String,
    #[serde(default)]
    category: Option<String>,
    /// rkey of the community record on the same PDS as this channel.
    community: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriMembership {
    /// AT-URI of the community the member wants to join.
    community: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriApproval {
    /// AT-URI of the member's social.colibri.membership record.
    membership: String,
}

pub async fn run(pool: PgPool, http: reqwest::Client, bus: EventBus) {
    let url = std::env::var("JETSTREAM_URL").unwrap_or_else(|_| DEFAULT_JETSTREAM_URL.to_string());

    // Load persisted cursor so we can replay missed events after a restart.
    let initial_ts = db::get_jetstream_cursor(&pool).await.unwrap_or(0);
    let cursor = Arc::new(AtomicI64::new(initial_ts));
    if initial_ts > 0 {
        info!("Jetstream: loaded persisted cursor {initial_ts}");
    }

    // Flush the in-memory cursor to DB every 5 s so restarts lose at most 5 s.
    {
        let pool_f = pool.clone();
        let cursor_f = cursor.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let ts = cursor_f.load(Ordering::Relaxed);
                if ts > 0 {
                    if let Err(e) = db::set_jetstream_cursor(&pool_f, ts).await {
                        error!("Jetstream: failed to persist cursor: {e}");
                    }
                }
            }
        });
    }

    loop {
        let ts = cursor.load(Ordering::Relaxed);
        if let Err(e) = connect_and_consume(&url, &pool, &http, &bus, &cursor, ts).await {
            error!("Jetstream error: {e}");
        }
        warn!("Reconnecting to Jetstream in 5 s…");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

async fn connect_and_consume(
    url: &str,
    pool: &PgPool,
    http: &reqwest::Client,
    bus: &EventBus,
    cursor: &Arc<AtomicI64>,
    cursor_ts: i64,
) -> Result<()> {
    // Append cursor to URL only if recent enough — Jetstream's replay window is ~72 h,
    // we use 24 h to stay safely within it.
    let connect_url = if cursor_ts > 0 {
        let age_secs = chrono::Utc::now().timestamp() - cursor_ts / 1_000_000;
        if age_secs < 86_400 {
            info!("Jetstream: connecting with cursor (age {age_secs}s)");
            format!("{url}&cursor={cursor_ts}")
        } else {
            warn!("Jetstream: cursor too old ({age_secs}s), connecting without cursor");
            url.to_string()
        }
    } else {
        info!("Connecting to Jetstream at {url}");
        url.to_string()
    };

    let (ws_stream, _) = connect_async(connect_url.as_str()).await?;
    info!("Jetstream connection established. URL: {}", connect_url);

    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                // Update the cursor atomically before spawning — cheap and keeps
                // the read loop unblocked.
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(ts) = v["time_us"].as_i64() {
                        cursor.store(ts, Ordering::Relaxed);
                    }
                    // Jetstream can send error events (e.g. stale cursor); log and continue.
                    if v["kind"].as_str() == Some("error") {
                        warn!(
                            "Jetstream error event: {}",
                            v["error"].as_str().unwrap_or("unknown")
                        );
                        continue;
                    }
                }

                // Spawn each event so the read loop is never blocked by DB/HTTP work.
                // Without this, high-volume collections (e.g. app.bsky.actor.profile)
                // stall the reader and the server eventually drops the connection.
                let pool = pool.clone();
                let http = http.clone();
                let bus = bus.clone();
                tokio::spawn(async move {
                    trace!(raw = %text, "Jetstream event received");
                    if let Err(e) = handle_event(text.as_str(), &pool, &http, &bus).await {
                        error!("Failed to handle Jetstream event: {e}");
                    }
                });
            }
            Ok(other) => {
                trace!("Jetstream: ignoring non-text frame: {other:?}");
            }
            Err(e) => {
                error!("Jetstream WebSocket error: {e}");
                break;
            }
        }
    }

    info!("Jetstream stream ended, will reconnect");
    Ok(())
}

async fn handle_event(
    raw: &str,
    pool: &PgPool,
    http: &reqwest::Client,
    bus: &EventBus,
) -> Result<()> {
    let event: JetstreamEvent = serde_json::from_str(raw)?;

    if event.kind != "commit" {
        return Ok(());
    }

    let commit = match event.commit {
        Some(c) => c,
        None => return Ok(()),
    };

    match commit.collection.as_str() {
        "social.colibri.message" => {
            debug!(did = %event.did, rkey = %commit.rkey, op = %commit.operation, "Dispatching message event");
            handle_message(commit, &event.did, pool, http, bus).await
        }
        "social.colibri.reaction" => {
            debug!(did = %event.did, rkey = %commit.rkey, op = %commit.operation, "Dispatching reaction event");
            handle_reaction(commit, &event.did, pool, bus).await
        }
        "social.colibri.community" => {
            debug!(did = %event.did, rkey = %commit.rkey, op = %commit.operation, "Dispatching community event");
            handle_community(commit, &event.did, pool).await
        }
        "social.colibri.channel" => {
            debug!(did = %event.did, rkey = %commit.rkey, op = %commit.operation, "Dispatching channel event");
            handle_channel(commit, &event.did, pool).await
        }
        "social.colibri.membership" => {
            debug!(did = %event.did, rkey = %commit.rkey, op = %commit.operation, "Dispatching membership event");
            handle_membership(commit, &event.did, pool).await
        }
        "social.colibri.approval" => {
            debug!(did = %event.did, rkey = %commit.rkey, op = %commit.operation, "Dispatching approval event");
            handle_approval(commit, &event.did, pool).await
        }
        "app.bsky.actor.profile" => {
            trace!(did = %event.did, op = %commit.operation, "Dispatching profile event");
            handle_profile_update(&event.did, pool, http).await
        }
        other => {
            trace!(did = %event.did, collection = other, "Ignoring unhandled collection");
            Ok(())
        }
    }
}

// ── Message handler ───────────────────────────────────────────────────────────

async fn handle_message(
    commit: CommitData,
    did: &str,
    pool: &PgPool,
    http: &reqwest::Client,
    bus: &EventBus,
) -> Result<()> {
    match commit.operation.as_str() {
        "create" => {
            let record: ColibriMessage = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Message create: missing or malformed record, skipping");
                    return Ok(());
                }
            };

            let created_at = DateTime::parse_from_rfc3339(&record.created_at)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now());

            // Gate: only index messages from community members/owners.
            // If the channel is not yet indexed, allow through (lenient).
            if let Ok(Some((community_uri, owner_did))) =
                db::get_community_for_channel(pool, &record.channel).await
            {
                if did != owner_did
                    && !db::is_approved_member(pool, &community_uri, did)
                        .await
                        .unwrap_or(false)
                {
                    debug!(did, channel = %record.channel, "Message dropped: not a community member");
                    return Ok(());
                }
            }

            let message = match db::save_message(
                pool,
                &commit.rkey,
                did,
                &record.text,
                &record.channel,
                record.parent.as_deref(),
                record.facets.as_ref(),
                created_at,
            )
            .await
            {
                Ok(Some(m)) => {
                    info!(did, rkey = %commit.rkey, channel = %record.channel, "Message indexed");
                    m
                }
                Ok(None) => {
                    debug!(did, rkey = %commit.rkey, "Message already indexed, skipping duplicate");
                    return Ok(());
                }
                Err(e) => {
                    error!(did, rkey = %commit.rkey, "DB error saving message: {e}");
                    return Ok(());
                }
            };

            let profile = ensure_profile_cached(pool, http, did).await;
            let msg_with_author = MessageWithAuthor::from((message, profile));
            let parent_message = fetch_parent(pool, &msg_with_author).await;
            let _ = bus.send(AppEvent::Message(MessageResponse {
                message: msg_with_author,
                parent_message,
                reactions: vec![],
            }));
        }

        "update" => {
            let record: ColibriMessage = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Message update: missing or malformed record, skipping");
                    return Ok(());
                }
            };

            // Gate: only allow updates from members/owners.
            if let Ok(Some((community_uri, owner_did))) =
                db::get_community_for_channel(pool, &record.channel).await
            {
                if did != owner_did
                    && !db::is_approved_member(pool, &community_uri, did)
                        .await
                        .unwrap_or(false)
                {
                    debug!(did, channel = %record.channel, "Message update dropped: not a community member");
                    return Ok(());
                }
            }

            let message = match db::update_message(
                pool,
                &commit.rkey,
                did,
                &record.text,
                &record.channel,
                record.parent.as_deref(),
                record.facets.as_ref(),
            )
            .await
            {
                Ok(Some(m)) => {
                    info!(did, rkey = %commit.rkey, channel = %record.channel, "Message updated");
                    m
                }
                Ok(None) => {
                    debug!(did, rkey = %commit.rkey, "Message update: not in DB, skipping");
                    return Ok(());
                }
                Err(e) => {
                    error!(did, rkey = %commit.rkey, "DB error updating message: {e}");
                    return Ok(());
                }
            };

            let profile = ensure_profile_cached(pool, http, did).await;
            let msg_with_author = MessageWithAuthor::from((message, profile));
            let parent_message = fetch_parent(pool, &msg_with_author).await;
            let reactions = db::enrich_messages(pool, vec![msg_with_author.clone()])
                .await
                .ok()
                .and_then(|mut v| v.pop())
                .map(|r| r.reactions)
                .unwrap_or_default();
            let _ = bus.send(AppEvent::Message(MessageResponse {
                message: msg_with_author,
                parent_message,
                reactions,
            }));
        }

        "delete" => match db::delete_message(pool, did, &commit.rkey).await {
            Ok(Some(msg)) => {
                info!(did, rkey = %commit.rkey, channel = %msg.channel, "Message deleted");
                let _ = bus.send(AppEvent::MessageDeleted {
                    id: msg.id,
                    rkey: msg.rkey,
                    author_did: msg.author_did,
                    channel: msg.channel,
                });
            }
            Ok(None) => {
                debug!(did, rkey = %commit.rkey, "Message delete: not indexed, ignoring");
            }
            Err(e) => error!(did, rkey = %commit.rkey, "DB error deleting message: {e}"),
        },

        other => {
            trace!(did, rkey = %commit.rkey, op = other, "Message: unhandled operation");
        }
    }

    Ok(())
}

// ── Reaction handler ──────────────────────────────────────────────────────────

async fn handle_reaction(
    commit: CommitData,
    did: &str,
    pool: &PgPool,
    bus: &EventBus,
) -> Result<()> {
    match commit.operation.as_str() {
        "create" => {
            let record: ColibriReaction = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Reaction create: missing or malformed record, skipping");
                    return Ok(());
                }
            };

            if !emoji::is_valid_emoji(&record.emoji) {
                debug!(did, rkey = %commit.rkey, emoji = %record.emoji, "Reaction rejected: not a valid emoji");
                return Ok(());
            }

            let target_rkey = db::extract_rkey(&record.parent).to_owned();

            let reaction = match db::save_reaction(
                pool,
                &commit.rkey,
                did,
                &record.emoji,
                &target_rkey,
                chrono::Utc::now(),
            )
            .await
            {
                Ok(Some(r)) => {
                    info!(did, rkey = %commit.rkey, emoji = %record.emoji, target = %target_rkey, "Reaction indexed");
                    r
                }
                Ok(None) => {
                    debug!(did, rkey = %commit.rkey, "Reaction already indexed, skipping duplicate");
                    return Ok(());
                }
                Err(e) => {
                    error!(did, rkey = %commit.rkey, "DB error saving reaction: {e}");
                    return Ok(());
                }
            };

            if let Ok(Some((channel, target_author_did))) =
                db::get_message_channel(pool, &target_rkey).await
            {
                let _ = bus.send(AppEvent::ReactionAdded {
                    rkey: reaction.rkey,
                    author_did: reaction.author_did,
                    emoji: reaction.emoji,
                    target_rkey: reaction.target_rkey,
                    target_author_did,
                    channel,
                });
            }
        }

        "delete" => match db::delete_reaction(pool, did, &commit.rkey).await {
            Ok(Some(reaction)) => {
                info!(did, rkey = %commit.rkey, emoji = %reaction.emoji, target = %reaction.target_rkey, "Reaction deleted");
                if let Ok(Some((channel, target_author_did))) =
                    db::get_message_channel(pool, &reaction.target_rkey).await
                {
                    let _ = bus.send(AppEvent::ReactionRemoved {
                        rkey: reaction.rkey,
                        author_did: reaction.author_did,
                        emoji: reaction.emoji,
                        target_rkey: reaction.target_rkey,
                        target_author_did,
                        channel,
                    });
                }
            }
            Ok(None) => {
                debug!(did, rkey = %commit.rkey, "Reaction delete: not indexed, ignoring");
            }
            Err(e) => error!(did, rkey = %commit.rkey, "DB error deleting reaction: {e}"),
        },

        other => {
            trace!(did, rkey = %commit.rkey, op = other, "Reaction: unhandled operation");
        }
    }

    Ok(())
}

// ── Profile handler ───────────────────────────────────────────────────────────

async fn handle_profile_update(did: &str, pool: &PgPool, http: &reqwest::Client) -> Result<()> {
    match db::get_author_profile(pool, did).await {
        Ok(None) => return Ok(()),
        Ok(Some(_)) => {}
        Err(e) => {
            error!("DB error checking profile for {did}: {e}");
            return Ok(());
        }
    }

    match atproto::fetch_profile(http, did).await {
        Ok(Some(data)) => {
            if let Err(e) = db::upsert_author_profile(
                pool,
                did,
                data.display_name.as_deref(),
                data.avatar_url.as_deref(),
            )
            .await
            {
                error!("Failed to update profile for {did}: {e}");
            } else {
                trace!("Profile updated for {did}");
            }
        }
        Ok(None) => {}
        Err(e) => error!("Failed to fetch profile for {did}: {e}"),
    }

    Ok(())
}

// ── Shared helpers ────────────────────────────────────────────────────────────

// ── Community handler ─────────────────────────────────────────────────────────

async fn handle_community(commit: CommitData, did: &str, pool: &PgPool) -> Result<()> {
    let uri = format!("at://{}/social.colibri.community/{}", did, commit.rkey);
    match commit.operation.as_str() {
        "create" | "update" => {
            let record: ColibriCommunity = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Community: missing/malformed record, skipping");
                    return Ok(());
                }
            };
            if let Err(e) = db::save_community(
                pool,
                &uri,
                did,
                &commit.rkey,
                &record.name,
                record.description.as_deref(),
                record.image.as_ref(),
                record.category_order.as_ref(),
            )
            .await
            {
                error!(did, rkey = %commit.rkey, "DB error saving community: {e}");
            } else {
                info!(did, rkey = %commit.rkey, name = %record.name, "Community indexed");
            }
        }
        "delete" => {
            if let Err(e) = db::delete_community(pool, &uri).await {
                error!(did, rkey = %commit.rkey, "DB error deleting community: {e}");
            } else {
                info!(did, rkey = %commit.rkey, "Community deleted");
            }
        }
        other => trace!(did, op = other, "Community: unhandled operation"),
    }
    Ok(())
}

// ── Channel handler ───────────────────────────────────────────────────────────

async fn handle_channel(commit: CommitData, did: &str, pool: &PgPool) -> Result<()> {
    let uri = format!("at://{}/social.colibri.channel/{}", did, commit.rkey);
    match commit.operation.as_str() {
        "create" | "update" => {
            let record: ColibriChannel = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Channel: missing/malformed record, skipping");
                    return Ok(());
                }
            };
            // Reconstruct the community AT-URI from the owner DID + community rkey.
            let community_uri = format!(
                "at://{}/social.colibri.community/{}",
                did, record.community
            );
            if let Err(e) = db::save_channel(
                pool,
                &uri,
                did,
                &commit.rkey,
                &community_uri,
                &record.name,
                record.description.as_deref(),
                &record.channel_type,
                record.category.as_deref(),
            )
            .await
            {
                error!(did, rkey = %commit.rkey, "DB error saving channel: {e}");
            } else {
                info!(did, rkey = %commit.rkey, name = %record.name, "Channel indexed");
            }
        }
        "delete" => {
            if let Err(e) = db::delete_channel(pool, &uri).await {
                error!(did, rkey = %commit.rkey, "DB error deleting channel: {e}");
            } else {
                info!(did, rkey = %commit.rkey, "Channel deleted");
            }
        }
        other => trace!(did, op = other, "Channel: unhandled operation"),
    }
    Ok(())
}

// ── Membership handler ────────────────────────────────────────────────────────

async fn handle_membership(commit: CommitData, did: &str, pool: &PgPool) -> Result<()> {
    let membership_uri = format!("at://{}/social.colibri.membership/{}", did, commit.rkey);
    match commit.operation.as_str() {
        "create" => {
            let record: ColibriMembership = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Membership: missing/malformed record, skipping");
                    return Ok(());
                }
            };
            if let Err(e) = db::save_membership(
                pool,
                &membership_uri,
                &record.community,
                did,
                &commit.rkey,
            )
            .await
            {
                error!(did, rkey = %commit.rkey, "DB error saving membership: {e}");
            } else {
                info!(did, rkey = %commit.rkey, community = %record.community, "Membership indexed (pending)");
            }
        }
        "delete" => {
            if let Err(e) = db::delete_membership(pool, &membership_uri).await {
                error!(did, rkey = %commit.rkey, "DB error deleting membership: {e}");
            } else {
                info!(did, rkey = %commit.rkey, "Membership deleted");
            }
        }
        other => trace!(did, op = other, "Membership: unhandled operation"),
    }
    Ok(())
}

// ── Approval handler ──────────────────────────────────────────────────────────

async fn handle_approval(commit: CommitData, did: &str, pool: &PgPool) -> Result<()> {
    let approval_uri = format!("at://{}/social.colibri.approval/{}", did, commit.rkey);
    match commit.operation.as_str() {
        "create" => {
            let record: ColibriApproval = match commit
                .record
                .and_then(|v| serde_json::from_value(v).ok())
            {
                Some(r) => r,
                None => {
                    debug!(did, rkey = %commit.rkey, "Approval: missing/malformed record, skipping");
                    return Ok(());
                }
            };
            if let Err(e) =
                db::save_approval(pool, &approval_uri, &record.membership, &commit.rkey).await
            {
                error!(did, rkey = %commit.rkey, "DB error saving approval: {e}");
            } else {
                info!(did, rkey = %commit.rkey, membership = %record.membership, "Approval indexed → member approved");
            }
        }
        "delete" => {
            if let Err(e) = db::delete_approval(pool, &approval_uri).await {
                error!(did, rkey = %commit.rkey, "DB error deleting approval: {e}");
            } else {
                info!(did, rkey = %commit.rkey, "Approval deleted → member reverted to pending");
            }
        }
        other => trace!(did, op = other, "Approval: unhandled operation"),
    }
    Ok(())
}

async fn fetch_parent(pool: &PgPool, msg: &MessageWithAuthor) -> Option<Box<MessageWithAuthor>> {
    let parent_rkey = db::extract_rkey(msg.parent.as_deref()?);
    db::get_message_by_rkey(pool, parent_rkey)
        .await
        .ok()
        .flatten()
        .map(Box::new)
}

/// Return the cached profile for `did`, fetching from the ATProto API if absent.
pub async fn ensure_profile_cached(
    pool: &PgPool,
    http: &reqwest::Client,
    did: &str,
) -> Option<crate::models::author::AuthorProfile> {
    if let Ok(Some(profile)) = db::get_author_profile(pool, did).await {
        return Some(profile);
    }

    match atproto::fetch_profile(http, did).await {
        Ok(Some(data)) => {
            match db::upsert_author_profile(
                pool,
                did,
                data.display_name.as_deref(),
                data.avatar_url.as_deref(),
            )
            .await
            {
                Ok(profile) => Some(profile),
                Err(e) => {
                    error!("Failed to cache profile for {did}: {e}");
                    None
                }
            }
        }
        Ok(None) => None,
        Err(e) => {
            error!("Failed to fetch profile for {did}: {e}");
            None
        }
    }
}
