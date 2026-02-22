use anyhow::Result;
use chrono::DateTime;
use futures_util::StreamExt;
use serde::Deserialize;
use sqlx::PgPool;
use tokio_tungstenite::connect_async;
use tracing::{debug, error, info, trace, warn};

use crate::{
    atproto,
    db,
    emoji,
    events::{AppEvent, EventBus},
    models::message::{MessageResponse, MessageWithAuthor},
};

const DEFAULT_JETSTREAM_URL: &str =
    "wss://jetstream2.us-east.bsky.network/subscribe\
     ?wantedCollections=social.colibri.message\
     &wantedCollections=social.colibri.reaction\
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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriReaction {
    emoji: String,
    target_message: String,
}

// ── Public entry point ────────────────────────────────────────────────────────

pub async fn run(pool: PgPool, http: reqwest::Client, bus: EventBus) {
    let url =
        std::env::var("JETSTREAM_URL").unwrap_or_else(|_| DEFAULT_JETSTREAM_URL.to_string());

    loop {
        if let Err(e) = connect_and_consume(&url, &pool, &http, &bus).await {
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
) -> Result<()> {
    info!("Connecting to Jetstream at {url}");
    let (ws_stream, _) = connect_async(url).await?;
    info!("Jetstream connection established");

    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                trace!(raw = %text, "Jetstream event received");
                if let Err(e) = handle_event(text.as_str(), pool, http, bus).await {
                    error!("Failed to handle Jetstream event: {e}");
                }
            }
            Err(e) => {
                error!("Jetstream WebSocket error: {e}");
                break;
            }
            _ => {}
        }
    }

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
            let record: ColibriMessage =
                match commit.record.and_then(|v| serde_json::from_value(v).ok()) {
                    Some(r) => r,
                    None => {
                        debug!(did, rkey = %commit.rkey, "Message create: missing or malformed record, skipping");
                        return Ok(());
                    }
                };

            let created_at = DateTime::parse_from_rfc3339(&record.created_at)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now());

            let message = match db::save_message(
                pool,
                &commit.rkey,
                did,
                &record.text,
                &record.channel,
                record.parent.as_deref(),
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
            let record: ColibriMessage =
                match commit.record.and_then(|v| serde_json::from_value(v).ok()) {
                    Some(r) => r,
                    None => {
                        debug!(did, rkey = %commit.rkey, "Message update: missing or malformed record, skipping");
                        return Ok(());
                    }
                };

            let message = match db::update_message(
                pool,
                &commit.rkey,
                did,
                &record.text,
                &record.channel,
                record.parent.as_deref(),
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

        "delete" => {
            match db::delete_message(pool, did, &commit.rkey).await {
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
            }
        }

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
            let record: ColibriReaction =
                match commit.record.and_then(|v| serde_json::from_value(v).ok()) {
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

            let target_rkey = db::extract_rkey(&record.target_message).to_owned();

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

            if let Ok(Some(channel)) =
                db::get_message_channel(pool, &target_rkey).await
            {
                let _ = bus.send(AppEvent::ReactionAdded {
                    rkey: reaction.rkey,
                    author_did: reaction.author_did,
                    emoji: reaction.emoji,
                    target_rkey: reaction.target_rkey,
                    channel,
                });
            }
        }

        "delete" => {
            match db::delete_reaction(pool, did, &commit.rkey).await {
                Ok(Some(reaction)) => {
                    info!(did, rkey = %commit.rkey, emoji = %reaction.emoji, target = %reaction.target_rkey, "Reaction deleted");
                    if let Ok(Some(channel)) =
                        db::get_message_channel(pool, &reaction.target_rkey).await
                    {
                        let _ = bus.send(AppEvent::ReactionRemoved {
                            rkey: reaction.rkey,
                            author_did: reaction.author_did,
                            emoji: reaction.emoji,
                            target_rkey: reaction.target_rkey,
                            channel,
                        });
                    }
                }
                Ok(None) => {
                    debug!(did, rkey = %commit.rkey, "Reaction delete: not indexed, ignoring");
                }
                Err(e) => error!(did, rkey = %commit.rkey, "DB error deleting reaction: {e}"),
            }
        }

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
