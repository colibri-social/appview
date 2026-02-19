use anyhow::Result;
use chrono::DateTime;
use futures_util::StreamExt;
use serde::Deserialize;
use sqlx::PgPool;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use crate::{
    atproto,
    db,
    events::{AppEvent, EventBus},
    models::message::MessageWithAuthor,
};

const DEFAULT_JETSTREAM_URL: &str =
    "wss://jetstream2.us-east.bsky.network/subscribe\
     ?wantedCollections=social.colibri.message\
     &wantedCollections=app.bsky.actor.profile";

// ── Jetstream wire types ─────────────────────────────────────────────────────

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

/// ATProto lexicon: social.colibri.message
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriRecord {
    text: String,
    created_at: String,
    channel: String,
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Runs forever, reconnecting on any error.
pub async fn run(pool: PgPool, http: reqwest::Client, bus: EventBus) {
    let url = std::env::var("JETSTREAM_URL").unwrap_or_else(|_| DEFAULT_JETSTREAM_URL.to_string());

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
            handle_message(commit, &event.did, pool, http, bus).await
        }
        "app.bsky.actor.profile" => {
            handle_profile_update(&event.did, pool, http).await
        }
        _ => Ok(()),
    }
}

async fn handle_message(
    commit: CommitData,
    did: &str,
    pool: &PgPool,
    http: &reqwest::Client,
    bus: &EventBus,
) -> Result<()> {
    match commit.operation.as_str() {
        "create" => {
            let record: ColibriRecord = match commit.record.and_then(|v| {
                serde_json::from_value(v).ok()
            }) {
                Some(r) => r,
                None => return Ok(()),
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
                created_at,
            )
            .await
            {
                Ok(Some(m)) => m,
                Ok(None) => return Ok(()), // duplicate
                Err(e) => {
                    error!("DB error saving message: {e}");
                    return Ok(());
                }
            };

            // Fetch and cache the author profile if we don't have it yet.
            let profile = ensure_profile_cached(pool, http, did).await;

            let _ = bus.send(AppEvent::Message(MessageWithAuthor::from((message, profile))));
        }

        "delete" => {
            if let Err(e) = db::delete_message(pool, did, &commit.rkey).await {
                error!("DB error deleting message: {e}");
            }
        }

        _ => {}
    }

    Ok(())
}

/// Handle a `app.bsky.actor.profile` (self) commit.
/// Only refreshes the cached profile if we already have an entry for this DID —
/// no new rows are inserted, no polling occurs.
async fn handle_profile_update(did: &str, pool: &PgPool, http: &reqwest::Client) -> Result<()> {
    // Only act if this author is already in our cache.
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
                info!("Profile updated for {did}");
            }
        }
        Ok(None) => {}
        Err(e) => error!("Failed to fetch profile for {did}: {e}"),
    }

    Ok(())
}

/// Return the cached profile for `did`, fetching from the ATProto API if absent.
pub async fn ensure_profile_cached(
    pool: &PgPool,
    http: &reqwest::Client,
    did: &str,
) -> Option<crate::models::author::AuthorProfile> {
    // Return cached copy if we already have one.
    if let Ok(Some(profile)) = db::get_author_profile(pool, did).await {
        return Some(profile);
    }

    // Fetch from the public Bluesky API.
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
