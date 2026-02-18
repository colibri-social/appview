use anyhow::Result;
use chrono::DateTime;
use futures_util::StreamExt;
use serde::Deserialize;
use sqlx::PgPool;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use crate::{
    db,
    events::{AppEvent, EventBus},
};

const DEFAULT_JETSTREAM_URL: &str =
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=social.colibri.message";

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
    record: Option<ColibriRecord>,
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
pub async fn run(pool: PgPool, bus: EventBus) {
    let url = std::env::var("JETSTREAM_URL")
        .unwrap_or_else(|_| DEFAULT_JETSTREAM_URL.to_string());

    loop {
        if let Err(e) = connect_and_consume(&url, &pool, &bus).await {
            error!("Jetstream error: {e}");
        }
        warn!("Reconnecting to Jetstream in 5 s…");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

async fn connect_and_consume(url: &str, pool: &PgPool, bus: &EventBus) -> Result<()> {
    info!("Connecting to Jetstream at {url}");
    let (ws_stream, _) = connect_async(url).await?;
    info!("Jetstream connection established");

    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                if let Err(e) = handle_event(text.as_str(), pool, bus).await {
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

async fn handle_event(raw: &str, pool: &PgPool, bus: &EventBus) -> Result<()> {
    let event: JetstreamEvent = serde_json::from_str(raw)?;

    if event.kind != "commit" {
        return Ok(());
    }

    let commit = match event.commit {
        Some(c) => c,
        None => return Ok(()),
    };

    if commit.collection != "social.colibri.message" {
        return Ok(());
    }

    match commit.operation.as_str() {
        "create" => {
            let record = match commit.record {
                Some(r) => r,
                None => return Ok(()),
            };

            let created_at = DateTime::parse_from_rfc3339(&record.created_at)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now());

            match db::save_message(
                pool,
                &commit.rkey,
                &event.did,
                &record.text,
                &record.channel,
                created_at,
            )
            .await
            {
                Ok(Some(message)) => {
                    let _ = bus.send(AppEvent::Message(message));
                }
                Ok(None) => {} // duplicate, ignored by ON CONFLICT
                Err(e) => error!("DB error saving message: {e}"),
            }
        }

        "delete" => {
            if let Err(e) = db::delete_message(pool, &event.did, &commit.rkey).await {
                error!("DB error deleting message: {e}");
            }
        }

        _ => {}
    }

    Ok(())
}
