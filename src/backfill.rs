use chrono::DateTime;
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::{atproto, db, jetstream};

/// Runs as a background task.
///
/// Every 30 s it checks whether the appview is caught up. Once caught up it
/// picks one (DID, collection) pair at a time and paginates through
/// `com.atproto.repo.listRecords` on the author's PDS, inserting any records
/// not already in the database.
///
/// Supports: social.colibri.message, social.colibri.reaction
pub async fn run(pool: PgPool, http: reqwest::Client) {
    loop {
        sleep(Duration::from_secs(30)).await;

        let caught_up = match db::is_caught_up(&pool).await {
            Ok(v) => v,
            Err(e) => {
                error!("Backfill: is_caught_up failed: {e}");
                continue;
            }
        };

        if !caught_up {
            continue;
        }

        match db::get_next_backfill_item(&pool).await {
            Ok(Some((did, collection))) => {
                info!("Backfill: starting {collection} for {did}");
                if let Err(e) = backfill_item(&pool, &http, &did, &collection).await {
                    error!("Backfill: failed {collection} for {did}: {e}");
                }
            }
            Ok(None) => {} // all known DIDs are done
            Err(e) => error!("Backfill: get_next_backfill_item failed: {e}"),
        }
    }
}

// ── Per-(DID, collection) backfill ────────────────────────────────────────────

async fn backfill_item(
    pool: &PgPool,
    http: &reqwest::Client,
    did: &str,
    collection: &str,
) -> anyhow::Result<()> {
    let pds_url = match atproto::resolve_pds(http, did).await {
        Ok(u) => u,
        Err(e) => {
            warn!("Backfill: cannot resolve PDS for {did}: {e} — marking complete");
            db::mark_backfill_complete(pool, did, collection).await?;
            return Ok(());
        }
    };

    // Cache the profile once per DID (cheapest: first time we touch this DID).
    let _ = jetstream::ensure_profile_cached(pool, http, did).await;

    match collection {
        "social.colibri.message" => {
            backfill_messages(pool, http, did, &pds_url, collection).await
        }
        "social.colibri.reaction" => {
            backfill_reactions(pool, http, did, &pds_url, collection).await
        }
        other => {
            warn!("Backfill: unknown collection {other} — marking complete");
            db::mark_backfill_complete(pool, did, collection).await?;
            Ok(())
        }
    }
}

async fn backfill_messages(
    pool: &PgPool,
    http: &reqwest::Client,
    did: &str,
    pds_url: &str,
    collection: &str,
) -> anyhow::Result<()> {
    let mut cursor = db::get_backfill_cursor(pool, did, collection).await?;
    let mut total: usize = 0;

    loop {
        let (records, next_cursor) =
            atproto::list_message_records(http, pds_url, did, cursor.as_deref()).await?;

        for record in records {
            let created_at = DateTime::parse_from_rfc3339(&record.created_at)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now());

            match db::save_message(
                pool,
                &record.rkey,
                did,
                &record.text,
                &record.channel,
                record.parent.as_deref(),
                created_at,
            )
            .await
            {
                Ok(Some(_)) => total += 1,
                Ok(None) => {} // already present
                Err(e) => error!("Backfill: DB error saving message {}: {e}", record.rkey),
            }
        }

        match next_cursor {
            Some(c) => {
                cursor = Some(c.clone());
                if let Err(e) = db::set_backfill_cursor(pool, did, collection, &c).await {
                    error!("Backfill: failed to save cursor for {did}: {e}");
                }
                sleep(Duration::from_millis(200)).await;
            }
            None => {
                db::mark_backfill_complete(pool, did, collection).await?;
                info!("Backfill: messages complete for {did} ({total} inserted)");
                break;
            }
        }
    }

    Ok(())
}

async fn backfill_reactions(
    pool: &PgPool,
    http: &reqwest::Client,
    did: &str,
    pds_url: &str,
    collection: &str,
) -> anyhow::Result<()> {
    let mut cursor = db::get_backfill_cursor(pool, did, collection).await?;
    let mut total: usize = 0;

    loop {
        let (records, next_cursor) =
            atproto::list_reaction_records(http, pds_url, did, cursor.as_deref()).await?;

        for record in records {
            match db::save_reaction(
                pool,
                &record.rkey,
                did,
                &record.emoji,
                &record.target_rkey,
                chrono::Utc::now(),
            )
            .await
            {
                Ok(Some(_)) => total += 1,
                Ok(None) => {} // already present
                Err(e) => error!("Backfill: DB error saving reaction {}: {e}", record.rkey),
            }
        }

        match next_cursor {
            Some(c) => {
                cursor = Some(c.clone());
                if let Err(e) = db::set_backfill_cursor(pool, did, collection, &c).await {
                    error!("Backfill: failed to save cursor for {did}: {e}");
                }
                sleep(Duration::from_millis(200)).await;
            }
            None => {
                db::mark_backfill_complete(pool, did, collection).await?;
                info!("Backfill: reactions complete for {did} ({total} inserted)");
                break;
            }
        }
    }

    Ok(())
}
