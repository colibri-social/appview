use chrono::DateTime;
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::{atproto, db, emoji, jetstream};

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
            debug!("Backfill: not caught up yet, waiting");
            continue;
        }

        match db::get_next_backfill_item(&pool).await {
            Ok(Some((did, collection))) => {
                info!("Backfill: starting {collection} for {did}");
                if let Err(e) = backfill_item(&pool, &http, &did, &collection).await {
                    error!("Backfill: failed {collection} for {did}: {e}");
                }
            }
            Ok(None) => {
                debug!("Backfill: no pending items, all DIDs complete");
            } // all known DIDs are done
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
        Ok(u) => {
            debug!(did, pds = %u, "Resolved PDS for backfill");
            u
        }
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
    let mut page: usize = 0;

    loop {
        page += 1;
        debug!(did, page, cursor = ?cursor, "Backfill: fetching message page");
        let (records, next_cursor) =
            atproto::list_message_records(http, pds_url, did, cursor.as_deref()).await?;

        let page_count = records.len();
        debug!(did, page, count = page_count, "Backfill: processing message records");

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
                Ok(Some(_)) => {
                    debug!(did, rkey = %record.rkey, "Backfill: message inserted");
                    total += 1;
                }
                Ok(None) => {
                    debug!(did, rkey = %record.rkey, "Backfill: message already present, skipping");
                }
                Err(e) => error!(did, rkey = %record.rkey, "Backfill: DB error saving message: {e}"),
            }
        }

        match next_cursor {
            Some(c) => {
                cursor = Some(c.clone());
                if let Err(e) = db::set_backfill_cursor(pool, did, collection, &c).await {
                    error!(did, "Backfill: failed to save cursor: {e}");
                }
                sleep(Duration::from_millis(200)).await;
            }
            None => {
                db::mark_backfill_complete(pool, did, collection).await?;
                info!(did, total, pages = page, "Backfill: messages complete");
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
    let mut skipped: usize = 0;
    let mut page: usize = 0;

    loop {
        page += 1;
        debug!(did, page, cursor = ?cursor, "Backfill: fetching reaction page");
        let (records, next_cursor) =
            atproto::list_reaction_records(http, pds_url, did, cursor.as_deref()).await?;

        let page_count = records.len();
        debug!(did, page, count = page_count, "Backfill: processing reaction records");

        for record in records {
            if !emoji::is_valid_emoji(&record.emoji) {
                debug!(did, rkey = %record.rkey, emoji = %record.emoji, "Backfill: reaction rejected, not a valid emoji");
                skipped += 1;
                continue;
            }
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
                Ok(Some(_)) => {
                    debug!(did, rkey = %record.rkey, emoji = %record.emoji, "Backfill: reaction inserted");
                    total += 1;
                }
                Ok(None) => {
                    debug!(did, rkey = %record.rkey, "Backfill: reaction already present, skipping");
                }
                Err(e) => error!(did, rkey = %record.rkey, "Backfill: DB error saving reaction: {e}"),
            }
        }

        match next_cursor {
            Some(c) => {
                cursor = Some(c.clone());
                if let Err(e) = db::set_backfill_cursor(pool, did, collection, &c).await {
                    error!(did, "Backfill: failed to save cursor: {e}");
                }
                sleep(Duration::from_millis(200)).await;
            }
            None => {
                db::mark_backfill_complete(pool, did, collection).await?;
                info!(did, total, skipped, pages = page, "Backfill: reactions complete");
                break;
            }
        }
    }

    Ok(())
}
