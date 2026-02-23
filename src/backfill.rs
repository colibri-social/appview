use chrono::DateTime;
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::{atproto, db, emoji, jetstream};

// How long since the last indexed message before we assume Jetstream is lagging.
const LAG_THRESHOLD_SECS: i64 = 300; // 5 min
// How often the sweep-loop wakes up to check lag.
const SWEEP_CHECK_SECS: u64 = 60;

/// Top-level entry point — spawns two independent tasks:
///
/// 1. **Historical backfill** — walks every (DID, collection) pair fully,
///    using a resumable cursor. Runs once per pair, picks up new DIDs as they
///    appear.
///
/// 2. **Lag-triggered sweep** — wakes every minute and checks whether any
///    message has been indexed recently. If the Jetstream appears to be lagging
///    (no new messages for >5 min) it fetches the newest page of records from
///    every known DID's PDS and inserts anything missing.
pub async fn run(pool: PgPool, http: reqwest::Client) {
    let pool2 = pool.clone();
    let http2 = http.clone();
    tokio::spawn(run_historical(pool2, http2));
    run_sweep_loop(pool, http).await;
}

// ── Historical backfill ───────────────────────────────────────────────────────

async fn run_historical(pool: PgPool, http: reqwest::Client) {
    info!("Backfill/historical: active");
    loop {
        match db::get_next_backfill_item(&pool).await {
            Ok(Some((did, collection))) => {
                info!(did, collection, "Backfill/historical: starting");
                if let Err(e) = backfill_item(&pool, &http, &did, &collection).await {
                    error!(did, collection, "Backfill/historical: failed: {e}");
                }
            }
            Ok(None) => {
                debug!("Backfill/historical: no pending items");
                sleep(Duration::from_secs(60)).await;
            }
            Err(e) => {
                error!("Backfill/historical: get_next_backfill_item failed: {e}");
                sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

// ── Lag-triggered sweep ───────────────────────────────────────────────────────

async fn run_sweep_loop(pool: PgPool, http: reqwest::Client) {
    loop {
        sleep(Duration::from_secs(SWEEP_CHECK_SECS)).await;

        let lagging = match db::get_last_indexed_at(&pool).await {
            Ok(Some(t)) => (chrono::Utc::now() - t).num_seconds() > LAG_THRESHOLD_SECS,
            Ok(None) => false, // no messages yet, nothing to lag on
            Err(e) => {
                error!("Backfill/sweep: get_last_indexed_at failed: {e}");
                false
            }
        };

        if lagging {
            info!("Backfill/sweep: Jetstream appears to be lagging — sweeping recent records");
            sweep_all_known_dids(&pool, &http).await;
        }
    }
}

async fn sweep_all_known_dids(pool: &PgPool, http: &reqwest::Client) {
    let dids = match db::get_all_known_dids(pool).await {
        Ok(d) => d,
        Err(e) => {
            error!("Backfill/sweep: get_all_known_dids failed: {e}");
            return;
        }
    };

    if dids.is_empty() {
        debug!("Backfill/sweep: no known DIDs");
        return;
    }

    info!(dids = dids.len(), "Backfill/sweep: sweeping recent records for all known DIDs");
    let mut total_inserted = 0usize;

    for did in &dids {
        let pds_url = match atproto::resolve_pds(http, did).await {
            Ok(u) => u,
            Err(e) => {
                warn!(did, "Backfill/sweep: cannot resolve PDS: {e}");
                continue;
            }
        };

        let _ = jetstream::ensure_profile_cached(pool, http, did).await;

        // Fetch newest page only (reverse=false gives oldest-first, so we use
        // a single page without cursor which returns the most recent records).
        if let Ok((records, _)) =
            atproto::list_message_records(http, &pds_url, did, None).await
        {
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
                    record.facets.as_ref(),
                    created_at,
                )
                .await
                {
                    Ok(Some(_)) => {
                        debug!(did, rkey = %record.rkey, "Backfill/sweep: message inserted");
                        total_inserted += 1;
                    }
                    Ok(None) => {}
                    Err(e) => error!(did, rkey = %record.rkey, "Backfill/sweep: DB error: {e}"),
                }
            }
        }

        if let Ok((records, _)) =
            atproto::list_reaction_records(http, &pds_url, did, None).await
        {
            for record in records {
                if !emoji::is_valid_emoji(&record.emoji) {
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
                        debug!(did, rkey = %record.rkey, "Backfill/sweep: reaction inserted");
                        total_inserted += 1;
                    }
                    Ok(None) => {}
                    Err(e) => error!(did, rkey = %record.rkey, "Backfill/sweep: DB error: {e}"),
                }
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    info!(dids = dids.len(), total_inserted, "Backfill/sweep: complete");
}

// ── Per-(DID, collection) historical helpers ──────────────────────────────────


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
                record.facets.as_ref(),
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
