use chrono::DateTime;
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::{atproto, db, jetstream};

/// Runs as a background task.
///
/// Every 30 seconds it checks whether the appview is caught up with real-time
/// (indexed_at ≈ created_at for recent messages). Once caught up it picks one
/// DID at a time and pages through all of their `social.colibri.message`
/// records via `com.atproto.repo.listRecords`, inserting anything that was not
/// already in the database.
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

        let did = match db::get_next_backfill_did(&pool).await {
            Ok(Some(d)) => d,
            Ok(None) => continue, // all known DIDs are done
            Err(e) => {
                error!("Backfill: get_next_backfill_did failed: {e}");
                continue;
            }
        };

        info!("Backfill: starting for {did}");
        if let Err(e) = backfill_did(&pool, &http, &did).await {
            error!("Backfill: failed for {did}: {e}");
        }
    }
}

// ── Per-DID backfill ──────────────────────────────────────────────────────────

async fn backfill_did(pool: &PgPool, http: &reqwest::Client, did: &str) -> anyhow::Result<()> {
    // Resolve the user's PDS endpoint from their DID document.
    let pds_url = match atproto::resolve_pds(http, did).await {
        Ok(u) => u,
        Err(e) => {
            warn!("Backfill: cannot resolve PDS for {did}: {e} — marking complete");
            db::mark_backfill_complete(pool, did).await?;
            return Ok(());
        }
    };

    // Resume from the last stored cursor (None = start from the beginning).
    let mut cursor = db::get_backfill_cursor(pool, did).await?;
    let mut total: usize = 0;

    loop {
        let (records, next_cursor) =
            atproto::list_message_records(http, &pds_url, did, cursor.as_deref()).await?;

        for record in records {
            let created_at = DateTime::parse_from_rfc3339(&record.created_at)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now());

            if let Err(e) =
                db::save_message(pool, &record.rkey, did, &record.text, &record.channel, record.parent.as_deref(), created_at)
                    .await
            {
                error!("Backfill: DB error saving message {}: {e}", record.rkey);
            } else {
                total += 1;
            }
        }

        // Ensure the author profile is cached once (cheapest: first page).
        if cursor.is_none() {
            let _ = jetstream::ensure_profile_cached(pool, http, did).await;
        }

        match next_cursor {
            Some(c) => {
                cursor = Some(c.clone());
                if let Err(e) = db::set_backfill_cursor(pool, did, &c).await {
                    error!("Backfill: failed to save cursor for {did}: {e}");
                }
                // Be a polite client.
                sleep(Duration::from_millis(200)).await;
            }
            None => {
                db::mark_backfill_complete(pool, did).await?;
                info!("Backfill: complete for {did} ({total} records processed)");
                break;
            }
        }
    }

    Ok(())
}
