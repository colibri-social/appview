use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::models::{author::AuthorProfile, message::{Message, MessageWithAuthor}};

/// Saves a given message to the database.
pub async fn save_message(
    pool: &PgPool,
    rkey: &str,
    author_did: &str,
    text: &str,
    channel: &str,
    created_at: DateTime<Utc>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
    INSERT INTO messages (rkey, author_did, text, channel, created_at)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (author_did, rkey) DO NOTHING
    RETURNING id, rkey, author_did, text, channel, created_at, indexed_at
    "#,
    )
    .bind(rkey)
    .bind(author_did)
    .bind(text)
    .bind(channel)
    .bind(created_at)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Removes a message from the database.
pub async fn delete_message(pool: &PgPool, author_did: &str, rkey: &str) -> Result<()> {
    sqlx::query("DELETE FROM messages WHERE author_did = $1 AND rkey = $2")
        .bind(author_did)
        .bind(rkey)
        .execute(pool)
        .await?;
    Ok(())
}

/// Returns messages for a channel joined with cached author profiles.
pub async fn get_messages(
    pool: &PgPool,
    channel: &str,
    limit: i64,
    before: Option<DateTime<Utc>>,
) -> Result<Vec<MessageWithAuthor>> {
    let messages = if let Some(before) = before {
        sqlx::query_as::<_, MessageWithAuthor>(
            r#"
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel, m.created_at, m.indexed_at,
                   a.display_name, a.avatar_url
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.channel = $1 AND m.created_at < $2
            ORDER BY m.created_at DESC
            LIMIT $3
            "#,
        )
        .bind(channel)
        .bind(before)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, MessageWithAuthor>(
            r#"
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel, m.created_at, m.indexed_at,
                   a.display_name, a.avatar_url
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.channel = $1
            ORDER BY m.created_at DESC
            LIMIT $2
            "#,
        )
        .bind(channel)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };

    Ok(messages)
}

// ── Author profiles ───────────────────────────────────────────────────────────

/// Insert or refresh a cached author profile.
pub async fn upsert_author_profile(
    pool: &PgPool,
    did: &str,
    display_name: Option<&str>,
    avatar_url: Option<&str>,
) -> Result<AuthorProfile> {
    let profile = sqlx::query_as::<_, AuthorProfile>(
        r#"
        INSERT INTO author_profiles (did, display_name, avatar_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (did) DO UPDATE
          SET display_name = EXCLUDED.display_name,
              avatar_url   = EXCLUDED.avatar_url,
              updated_at   = NOW()
        RETURNING did, display_name, avatar_url, updated_at
        "#,
    )
    .bind(did)
    .bind(display_name)
    .bind(avatar_url)
    .fetch_one(pool)
    .await?;

    Ok(profile)
}

/// Retrieve a cached author profile. Returns `None` if never fetched.
pub async fn get_author_profile(pool: &PgPool, did: &str) -> Result<Option<AuthorProfile>> {
    let profile = sqlx::query_as::<_, AuthorProfile>(
        "SELECT did, display_name, avatar_url, updated_at FROM author_profiles WHERE did = $1",
    )
    .bind(did)
    .fetch_optional(pool)
    .await?;

    Ok(profile)
}

// ── Backfill helpers ──────────────────────────────────────────────────────────

/// Returns true when the appview is caught up with real-time: the average lag
/// between `created_at` and `indexed_at` across the 20 most recently indexed
/// messages is less than 60 seconds.
pub async fn is_caught_up(pool: &PgPool) -> Result<bool> {
    let avg_lag: Option<f64> = sqlx::query_scalar(
        r#"
        SELECT AVG(EXTRACT(EPOCH FROM (indexed_at - created_at)))
        FROM (
            SELECT indexed_at, created_at
            FROM messages
            ORDER BY indexed_at DESC
            LIMIT 20
        ) recent
        "#,
    )
    .fetch_one(pool)
    .await?;

    // No messages yet → assume not caught up
    Ok(avg_lag.unwrap_or(f64::MAX) < 60.0)
}

/// Return one DID whose backfill is not yet complete.
pub async fn get_next_backfill_did(pool: &PgPool) -> Result<Option<String>> {
    let did: Option<String> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT m.author_did
        FROM messages m
        WHERE NOT EXISTS (
            SELECT 1 FROM backfill_state bs
            WHERE bs.did = m.author_did AND bs.completed = TRUE
        )
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(did)
}

/// Return the stored cursor for a DID's backfill (`None` = start from the top).
pub async fn get_backfill_cursor(pool: &PgPool, did: &str) -> Result<Option<String>> {
    // fetch_optional on a nullable column gives Option<Option<String>>.
    let cursor: Option<Option<String>> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT cursor FROM backfill_state WHERE did = $1 AND completed = FALSE",
    )
    .bind(did)
    .fetch_optional(pool)
    .await?;

    Ok(cursor.flatten())
}

/// Persist the pagination cursor mid-backfill.
pub async fn set_backfill_cursor(pool: &PgPool, did: &str, cursor: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backfill_state (did, completed, cursor)
        VALUES ($1, FALSE, $2)
        ON CONFLICT (did) DO UPDATE
          SET cursor = EXCLUDED.cursor, completed = FALSE, updated_at = NOW()
        "#,
    )
    .bind(did)
    .bind(cursor)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a DID's backfill as complete.
pub async fn mark_backfill_complete(pool: &PgPool, did: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backfill_state (did, completed, cursor)
        VALUES ($1, TRUE, NULL)
        ON CONFLICT (did) DO UPDATE
          SET completed = TRUE, cursor = NULL, updated_at = NOW()
        "#,
    )
    .bind(did)
    .execute(pool)
    .await?;
    Ok(())
}
