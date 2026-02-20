use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use sqlx::PgPool;

use crate::models::{
    author::AuthorProfile,
    message::{Message, MessageResponse, MessageWithAuthor},
};

// ── Messages ──────────────────────────────────────────────────────────────────

/// Insert a new message (no-op on duplicate author_did+rkey).
pub async fn save_message(
    pool: &PgPool,
    rkey: &str,
    author_did: &str,
    text: &str,
    channel: &str,
    parent: Option<&str>,
    created_at: DateTime<Utc>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        INSERT INTO messages (rkey, author_did, text, channel, parent, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (author_did, rkey) DO NOTHING
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent
        "#,
    )
    .bind(rkey)
    .bind(author_did)
    .bind(text)
    .bind(channel)
    .bind(parent)
    .bind(created_at)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Update an existing message (edit). Sets `edited = TRUE`.
pub async fn update_message(
    pool: &PgPool,
    rkey: &str,
    author_did: &str,
    text: &str,
    channel: &str,
    parent: Option<&str>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        UPDATE messages
        SET text = $1, channel = $2, parent = $3, edited = TRUE
        WHERE author_did = $4 AND rkey = $5
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent
        "#,
    )
    .bind(text)
    .bind(channel)
    .bind(parent)
    .bind(author_did)
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Delete a message, returning the deleted row so callers can broadcast the event.
pub async fn delete_message(
    pool: &PgPool,
    author_did: &str,
    rkey: &str,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        DELETE FROM messages
        WHERE author_did = $1 AND rkey = $2
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent
        "#,
    )
    .bind(author_did)
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Paginated messages for a channel, with author profiles and parent messages populated.
pub async fn get_messages(
    pool: &PgPool,
    channel: &str,
    limit: i64,
    before: Option<DateTime<Utc>>,
) -> Result<Vec<MessageResponse>> {
    let messages: Vec<MessageWithAuthor> = if let Some(before) = before {
        sqlx::query_as::<_, MessageWithAuthor>(
            r#"
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
                   m.created_at, m.indexed_at, m.edited, m.parent,
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
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
                   m.created_at, m.indexed_at, m.edited, m.parent,
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

    populate_parents(pool, messages).await
}

/// Look up a single message by rkey (first match), with author profile.
pub async fn get_message_by_rkey(
    pool: &PgPool,
    rkey: &str,
) -> Result<Option<MessageWithAuthor>> {
    let msg = sqlx::query_as::<_, MessageWithAuthor>(
        r#"
        SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
               m.created_at, m.indexed_at, m.edited, m.parent,
               a.display_name, a.avatar_url
        FROM messages m
        LEFT JOIN author_profiles a ON m.author_did = a.did
        WHERE m.rkey = $1
        LIMIT 1
        "#,
    )
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Batch-fetch parent messages and attach them to the given list.
pub async fn populate_parents(
    pool: &PgPool,
    messages: Vec<MessageWithAuthor>,
) -> Result<Vec<MessageResponse>> {
    let parent_rkeys: Vec<String> = messages
        .iter()
        .filter_map(|m| m.parent.as_deref())
        .map(|p| extract_rkey(p).to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if parent_rkeys.is_empty() {
        return Ok(messages
            .into_iter()
            .map(|m| MessageResponse { message: m, parent_message: None })
            .collect());
    }

    let parents: Vec<MessageWithAuthor> = sqlx::query_as::<_, MessageWithAuthor>(
        r#"
        SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
               m.created_at, m.indexed_at, m.edited, m.parent,
               a.display_name, a.avatar_url
        FROM messages m
        LEFT JOIN author_profiles a ON m.author_did = a.did
        WHERE m.rkey = ANY($1)
        "#,
    )
    .bind(&parent_rkeys[..])
    .fetch_all(pool)
    .await?;

    let parent_map: HashMap<String, MessageWithAuthor> =
        parents.into_iter().map(|p| (p.rkey.clone(), p)).collect();

    Ok(messages
        .into_iter()
        .map(|m| {
            let parent_message = m
                .parent
                .as_deref()
                .and_then(|p| parent_map.get(extract_rkey(p)))
                .cloned()
                .map(Box::new);
            MessageResponse { message: m, parent_message }
        })
        .collect())
}

/// Extract the rkey tail from an AT-URI (`at://did/collection/rkey`) or return as-is.
pub fn extract_rkey(s: &str) -> &str {
    s.rsplit('/').next().unwrap_or(s)
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

/// Returns true when the appview is caught up with real-time: average lag across
/// the 20 most recently indexed messages is less than 60 seconds.
pub async fn is_caught_up(pool: &PgPool) -> Result<bool> {
    let avg_lag: Option<f64> = sqlx::query_scalar(
        r#"
        SELECT AVG(EXTRACT(EPOCH FROM (indexed_at - created_at)))::FLOAT8
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
