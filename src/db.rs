use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use sqlx::PgPool;

use crate::models::{
    author::AuthorProfile,
    message::{Message, MessageResponse, MessageWithAuthor},
    reaction::{Reaction, ReactionSummary},
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
    facets: Option<&serde_json::Value>,
    created_at: DateTime<Utc>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        INSERT INTO messages (rkey, author_did, text, channel, parent, facets, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (author_did, rkey) DO NOTHING
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets
        "#,
    )
    .bind(rkey)
    .bind(author_did)
    .bind(text)
    .bind(channel)
    .bind(parent)
    .bind(facets.map(|f| sqlx::types::Json(f)))
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
    facets: Option<&serde_json::Value>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        UPDATE messages
        SET text = $1, channel = $2, parent = $3, facets = $4, edited = TRUE
        WHERE author_did = $5 AND rkey = $6
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets
        "#,
    )
    .bind(text)
    .bind(channel)
    .bind(parent)
    .bind(facets.map(|f| sqlx::types::Json(f)))
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
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets
        "#,
    )
    .bind(author_did)
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Paginated messages for a channel, enriched with author profiles, parent messages,
/// and grouped reactions.
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
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets,
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
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets,
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

    enrich_messages(pool, messages).await
}

/// Look up a single message by rkey (first match), with author profile.
pub async fn get_message_by_rkey(
    pool: &PgPool,
    rkey: &str,
) -> Result<Option<MessageWithAuthor>> {
    let msg = sqlx::query_as::<_, MessageWithAuthor>(
        r#"
        SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
               m.created_at, m.indexed_at, m.edited, m.parent, m.facets,
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

/// Look up a single message by author DID + rkey, fully enriched.
pub async fn get_message_by_author_and_rkey(
    pool: &PgPool,
    author_did: &str,
    rkey: &str,
) -> Result<Option<MessageResponse>> {
    let msg = sqlx::query_as::<_, MessageWithAuthor>(
        r#"
        SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
               m.created_at, m.indexed_at, m.edited, m.parent, m.facets,
               a.display_name, a.avatar_url
        FROM messages m
        LEFT JOIN author_profiles a ON m.author_did = a.did
        WHERE m.author_did = $1 AND m.rkey = $2
        LIMIT 1
        "#,
    )
    .bind(author_did)
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    match msg {
        None => Ok(None),
        Some(m) => {
            let mut enriched = enrich_messages(pool, vec![m]).await?;
            Ok(enriched.pop())
        }
    }
}

/// Return the channel and author_did of a message by its rkey, or None if not found.
pub async fn get_message_channel(pool: &PgPool, rkey: &str) -> Result<Option<(String, String)>> {
    #[derive(sqlx::FromRow)]
    struct Row { channel: String, author_did: String }
    let row = sqlx::query_as::<_, Row>(
        "SELECT channel, author_did FROM messages WHERE rkey = $1 LIMIT 1"
    )
    .bind(rkey)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| (r.channel, r.author_did)))
}

/// Populate parent messages and reactions for a batch, producing full MessageResponses.
pub async fn enrich_messages(
    pool: &PgPool,
    messages: Vec<MessageWithAuthor>,
) -> Result<Vec<MessageResponse>> {
    if messages.is_empty() {
        return Ok(vec![]);
    }

    let rkeys: Vec<&str> = messages.iter().map(|m| m.rkey.as_str()).collect();

    // ── Parent messages ───────────────────────────────────────────────────────
    let parent_rkeys: Vec<String> = messages
        .iter()
        .filter_map(|m| m.parent.as_deref())
        .map(|p| extract_rkey(p).to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let parent_map: HashMap<String, MessageWithAuthor> = if !parent_rkeys.is_empty() {
        sqlx::query_as::<_, MessageWithAuthor>(
            r#"
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets,
                   a.display_name, a.avatar_url
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.rkey = ANY($1)
            "#,
        )
        .bind(&parent_rkeys[..])
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|p| (p.rkey.clone(), p))
        .collect()
    } else {
        HashMap::new()
    };

    // ── Reactions (grouped by rkey + emoji) ───────────────────────────────────
    #[derive(sqlx::FromRow)]
    struct ReactionGroupRow {
        target_rkey: String,
        emoji: String,
        count: i64,
        authors: Vec<String>,
        rkeys: Vec<String>,
    }

    let reaction_rows: Vec<ReactionGroupRow> = sqlx::query_as::<_, ReactionGroupRow>(
        r#"
        SELECT target_rkey, emoji,
               COUNT(*)::BIGINT AS count,
               ARRAY_AGG(author_did ORDER BY created_at) AS authors,
               ARRAY_AGG(rkey ORDER BY created_at) AS rkeys
        FROM reactions
        WHERE target_rkey = ANY($1)
        GROUP BY target_rkey, emoji
        ORDER BY target_rkey, count DESC
        "#,
    )
    .bind(&rkeys[..])
    .fetch_all(pool)
    .await?;

    let mut reaction_map: HashMap<String, Vec<ReactionSummary>> = HashMap::new();
    for row in reaction_rows {
        reaction_map
            .entry(row.target_rkey)
            .or_default()
            .push(ReactionSummary { emoji: row.emoji, count: row.count, authors: row.authors, rkeys: row.rkeys });
    }

    // ── Assemble responses ────────────────────────────────────────────────────
    Ok(messages
        .into_iter()
        .map(|m| {
            let parent_message = m
                .parent
                .as_deref()
                .and_then(|p| parent_map.get(extract_rkey(p)))
                .cloned()
                .map(Box::new);
            let reactions = reaction_map.remove(&m.rkey).unwrap_or_default();
            MessageResponse { message: m, parent_message, reactions }
        })
        .collect())
}

/// Extract the rkey tail from an AT-URI (`at://did/collection/rkey`) or return as-is.
pub fn extract_rkey(s: &str) -> &str {
    s.rsplit('/').next().unwrap_or(s)
}

// ── Reactions ─────────────────────────────────────────────────────────────────

/// Insert a new reaction (no-op on duplicate author_did+rkey).
pub async fn save_reaction(
    pool: &PgPool,
    rkey: &str,
    author_did: &str,
    emoji: &str,
    target_rkey: &str,
    created_at: DateTime<Utc>,
) -> Result<Option<Reaction>> {
    let r = sqlx::query_as::<_, Reaction>(
        r#"
        INSERT INTO reactions (rkey, author_did, emoji, target_rkey, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (author_did, target_rkey, emoji) DO NOTHING
        RETURNING id, rkey, author_did, emoji, target_rkey, created_at
        "#,
    )
    .bind(rkey)
    .bind(author_did)
    .bind(emoji)
    .bind(target_rkey)
    .bind(created_at)
    .fetch_optional(pool)
    .await?;

    Ok(r)
}

/// Delete a reaction, returning the deleted row.
pub async fn delete_reaction(
    pool: &PgPool,
    author_did: &str,
    rkey: &str,
) -> Result<Option<Reaction>> {
    let r = sqlx::query_as::<_, Reaction>(
        r#"
        DELETE FROM reactions
        WHERE author_did = $1 AND rkey = $2
        RETURNING id, rkey, author_did, emoji, target_rkey, created_at
        "#,
    )
    .bind(author_did)
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    Ok(r)
}

/// Grouped reactions for a single message.
pub async fn get_reactions_for_message(
    pool: &PgPool,
    target_rkey: &str,
) -> Result<Vec<ReactionSummary>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        emoji: String,
        count: i64,
        authors: Vec<String>,
        rkeys: Vec<String>,
    }

    let rows = sqlx::query_as::<_, Row>(
        r#"
        SELECT emoji,
               COUNT(*)::BIGINT AS count,
               ARRAY_AGG(author_did ORDER BY created_at) AS authors,
               ARRAY_AGG(rkey ORDER BY created_at) AS rkeys
        FROM reactions
        WHERE target_rkey = $1
        GROUP BY emoji
        ORDER BY count DESC
        "#,
    )
    .bind(target_rkey)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| ReactionSummary { emoji: r.emoji, count: r.count, authors: r.authors, rkeys: r.rkeys }).collect())
}

/// All reactions in a channel, grouped by target message rkey then by emoji.
/// Returns a map of `target_rkey → Vec<ReactionSummary>`.
pub async fn get_reactions_for_channel(
    pool: &PgPool,
    channel: &str,
) -> Result<HashMap<String, Vec<ReactionSummary>>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        target_rkey: String,
        emoji: String,
        count: i64,
        authors: Vec<String>,
        rkeys: Vec<String>,
    }

    let rows = sqlx::query_as::<_, Row>(
        r#"
        SELECT r.target_rkey, r.emoji,
               COUNT(*)::BIGINT AS count,
               ARRAY_AGG(r.author_did ORDER BY r.created_at) AS authors,
               ARRAY_AGG(r.rkey ORDER BY r.created_at) AS rkeys
        FROM reactions r
        INNER JOIN messages m ON m.rkey = r.target_rkey
        WHERE m.channel = $1
        GROUP BY r.target_rkey, r.emoji
        ORDER BY r.target_rkey, count DESC
        "#,
    )
    .bind(channel)
    .fetch_all(pool)
    .await?;

    let mut map: HashMap<String, Vec<ReactionSummary>> = HashMap::new();
    for row in rows {
        map.entry(row.target_rkey)
            .or_default()
            .push(ReactionSummary { emoji: row.emoji, count: row.count, authors: row.authors, rkeys: row.rkeys });
    }
    Ok(map)
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

/// Returns true when the appview is caught up with real-time.
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

    // No messages yet → treat as caught up so backfill can start immediately.
    Ok(avg_lag.unwrap_or(0.0) < 60.0)
}

/// Return the next (did, collection) pair that still needs backfilling.
/// Considers both social.colibri.message and social.colibri.reaction for every
/// known author DID.
pub async fn get_next_backfill_item(pool: &PgPool) -> Result<Option<(String, String)>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        author_did: String,
        collection: String,
    }

    let row = sqlx::query_as::<_, Row>(
        r#"
        SELECT DISTINCT m.author_did, c.collection
        FROM messages m
        CROSS JOIN (
            VALUES ('social.colibri.message'),
                   ('social.colibri.reaction')
        ) AS c(collection)
        WHERE NOT EXISTS (
            SELECT 1 FROM backfill_state bs
            WHERE bs.did = m.author_did
              AND bs.collection = c.collection
              AND bs.completed = TRUE
        )
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| (r.author_did, r.collection)))
}

/// Return the stored cursor for a (did, collection) backfill.
pub async fn get_backfill_cursor(
    pool: &PgPool,
    did: &str,
    collection: &str,
) -> Result<Option<String>> {
    let cursor: Option<Option<String>> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT cursor FROM backfill_state WHERE did = $1 AND collection = $2 AND completed = FALSE",
    )
    .bind(did)
    .bind(collection)
    .fetch_optional(pool)
    .await?;

    Ok(cursor.flatten())
}

/// Persist the pagination cursor mid-backfill.
pub async fn set_backfill_cursor(
    pool: &PgPool,
    did: &str,
    collection: &str,
    cursor: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backfill_state (did, collection, completed, cursor)
        VALUES ($1, $2, FALSE, $3)
        ON CONFLICT (did, collection) DO UPDATE
          SET cursor = EXCLUDED.cursor, completed = FALSE, updated_at = NOW()
        "#,
    )
    .bind(did)
    .bind(collection)
    .bind(cursor)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a (did, collection) backfill as complete.
pub async fn mark_backfill_complete(
    pool: &PgPool,
    did: &str,
    collection: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backfill_state (did, collection, completed, cursor)
        VALUES ($1, $2, TRUE, NULL)
        ON CONFLICT (did, collection) DO UPDATE
          SET completed = TRUE, cursor = NULL, updated_at = NOW()
        "#,
    )
    .bind(did)
    .bind(collection)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Jetstream cursor ──────────────────────────────────────────────────────────

/// Read the persisted Jetstream `time_us` cursor (0 = none stored yet).
pub async fn get_jetstream_cursor(pool: &PgPool) -> Result<i64> {
    let ts: i64 = sqlx::query_scalar("SELECT time_us FROM jetstream_cursor WHERE id = TRUE")
        .fetch_one(pool)
        .await?;
    Ok(ts)
}

/// Persist the latest Jetstream `time_us` cursor.
pub async fn set_jetstream_cursor(pool: &PgPool, time_us: i64) -> Result<()> {
    sqlx::query(
        "UPDATE jetstream_cursor SET time_us = $1, updated_at = NOW() WHERE id = TRUE",
    )
    .bind(time_us)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Sweep helpers ─────────────────────────────────────────────────────────────

/// Timestamp of the most recently indexed message (for lag detection).
pub async fn get_last_indexed_at(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let ts: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT MAX(indexed_at) FROM messages")
            .fetch_one(pool)
            .await?;
    Ok(ts)
}

/// All distinct author DIDs we have ever indexed a message for.
pub async fn get_all_known_dids(pool: &PgPool) -> Result<Vec<String>> {
    let dids: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT author_did FROM messages ORDER BY author_did")
            .fetch_all(pool)
            .await?;
    Ok(dids)
}
