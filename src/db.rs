use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::models::{
    author::AuthorProfile,
    channel_read::ChannelRead,
    community::{CommunitiesResponse, Community, InviteCodeInfo},
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
    attachments: Option<&serde_json::Value>,
    created_at: DateTime<Utc>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        INSERT INTO messages (rkey, author_did, text, channel, parent, facets, attachments, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (author_did, rkey) DO NOTHING
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets, attachments
        "#,
    )
    .bind(rkey)
    .bind(author_did)
    .bind(text)
    .bind(channel)
    .bind(parent)
    .bind(facets.map(|f| sqlx::types::Json(f)))
    .bind(attachments.map(|a| sqlx::types::Json(a)))
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
    attachments: Option<&serde_json::Value>,
) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        UPDATE messages
        SET text = $1, channel = $2, parent = $3, facets = $4, attachments = $5, edited = TRUE
        WHERE author_did = $6 AND rkey = $7
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets, attachments
        "#,
    )
    .bind(text)
    .bind(channel)
    .bind(parent)
    .bind(facets.map(|f| sqlx::types::Json(f)))
    .bind(attachments.map(|a| sqlx::types::Json(a)))
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
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets, attachments
        "#,
    )
    .bind(author_did)
    .bind(rkey)
    .fetch_optional(pool)
    .await?;

    Ok(msg)
}

/// Mark a message as blocked (hidden from all clients). Returns the message
/// metadata needed to emit a MessageDeleted event, or None if not found.
pub async fn block_message(pool: &PgPool, author_did: &str, rkey: &str) -> Result<Option<Message>> {
    let msg = sqlx::query_as::<_, Message>(
        r#"
        UPDATE messages SET blocked = TRUE
        WHERE author_did = $1 AND rkey = $2 AND NOT blocked
        RETURNING id, rkey, author_did, text, channel, created_at, indexed_at, edited, parent, facets, attachments
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
    all: Option<&str>,
) -> Result<Vec<MessageResponse>> {
    if all.is_some() {
        let messages: Vec<MessageWithAuthor> = sqlx::query_as::<_, MessageWithAuthor>(
            r#"
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets, m.attachments,
                   a.display_name, a.avatar_url, a.banner_url, a.description, a.handle, a.status AS status_text, a.emoji, a.state
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.channel = $1 AND NOT m.blocked
            ORDER BY m.created_at DESC
            "#,
        )
        .bind(channel)
        .fetch_all(pool)
        .await?;

        return enrich_messages(pool, messages).await;
    }

    let messages: Vec<MessageWithAuthor> = if let Some(before) = before {
        sqlx::query_as::<_, MessageWithAuthor>(
            r#"
            SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets, m.attachments,
                   a.display_name, a.avatar_url, a.banner_url, a.description, a.handle, a.status AS status_text, a.emoji, a.state
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.channel = $1 AND m.created_at < $2 AND NOT m.blocked
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
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets, m.attachments,
                   a.display_name, a.avatar_url, a.banner_url, a.description, a.handle, a.status AS status_text, a.emoji, a.state
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.channel = $1 AND NOT m.blocked
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
pub async fn get_message_by_rkey(pool: &PgPool, rkey: &str) -> Result<Option<MessageWithAuthor>> {
    let msg = sqlx::query_as::<_, MessageWithAuthor>(
        r#"
        SELECT m.id, m.rkey, m.author_did, m.text, m.channel,
               m.created_at, m.indexed_at, m.edited, m.parent, m.facets, m.attachments,
               a.display_name, a.avatar_url, a.banner_url, a.description, a.handle, a.status AS status_text, a.emoji, a.state
        FROM messages m
        LEFT JOIN author_profiles a ON m.author_did = a.did
        WHERE m.rkey = $1 AND NOT m.blocked
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
               m.created_at, m.indexed_at, m.edited, m.parent, m.facets, m.attachments,
               a.display_name, a.avatar_url, a.banner_url, a.description, a.handle, a.status AS status_text, a.emoji, a.state
        FROM messages m
        LEFT JOIN author_profiles a ON m.author_did = a.did
        WHERE m.author_did = $1 AND m.rkey = $2 AND NOT m.blocked
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
    struct Row {
        channel: String,
        author_did: String,
    }
    let row = sqlx::query_as::<_, Row>(
        "SELECT channel, author_did FROM messages WHERE rkey = $1 LIMIT 1",
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
                   m.created_at, m.indexed_at, m.edited, m.parent, m.facets, m.attachments,
                   a.display_name, a.avatar_url, a.banner_url, a.description, a.handle, a.status AS status_text, a.emoji, a.state
            FROM messages m
            LEFT JOIN author_profiles a ON m.author_did = a.did
            WHERE m.rkey = ANY($1) AND NOT m.blocked
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
            .push(ReactionSummary {
                emoji: row.emoji,
                count: row.count,
                authors: row.authors,
                rkeys: row.rkeys,
            });
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
            MessageResponse {
                message: m,
                parent_message,
                reactions,
            }
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

    Ok(rows
        .into_iter()
        .map(|r| ReactionSummary {
            emoji: r.emoji,
            count: r.count,
            authors: r.authors,
            rkeys: r.rkeys,
        })
        .collect())
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
            .push(ReactionSummary {
                emoji: row.emoji,
                count: row.count,
                authors: row.authors,
                rkeys: row.rkeys,
            });
    }
    Ok(map)
}

// ── Author profiles ───────────────────────────────────────────────────────────

/// Insert or refresh a cached author profile (display name + avatar only).
pub async fn upsert_author_profile(
    pool: &PgPool,
    did: &str,
    display_name: Option<&str>,
    avatar_url: Option<&str>,
    banner_url: Option<&str>,
    handle: Option<&str>,
    description: Option<&str>,
) -> Result<AuthorProfile> {
    let profile = sqlx::query_as::<_, AuthorProfile>(
        r#"
        INSERT INTO author_profiles (did, display_name, avatar_url, banner_url, handle, description)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (did) DO UPDATE
          SET display_name = EXCLUDED.display_name,
              avatar_url   = EXCLUDED.avatar_url,
              banner_url   = EXCLUDED.banner_url,
              handle       = COALESCE(EXCLUDED.handle, author_profiles.handle),
              description  = EXCLUDED.description,
              updated_at   = NOW()
        RETURNING did, display_name, avatar_url, banner_url, description, handle, status, emoji, state, preferred_state, updated_at
        "#,
    )
    .bind(did)
    .bind(display_name)
    .bind(avatar_url)
    .bind(banner_url)
    .bind(handle)
    .bind(description)
    .fetch_one(pool)
    .await?;

    Ok(profile)
}

/// Update (or insert) the actor status fields for a cached profile.
/// Only touches `status` / `emoji`; profile fields are unchanged.
pub async fn upsert_actor_status(
    pool: &PgPool,
    did: &str,
    status: &str,
    emoji: Option<&str>,
) -> Result<AuthorProfile> {
    let profile = sqlx::query_as::<_, AuthorProfile>(
        r#"
        INSERT INTO author_profiles (did, status, emoji)
        VALUES ($1, $2, $3)
        ON CONFLICT (did) DO UPDATE
          SET status     = EXCLUDED.status,
              emoji      = EXCLUDED.emoji,
              updated_at = NOW()
        RETURNING did, display_name, avatar_url, banner_url, description, handle, status, emoji, state, preferred_state, updated_at
        "#,
    )
    .bind(did)
    .bind(status)
    .bind(emoji)
    .fetch_one(pool)
    .await?;

    Ok(profile)
}

/// Retrieve a cached author profile. Returns `None` if never fetched.
pub async fn get_author_profile(pool: &PgPool, did: &str) -> Result<Option<AuthorProfile>> {
    let profile = sqlx::query_as::<_, AuthorProfile>(
        "SELECT did, display_name, avatar_url, banner_url, description, handle, status, emoji, state, preferred_state, updated_at FROM author_profiles WHERE did = $1",
    )
    .bind(did)
    .fetch_optional(pool)
    .await?;

    Ok(profile)
}

/// Set the current presence state for a user.
/// If `update_preferred` is true, also persists the state as `preferred_state`
/// (used when the user manually sets their state).
/// Returns the updated profile.
pub async fn set_user_state(
    pool: &PgPool,
    did: &str,
    state: &str,
    update_preferred: bool,
) -> Result<AuthorProfile> {
    let profile = if update_preferred {
        sqlx::query_as::<_, AuthorProfile>(
            r#"
            UPDATE author_profiles
               SET state           = $2,
                   preferred_state = $2,
                   updated_at      = NOW()
             WHERE did = $1
            RETURNING did, display_name, avatar_url, banner_url, description, handle, status, emoji, state, preferred_state, updated_at
            "#,
        )
        .bind(did)
        .bind(state)
        .fetch_one(pool)
        .await?
    } else {
        sqlx::query_as::<_, AuthorProfile>(
            r#"
            UPDATE author_profiles
               SET state      = $2,
                   updated_at = NOW()
             WHERE did = $1
            RETURNING did, display_name, avatar_url, banner_url, description, handle, status, emoji, state, preferred_state, updated_at
            "#,
        )
        .bind(did)
        .bind(state)
        .fetch_one(pool)
        .await?
    };
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
        SELECT DISTINCT known.did AS author_did, c.collection
        FROM (
            SELECT author_did AS did FROM messages
            UNION
            SELECT owner_did  AS did FROM communities
            UNION
            SELECT did        AS did FROM author_profiles
            UNION
            SELECT member_did AS did FROM community_members
        ) AS known
        CROSS JOIN (
            VALUES ('social.colibri.message'),
                   ('social.colibri.reaction'),
                   ('social.colibri.community'),
                   ('social.colibri.category'),
                   ('social.colibri.channel'),
                   ('social.colibri.membership'),
                   ('social.colibri.approval')
        ) AS c(collection)
        WHERE NOT EXISTS (
            SELECT 1 FROM backfill_state bs
            WHERE bs.did = known.did
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
pub async fn mark_backfill_complete(pool: &PgPool, did: &str, collection: &str) -> Result<()> {
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
    sqlx::query("UPDATE jetstream_cursor SET time_us = $1, updated_at = NOW() WHERE id = TRUE")
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

/// All distinct DIDs we know about: message authors + community owners + cached profiles.
pub async fn get_all_known_dids(pool: &PgPool) -> Result<Vec<String>> {
    let dids: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT did FROM (
            SELECT author_did AS did FROM messages
            UNION
            SELECT owner_did  AS did FROM communities
            UNION
            SELECT did        AS did FROM author_profiles
            UNION
            SELECT member_did AS did FROM community_members
        ) AS known
        ORDER BY did
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(dids)
}

// ── Communities ───────────────────────────────────────────────────────────────

/// Insert or replace a community record.
pub async fn save_community(
    pool: &PgPool,
    uri: &str,
    owner_did: &str,
    rkey: &str,
    name: &str,
    description: Option<&str>,
    picture: Option<&serde_json::Value>,
    category_order: Option<&serde_json::Value>,
    requires_approval_to_join: bool,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO communities (uri, owner_did, rkey, name, description, picture, category_order, requires_approval_to_join)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (uri) DO UPDATE
          SET name           = EXCLUDED.name,
              description    = EXCLUDED.description,
              picture        = EXCLUDED.picture,
              category_order = EXCLUDED.category_order,
              requires_approval_to_join = EXCLUDED.requires_approval_to_join
        "#,
    )
    .bind(uri)
    .bind(owner_did)
    .bind(rkey)
    .bind(name)
    .bind(description)
    .bind(picture.map(|v| sqlx::types::Json(v)))
    .bind(category_order.map(|v| sqlx::types::Json(v)))
    .bind(requires_approval_to_join)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete a community record.
pub async fn delete_community(pool: &PgPool, uri: &str) -> Result<()> {
    sqlx::query("DELETE FROM communities WHERE uri = $1")
        .bind(uri)
        .execute(pool)
        .await?;
    Ok(())
}

/// Delete any community rows owned by `owner_did` whose rkey is NOT in `live_rkeys`.
/// Used by backfill to prune communities deleted from the PDS.
/// Returns the URIs of deleted communities.
pub async fn prune_communities_for_owner(
    pool: &PgPool,
    owner_did: &str,
    live_rkeys: &[String],
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "DELETE FROM communities WHERE owner_did = $1 AND rkey <> ALL($2) RETURNING uri",
    )
    .bind(owner_did)
    .bind(live_rkeys)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(uri,)| uri).collect())
}

// ── Channels ──────────────────────────────────────────────────────────────────

/// Insert or replace a channel record.
pub async fn save_channel(
    pool: &PgPool,
    uri: &str,
    owner_did: &str,
    rkey: &str,
    community_uri: &str,
    name: &str,
    description: Option<&str>,
    channel_type: &str,
    category_rkey: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO channels (uri, owner_did, rkey, community_uri, name, description, channel_type, category_rkey)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (uri) DO UPDATE
          SET name = EXCLUDED.name,
              description = EXCLUDED.description,
              channel_type = EXCLUDED.channel_type,
              category_rkey = EXCLUDED.category_rkey
        "#,
    )
    .bind(uri)
    .bind(owner_did)
    .bind(rkey)
    .bind(community_uri)
    .bind(name)
    .bind(description)
    .bind(channel_type)
    .bind(category_rkey)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delete a channel record.
/// Fetch channel community_uri by URI (used before deleting, for event emission).
pub async fn get_channel_community_by_uri(pool: &PgPool, uri: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT community_uri FROM channels WHERE uri = $1 LIMIT 1")
            .bind(uri)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(v,)| v))
}

pub async fn delete_channel(pool: &PgPool, uri: &str) -> Result<()> {
    sqlx::query("DELETE FROM channels WHERE uri = $1")
        .bind(uri)
        .execute(pool)
        .await?;
    Ok(())
}

/// Delete any channel rows owned by `owner_did` whose rkey is NOT in `live_rkeys`.
/// Returns the URIs of deleted channels.
pub async fn prune_channels_for_owner(
    pool: &PgPool,
    owner_did: &str,
    live_rkeys: &[String],
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "DELETE FROM channels WHERE owner_did = $1 AND rkey <> ALL($2) RETURNING uri",
    )
    .bind(owner_did)
    .bind(live_rkeys)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(uri,)| uri).collect())
}

/// Look up the community URI and owner DID for a channel by its rkey.
/// Returns None if the channel is not yet indexed.
pub async fn get_community_for_channel(
    pool: &PgPool,
    channel_rkey: &str,
) -> Result<Option<(String, String, bool)>> {
    let row: Option<(String, String, bool)> = sqlx::query_as(
        r#"
        SELECT c.community_uri, c.owner_did, co.requires_approval_to_join
          FROM channels c
          JOIN communities co ON co.uri = c.community_uri
         WHERE c.rkey = $1
         LIMIT 1
        "#,
    )
    .bind(channel_rkey)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

// ── Membership ────────────────────────────────────────────────────────────────

/// Record a pending membership request (from the member's PDS).
pub async fn save_membership(
    pool: &PgPool,
    membership_uri: &str,
    community_uri: &str,
    member_did: &str,
    membership_rkey: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO community_members
            (community_uri, member_did, membership_rkey, membership_uri, status)
        VALUES ($1, $2, $3, $4, 'pending')
        ON CONFLICT (membership_uri) DO NOTHING
        "#,
    )
    .bind(community_uri)
    .bind(member_did)
    .bind(membership_rkey)
    .bind(membership_uri)
    .execute(pool)
    .await?;

    // Drain any staged approval that arrived before this membership row existed.
    sqlx::query(
        r#"
        WITH staged AS (
            DELETE FROM pending_approvals WHERE membership_uri = $1 RETURNING approval_uri, approval_rkey
        )
        UPDATE community_members
           SET approval_uri  = staged.approval_uri,
               approval_rkey = staged.approval_rkey,
               status        = 'approved'
          FROM staged
         WHERE community_members.membership_uri = $1
        "#,
    )
    .bind(membership_uri)
    .execute(pool)
    .await?;

    Ok(())
}

/// Remove a membership row when the member's record is deleted.
pub async fn delete_membership(pool: &PgPool, membership_uri: &str) -> Result<()> {
    sqlx::query("DELETE FROM community_members WHERE membership_uri = $1")
        .bind(membership_uri)
        .execute(pool)
        .await?;
    Ok(())
}

/// Ban a DID from participating in a community.
pub async fn ban_member_from_community(
    pool: &PgPool,
    community_uri: &str,
    member_did: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO community_bans (community_uri, member_did) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(community_uri)
    .bind(member_did)
    .execute(pool)
    .await?;
    Ok(())
}

/// Unban a DID from a community. Returns true if a row was deleted.
pub async fn unban_member_from_community(
    pool: &PgPool,
    community_uri: &str,
    member_did: &str,
) -> Result<bool> {
    let rows =
        sqlx::query("DELETE FROM community_bans WHERE community_uri = $1 AND member_did = $2")
            .bind(community_uri)
            .bind(member_did)
            .execute(pool)
            .await?
            .rows_affected();
    Ok(rows > 0)
}

/// Return true if a DID is banned from a community.
pub async fn is_member_banned(
    pool: &PgPool,
    community_uri: &str,
    member_did: &str,
) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM community_bans WHERE community_uri = $1 AND member_did = $2)",
    )
    .bind(community_uri)
    .bind(member_did)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Mark approved members without approval URIs as pending and return their DIDs/URIs.
pub async fn mark_members_without_approval_pending(
    pool: &PgPool,
    community_uri: &str,
) -> Result<Vec<(String, String)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        UPDATE community_members
           SET status = 'pending'
         WHERE community_uri = $1
           AND approval_uri IS NULL
           AND status = 'approved'
         RETURNING member_did, membership_uri
        "#,
    )
    .bind(community_uri)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Look up (community_uri, member_did) for a membership URI.
pub async fn get_membership_info(
    pool: &PgPool,
    membership_uri: &str,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT community_uri, member_did FROM community_members WHERE membership_uri = $1 LIMIT 1",
    )
    .bind(membership_uri)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Look up (community_uri, member_did) by approval URI.
pub async fn get_membership_info_by_approval(
    pool: &PgPool,
    approval_uri: &str,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT community_uri, member_did FROM community_members WHERE approval_uri = $1 LIMIT 1",
    )
    .bind(approval_uri)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Upgrade a pending membership to approved when the owner writes an approval.
pub async fn save_approval(
    pool: &PgPool,
    approval_uri: &str,
    membership_uri: &str,
    approval_rkey: &str,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        UPDATE community_members
           SET approval_uri  = $1,
               approval_rkey = $2,
               status        = 'approved'
         WHERE membership_uri = $3
        "#,
    )
    .bind(approval_uri)
    .bind(approval_rkey)
    .bind(membership_uri)
    .execute(pool)
    .await?
    .rows_affected();

    // Membership row doesn't exist yet — stage the approval so save_membership
    // can pick it up when the membership record arrives.
    if rows == 0 {
        sqlx::query(
            r#"
            INSERT INTO pending_approvals (approval_uri, membership_uri, approval_rkey)
            VALUES ($1, $2, $3)
            ON CONFLICT (approval_uri) DO NOTHING
            "#,
        )
        .bind(approval_uri)
        .bind(membership_uri)
        .bind(approval_rkey)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Revert an approval to pending when the owner deletes their approval record.
pub async fn delete_approval(pool: &PgPool, approval_uri: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE community_members
           SET approval_uri  = NULL,
               approval_rkey = NULL,
               status        = 'pending'
         WHERE approval_uri = $1
        "#,
    )
    .bind(approval_uri)
    .execute(pool)
    .await?;
    Ok(())
}

/// Return true if `did` has an approved membership in `community_uri`.
pub async fn is_approved_member(pool: &PgPool, community_uri: &str, did: &str) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM community_members
             WHERE community_uri = $1
               AND member_did    = $2
               AND status        = 'approved'
        )
        "#,
    )
    .bind(community_uri)
    .bind(did)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Return true if `did` has submitted a membership declaration for `community_uri`.
pub async fn has_membership_declaration(
    pool: &PgPool,
    community_uri: &str,
    did: &str,
) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM community_members
             WHERE community_uri = $1
               AND member_did    = $2
        )
        "#,
    )
    .bind(community_uri)
    .bind(did)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Look up a single community by AT-URI or rkey.
pub async fn get_community(pool: &PgPool, uri_or_rkey: &str) -> Result<Option<Community>> {
    let community = sqlx::query_as::<_, Community>(
        r#"
        SELECT uri, owner_did, rkey, name, description, picture, category_order, requires_approval_to_join
          FROM communities
         WHERE uri = $1 OR rkey = $1
         LIMIT 1
        "#,
    )
    .bind(uri_or_rkey)
    .fetch_optional(pool)
    .await?;
    Ok(community)
}

/// Return all communities owned by or joined (approved) by a DID.
pub async fn get_communities_for_user(pool: &PgPool, did: &str) -> Result<CommunitiesResponse> {
    let owned = sqlx::query_as::<_, Community>(
        "SELECT uri, owner_did, rkey, name, description, picture, category_order, requires_approval_to_join FROM communities WHERE owner_did = $1",
    )
    .bind(did)
    .fetch_all(pool)
    .await?;

    let joined = sqlx::query_as::<_, Community>(
        r#"
        SELECT DISTINCT ON (c.uri)
               c.uri, c.owner_did, c.rkey, c.name, c.description, c.picture, c.category_order, c.requires_approval_to_join
          FROM communities c
          JOIN community_members cm ON cm.community_uri = c.uri
         WHERE cm.member_did = $1
           AND cm.status     = 'approved'
        "#,
    )
    .bind(did)
    .fetch_all(pool)
    .await?;

    Ok(CommunitiesResponse { owned, joined })
}

/// Upsert a channel read cursor for a DID.
pub async fn upsert_channel_read(
    pool: &PgPool,
    did: &str,
    channel_uri: &str,
    cursor_at: DateTime<Utc>,
    record_rkey: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO channel_reads (did, channel_uri, cursor_at, record_rkey)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (did, channel_uri) DO UPDATE
          SET cursor_at   = EXCLUDED.cursor_at,
              record_rkey = EXCLUDED.record_rkey,
              updated_at  = NOW()
        "#,
    )
    .bind(did)
    .bind(channel_uri)
    .bind(cursor_at)
    .bind(record_rkey)
    .execute(pool)
    .await?;
    Ok(())
}

/// Remove a channel read cursor when the record is deleted.
pub async fn delete_channel_read(pool: &PgPool, did: &str, record_rkey: &str) -> Result<()> {
    sqlx::query("DELETE FROM channel_reads WHERE did = $1 AND record_rkey = $2")
        .bind(did)
        .bind(record_rkey)
        .execute(pool)
        .await?;
    Ok(())
}

/// Return all channel reads for a DID.
pub async fn get_channel_reads(pool: &PgPool, did: &str) -> Result<Vec<ChannelRead>> {
    let reads = sqlx::query_as::<_, ChannelRead>(
        r#"
        SELECT channel_uri, cursor_at
          FROM channel_reads
         WHERE did = $1
         ORDER BY cursor_at DESC
        "#,
    )
    .bind(did)
    .fetch_all(pool)
    .await?;
    Ok(reads)
}

// ── Invite codes ──────────────────────────────────────────────────────────────

/// Create a new invite code for a community.  Returns the generated code.
/// Verifies that `owner_did` is the registered owner of `community_uri`.
pub async fn create_invite_code(
    pool: &PgPool,
    community_uri: &str,
    owner_did: &str,
    max_uses: Option<i32>,
) -> Result<Option<String>> {
    // Verify ownership.
    let is_owner: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM communities WHERE uri = $1 AND owner_did = $2)",
    )
    .bind(community_uri)
    .bind(owner_did)
    .fetch_one(pool)
    .await?;

    if !is_owner {
        return Ok(None);
    }

    let code = Uuid::new_v4().to_string().replace('-', "")[..16].to_string();

    sqlx::query(
        r#"
        INSERT INTO invite_codes (code, community_uri, created_by_did, max_uses)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(&code)
    .bind(community_uri)
    .bind(owner_did)
    .bind(max_uses)
    .execute(pool)
    .await?;

    Ok(Some(code))
}

/// Fetch invite code details along with the community info.
pub async fn get_invite_code(
    pool: &PgPool,
    code: &str,
) -> Result<Option<(InviteCodeInfo, Community)>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        code: String,
        community_uri: String,
        created_by_did: String,
        max_uses: Option<i32>,
        use_count: i32,
        active: bool,
        // community fields
        c_uri: String,
        c_owner_did: String,
        c_rkey: String,
        c_name: String,
        c_description: Option<String>,
        c_picture: Option<sqlx::types::Json<serde_json::Value>>,
        c_category_order: Option<sqlx::types::Json<serde_json::Value>>,
        c_requires_approval_to_join: bool,
    }

    let row = sqlx::query_as::<_, Row>(
        r#"
            SELECT i.code, i.community_uri, i.created_by_did, i.max_uses, i.use_count, i.active,
                   c.uri AS c_uri, c.owner_did AS c_owner_did, c.rkey AS c_rkey,
                   c.name AS c_name, c.description AS c_description,
                   c.picture AS c_picture, c.category_order AS c_category_order,
                   c.requires_approval_to_join AS c_requires_approval_to_join
              FROM invite_codes i
              JOIN communities c ON c.uri = i.community_uri
             WHERE i.code = $1
            "#,
    )
    .bind(code)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| {
        (
            InviteCodeInfo {
                code: r.code,
                community_uri: r.community_uri,
                created_by_did: r.created_by_did,
                max_uses: r.max_uses,
                use_count: r.use_count,
                active: r.active,
            },
            Community {
                uri: r.c_uri,
                owner_did: r.c_owner_did,
                rkey: r.c_rkey,
                name: r.c_name,
                description: r.c_description,
                picture: r.c_picture,
                category_order: r.c_category_order,
                requires_approval_to_join: r.c_requires_approval_to_join,
            },
        )
    }))
}

/// Deactivate an invite code.  Returns false if the code doesn't exist or
/// `owner_did` is not the owner.
pub async fn revoke_invite_code(pool: &PgPool, code: &str, owner_did: &str) -> Result<bool> {
    let rows_affected = sqlx::query(
        "UPDATE invite_codes SET active = FALSE WHERE code = $1 AND created_by_did = $2",
    )
    .bind(code)
    .bind(owner_did)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(rows_affected > 0)
}

/// Return all invite codes for a community, ordered newest first.
pub async fn get_invite_codes_for_community(
    pool: &PgPool,
    community_uri: &str,
) -> Result<Vec<InviteCodeInfo>> {
    let codes = sqlx::query_as::<_, InviteCodeInfo>(
        r#"
        SELECT code, community_uri, created_by_did, max_uses, use_count, active
        FROM invite_codes
        WHERE community_uri = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(community_uri)
    .fetch_all(pool)
    .await?;

    Ok(codes)
}

/// Increment the use counter for a code and return whether it is still usable.
/// Returns false if the code is inactive or has hit max_uses.
pub async fn use_invite_code(pool: &PgPool, code: &str) -> Result<bool> {
    #[derive(sqlx::FromRow)]
    struct Row {
        active: bool,
        max_uses: Option<i32>,
        use_count: i32,
    }

    let row = sqlx::query_as::<_, Row>(
        "SELECT active, max_uses, use_count FROM invite_codes WHERE code = $1",
    )
    .bind(code)
    .fetch_optional(pool)
    .await?;

    let row = match row {
        Some(r) => r,
        None => return Ok(false),
    };

    if !row.active {
        return Ok(false);
    }
    if let Some(max) = row.max_uses {
        if row.use_count >= max {
            return Ok(false);
        }
    }

    sqlx::query("UPDATE invite_codes SET use_count = use_count + 1 WHERE code = $1")
        .bind(code)
        .execute(pool)
        .await?;

    Ok(true)
}

/// Return all channels for a community, ordered by name.
pub async fn get_channels_for_community(
    pool: &PgPool,
    community_uri: &str,
) -> Result<Vec<crate::models::community::Channel>> {
    let channels = sqlx::query_as::<_, crate::models::community::Channel>(
        r#"
        SELECT uri, rkey, community_uri, name, description, channel_type, category_rkey
          FROM channels
         WHERE community_uri = $1
         ORDER BY name
        "#,
    )
    .bind(community_uri)
    .fetch_all(pool)
    .await?;
    Ok(channels)
}

// ── Categories ────────────────────────────────────────────────────────────────

pub async fn save_category(
    pool: &PgPool,
    uri: &str,
    owner_did: &str,
    rkey: &str,
    community_uri: &str,
    name: &str,
    channel_order: Option<&serde_json::Value>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO categories (uri, owner_did, rkey, community_uri, name, channel_order)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (uri) DO UPDATE
           SET name          = EXCLUDED.name,
               channel_order = EXCLUDED.channel_order
        "#,
    )
    .bind(uri)
    .bind(owner_did)
    .bind(rkey)
    .bind(community_uri)
    .bind(name)
    .bind(channel_order.map(|v| sqlx::types::Json(v)))
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_category(pool: &PgPool, uri: &str) -> Result<()> {
    sqlx::query("DELETE FROM categories WHERE uri = $1")
        .bind(uri)
        .execute(pool)
        .await?;
    Ok(())
}

/// Delete any category rows owned by `owner_did` whose rkey is NOT in `live_rkeys`.
/// Returns the URIs of deleted categories.
pub async fn prune_categories_for_owner(
    pool: &PgPool,
    owner_did: &str,
    live_rkeys: &[String],
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "DELETE FROM categories WHERE owner_did = $1 AND rkey <> ALL($2) RETURNING uri",
    )
    .bind(owner_did)
    .bind(live_rkeys)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(uri,)| uri).collect())
}

/// Return all categories for a community, ordered by name.
pub async fn get_categories_for_community(
    pool: &PgPool,
    community_uri: &str,
) -> Result<Vec<crate::models::community::Category>> {
    let cats = sqlx::query_as::<_, crate::models::community::Category>(
        r#"
        SELECT uri, rkey, community_uri, name, channel_order
          FROM categories
         WHERE community_uri = $1
         ORDER BY name
        "#,
    )
    .bind(community_uri)
    .fetch_all(pool)
    .await?;
    Ok(cats)
}

/// Return channels and categories combined into a sidebar-ready structure.
/// Channels within each category are ordered by the category's `channelOrder` array.
pub async fn get_sidebar_for_community(
    pool: &PgPool,
    community_uri: &str,
) -> Result<crate::models::community::SidebarResponse> {
    use crate::models::community::{SidebarCategory, SidebarResponse};

    let categories = get_categories_for_community(pool, community_uri).await?;
    let channels = get_channels_for_community(pool, community_uri).await?;

    let sidebar_cats: Vec<SidebarCategory> = categories
        .into_iter()
        .map(|cat| {
            // Build the ordered channel list using channelOrder if present.
            let mut cat_channels: Vec<_> = channels
                .iter()
                .filter(|ch| ch.category_rkey.as_deref() == Some(&cat.rkey))
                .cloned()
                .collect();

            if let Some(order) = cat.channel_order.as_ref().and_then(|j| j.as_array()) {
                let order_map: std::collections::HashMap<&str, usize> = order
                    .iter()
                    .enumerate()
                    .filter_map(|(i, v)| v.as_str().map(|s| (s, i)))
                    .collect();
                cat_channels.sort_by_key(|ch| {
                    order_map
                        .get(ch.rkey.as_str())
                        .copied()
                        .unwrap_or(usize::MAX)
                });
            }

            SidebarCategory {
                uri: cat.uri,
                rkey: cat.rkey,
                name: cat.name,
                channel_order: cat.channel_order,
                channels: cat_channels,
            }
        })
        .collect();

    let uncategorized = channels
        .into_iter()
        .filter(|ch| ch.category_rkey.is_none())
        .collect();

    Ok(SidebarResponse {
        categories: sidebar_cats,
        uncategorized,
    })
}
/// Return all members (pending + approved) for a community, enriched with
/// cached profile data where available. The community owner is always included.
/// Returns just the member DIDs for a community (owner + approved/pending members).
/// Lightweight version used to build WS subscription watch-lists.
pub async fn get_member_dids_for_community(
    pool: &PgPool,
    community_uri: &str,
) -> Result<Vec<String>> {
    let dids = sqlx::query_scalar(
        r#"
        SELECT DISTINCT did FROM (
            SELECT member_did AS did FROM community_members WHERE community_uri = $1
            UNION
            SELECT owner_did  AS did FROM communities        WHERE uri          = $1
        ) combined
        "#,
    )
    .bind(community_uri)
    .fetch_all(pool)
    .await?;
    Ok(dids)
}

pub async fn get_members_for_community(
    pool: &PgPool,
    community_uri: &str,
) -> Result<Vec<crate::models::community::CommunityMember>> {
    let members = sqlx::query_as::<_, crate::models::community::CommunityMember>(
        r#"
        -- Always include the owner (status = 'owner'), then one row per unique member
        -- (prefer approved over pending when a member has multiple declarations).
        SELECT co.owner_did  AS member_did,
               'owner'       AS status,
               ap.display_name,
               ap.avatar_url,
               ap.banner_url,
               ap.description,
               ap.handle,
               ap.status     AS status_text,
               ap.emoji,
               ap.state
          FROM communities co
          LEFT JOIN author_profiles ap ON ap.did = co.owner_did
         WHERE co.uri = $1

        UNION ALL

        SELECT member_did, status, display_name, avatar_url, banner_url, description, handle, status_text, emoji, state
          FROM (
              SELECT DISTINCT ON (cm.member_did)
                     cm.member_did,
                     cm.status,
                     ap.display_name,
                     ap.avatar_url,
                     ap.banner_url,
               ap.description,
                     ap.handle,
                     ap.status AS status_text,
                     ap.emoji,
                     ap.state
                FROM community_members cm
                LEFT JOIN author_profiles ap ON ap.did = cm.member_did
               WHERE cm.community_uri = $2
               ORDER BY cm.member_did,
                        CASE WHEN cm.status = 'approved' THEN 0 ELSE 1 END
          ) deduped
         ORDER BY status, member_did
        "#,
    )
    .bind(community_uri)
    .bind(community_uri)
    .fetch_all(pool)
    .await?;
    Ok(members)
}
