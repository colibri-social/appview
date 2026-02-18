use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::models::{message::Message};

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

/// Returns a number of messages in a certain window.
pub async fn get_messages(
    pool: &PgPool,
    channel: &str,
    limit: i64,
    before: Option<DateTime<Utc>>,
) -> Result<Vec<Message>> {
    let messages = if let Some(before) = before {
        sqlx::query_as::<_, Message>(
            r#"
      SELECT id, rkey, author_did, text, channel, created_at, indexed_at
      FROM messages
      WHERE channel = $1 AND created_at < $2
      ORDER BY created_at DESC
      LIMIT $3
      "#,
        )
        .bind(channel)
        .bind(before)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, Message>(
            r#"
      SELECT id, rkey, author_did, text, channel, created_at, indexed_at
      FROM messages
      WHERE channel = $1
      ORDER BY created_at DESC
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
