use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::models::{message::Message, user::UserStatus};

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

pub async fn delete_message(pool: &PgPool, author_did: &str, rkey: &str) -> Result<()> {
    sqlx::query("DELETE FROM messages WHERE author_did = $1 AND rkey = $2")
        .bind(author_did)
        .bind(rkey)
        .execute(pool)
        .await?;
    Ok(())
}

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

pub async fn upsert_user_status(pool: &PgPool, did: &str, status: &str) -> Result<UserStatus> {
    let user_status = sqlx::query_as::<_, UserStatus>(
        r#"
        INSERT INTO user_status (did, status)
        VALUES ($1, $2)
        ON CONFLICT (did) DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
        RETURNING did, status, updated_at
        "#,
    )
    .bind(did)
    .bind(status)
    .fetch_one(pool)
    .await?;

    Ok(user_status)
}
