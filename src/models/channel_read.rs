use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ChannelRead {
    pub channel_uri: String,
    pub cursor_at: DateTime<Utc>,
}
