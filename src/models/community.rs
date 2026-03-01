use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Community {
    #[serde(skip)]
    pub uri: String,
    #[serde(skip)]
    pub owner_did: String,
    pub rkey: String,
    pub name: String,
    pub description: Option<String>,
    pub image: Option<sqlx::types::Json<serde_json::Value>>,
    pub category_order: Option<sqlx::types::Json<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct CommunitiesResponse {
    pub owned: Vec<Community>,
    pub joined: Vec<Community>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Channel {
    pub uri: String,
    pub rkey: String,
    pub community_uri: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_type: String,
    pub category_rkey: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct CommunityMember {
    pub member_did: String,
    pub status: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct InviteCodeInfo {
    pub code: String,
    pub community_uri: String,
    pub created_by_did: String,
    pub max_uses: Option<i32>,
    pub use_count: i32,
    pub active: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateInviteRequest {
    pub community_uri: String,
    pub owner_did: String,
    pub max_uses: Option<i32>,
}
