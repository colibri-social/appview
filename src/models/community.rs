use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Community {
    pub uri: String,
    pub owner_did: String,
    pub rkey: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CommunitiesResponse {
    pub owned: Vec<Community>,
    pub joined: Vec<Community>,
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
