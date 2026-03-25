use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Community {
    #[serde(skip)]
    pub uri: String,
    pub owner_did: String,
    pub rkey: String,
    pub name: String,
    pub description: Option<String>,
    pub picture: Option<sqlx::types::Json<serde_json::Value>>,
    pub category_order: Option<sqlx::types::Json<serde_json::Value>>,
    pub requires_approval_to_join: bool,
}

#[derive(Debug, Serialize)]
pub struct CommunitiesResponse {
    pub owned: Vec<Community>,
    pub joined: Vec<Community>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Category {
    pub uri: String,
    pub rkey: String,
    pub community_uri: String,
    pub name: String,
    pub channel_order: Option<sqlx::types::Json<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Channel {
    pub uri: String,
    pub rkey: String,
    pub community_uri: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_type: String,
    pub category_rkey: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChannelWithVoice {
    pub uri: String,
    pub rkey: String,
    pub community_uri: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_type: String,
    pub category_rkey: Option<String>,
    #[serde(default)]
    pub voice_members: Vec<String>,
}

impl From<Channel> for ChannelWithVoice {
    fn from(channel: Channel) -> Self {
        ChannelWithVoice {
            uri: channel.uri,
            rkey: channel.rkey,
            community_uri: channel.community_uri,
            name: channel.name,
            description: channel.description,
            channel_type: channel.channel_type,
            category_rkey: channel.category_rkey,
            voice_members: Vec::new(),
        }
    }
}

/// A category with its channels nested inside — used for the sidebar endpoint.
#[derive(Debug, Serialize)]
pub struct SidebarCategory {
    pub uri: String,
    pub rkey: String,
    pub name: String,
    pub channel_order: Option<sqlx::types::Json<serde_json::Value>>,
    pub channels: Vec<Channel>,
}

/// Full sidebar payload: categorised channels + any channels with no category.
#[derive(Debug, Serialize)]
pub struct SidebarResponse {
    pub categories: Vec<SidebarCategory>,
    pub uncategorized: Vec<Channel>,
}

#[derive(Debug, Serialize)]
pub struct SidebarCategoryWithVoice {
    pub uri: String,
    pub rkey: String,
    pub name: String,
    pub channel_order: Option<sqlx::types::Json<serde_json::Value>>,
    pub channels: Vec<ChannelWithVoice>,
}

#[derive(Debug, Serialize)]
pub struct SidebarResponseWithVoice {
    pub categories: Vec<SidebarCategoryWithVoice>,
    pub uncategorized: Vec<ChannelWithVoice>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct CommunityMember {
    pub member_did: String,
    pub status: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub banner_url: Option<String>,
    pub description: Option<String>,
    pub handle: Option<String>,
    pub status_text: Option<String>,
    pub emoji: Option<String>,
    pub state: Option<String>,
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
