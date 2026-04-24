use serde::{Deserialize, Serialize};
use serde_json::Value;

// -- Server -> Client

#[derive(Serialize, Deserialize)]
struct CommunityEventData {
    event: String,
    uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    picture: Option<Value>,
    #[serde(rename = "categoryOrder", skip_serializing_if = "Option::is_none")]
    category_order: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct MemberEventData {
    event: String,
    community: String,
    membership: String,
}

#[derive(Serialize, Deserialize)]
struct CategoryEventData {
    event: String,
    uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    community: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(rename = "channelOrder", skip_serializing_if = "Option::is_none")]
    channel_order: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct ChannelEventData {
    event: String,
    uri: String,
    community: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    channel_type: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct MessageEventData {
    event: String,
    uri: String,
    channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facets: Option<Vec<Value>>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(rename = "indexedAt", skip_serializing_if = "Option::is_none")]
    indexed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    edited: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachments: Option<Vec<Value>>,
}

#[derive(Serialize, Deserialize)]
struct ReactionEventData {
    event: String,
    uri: String,
    emoji: String,
    target: String,
    channel: String,
}

#[derive(Serialize, Deserialize)]
struct UserEventStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    emoji: Option<String>,
    text: String,
    state: String,
}

#[derive(Serialize, Deserialize)]
struct UserEventProfile {
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    handle: String,
}

#[derive(Serialize, Deserialize)]
struct UserEventData {
    did: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<UserEventStatus>,
    profile: UserEventProfile,
}

#[derive(Serialize, Deserialize)]
struct TypingEventData {
    event: String,
    channel: String,
    did: String,
}

#[derive(Serialize, Deserialize)]
pub enum ColibriServerEventData {
    CommunityEventData,
    MemberEventData,
    CategoryEventData,
    ChannelEventData,
    MessageEventData,
    ReactionEventData,
    UserEventData,
    TypingEventData,
}

#[derive(Serialize, Deserialize)]
pub struct ColibriServerEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<ColibriServerEventData>,
}

// -- Client -> Server

#[derive(Serialize, Deserialize)]
pub enum ColibriClientEventData {}

#[derive(Serialize, Deserialize)]
pub struct ColibriClientEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Option<ColibriClientEventData>,
}
