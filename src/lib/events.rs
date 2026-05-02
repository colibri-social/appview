use serde::{Deserialize, Serialize};
use serde_json::Value;

// -- Server -> Client

#[derive(Serialize, Deserialize, Debug)]
pub struct CommunityEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<Value>,
    #[serde(rename = "categoryOrder", skip_serializing_if = "Option::is_none")]
    pub category_order: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MemberEventData {
    pub event: String,
    pub community: String,
    pub membership: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CategoryEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub community: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "channelOrder", skip_serializing_if = "Option::is_none")]
    pub channel_order: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChannelEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub community: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub channel_type: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<Value>>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<Value>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReactionEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserEventStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    pub text: String,
    pub state: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserEventProfile {
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub handle: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserEventData {
    pub did: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<UserEventStatus>,
    pub profile: UserEventProfile,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TypingEventData {
    pub event: String,
    pub channel: String,
    pub did: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ColibriServerEventData {
    CommunityEventData(CommunityEventData),
    MemberEventData(MemberEventData),
    CategoryEventData(CategoryEventData),
    ChannelEventData(ChannelEventData),
    MessageEventData(MessageEventData),
    ReactionEventData(ReactionEventData),
    UserEventData(UserEventData),
    TypingEventData(TypingEventData),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColibriServerEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<ColibriServerEventData>,
    #[serde(skip_serializing)]
    pub is_relevant: bool,
}

impl ColibriServerEvent {
    pub fn serialize(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

// -- Client -> Server

#[derive(Serialize, Deserialize, Debug)]
pub struct TypingMessageData {
    pub channel: String,
}

#[derive(Serialize, Deserialize)]
pub enum ColibriClientEventData {
    TypingMessageData(TypingMessageData),
}

#[derive(Serialize, Deserialize)]
pub struct ColibriClientEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Option<ColibriClientEventData>,
}
