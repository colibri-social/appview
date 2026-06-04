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
pub struct MemberEventMemberStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MemberEventMemberData {
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "onlineState")]
    pub online_state: String,
    pub status: MemberEventMemberStatus,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MemberEventMember {
    pub did: String,
    pub handle: String,
    pub roles: Vec<String>,
    #[serde(rename = "joinedAt")]
    pub joined_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
    pub data: MemberEventMemberData,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MemberEventData {
    pub event: String,
    pub community: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub membership: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberEventMember>,
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
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub channel_type: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageEventAuthorStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageEventAuthorData {
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "onlineState")]
    pub online_state: String,
    pub status: MessageEventAuthorStatus,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageEventAuthor {
    pub did: String,
    pub handle: String,
    pub data: MessageEventAuthorData,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<MessageEventAuthor>,
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
pub struct NotificationEventMessage {
    pub text: String,
    #[serde(default)]
    pub facets: Vec<Value>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NotificationEventData {
    pub id: i64,
    pub kind: String,
    #[serde(rename = "messageUri")]
    pub message_uri: String,
    #[serde(rename = "authorDid")]
    pub author_did: String,
    #[serde(rename = "channelRkey")]
    pub channel_rkey: String,
    #[serde(rename = "indexedAt")]
    pub indexed_at: String,
    pub message: NotificationEventMessage,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ColibriServerEventData {
    Community(CommunityEventData),
    Member(MemberEventData),
    Category(CategoryEventData),
    Channel(ChannelEventData),
    Message(MessageEventData),
    Reaction(ReactionEventData),
    User(UserEventData),
    Typing(TypingEventData),
    Notification(NotificationEventData),
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
pub struct VoiceChannelData {
    pub channel: String,
    pub community: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewData {
    pub channel: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TypingMessageData {
    pub channel: String,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColibriClientEventData {
    TypingMessage(TypingMessageData),
    View(ViewData),
    VoiceChannel(VoiceChannelData),
}

#[derive(Serialize, Deserialize)]
pub struct ColibriClientEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Option<ColibriClientEventData>,
}
