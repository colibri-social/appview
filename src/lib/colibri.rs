use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColibriActorData {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,

    pub status: String, // required, default ""

    pub communities: Vec<String>, // required, items are record-keys
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriCommunity {
    /// The type of the record (e.g., the record ID constant).
    #[serde(rename = "$type")]
    pub r#type: String,

    /// The name of the community. 1-32 characters.
    /// Default: "New Community"
    pub name: String,

    /// A description of the community. Max 256 characters.
    /// Default: ""
    pub description: String,

    /// The order of the categories in this community.
    /// Items are record-keys.
    pub category_order: Vec<String>,

    /// Whether users can chat without an acknowledgement record.
    /// Default: true
    pub requires_approval_to_join: bool,

    /// Optional image for the community.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriCategory {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// The name of the category. 1-32 characters.
    /// Default: "New category"
    pub name: String,

    /// The order of the channels in this category.
    /// Items are record-keys.
    pub channel_order: Vec<String>,

    /// The community this category belongs to.
    /// Format: record-key
    pub community: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriChannel {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// The name of the channel. 1-32 characters.
    /// Default: "New channel"
    pub name: String,

    /// A description of the channel. Max 256 characters.
    /// Default: ""
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The type of the channel (e.g., social.colibri.channel.text).
    #[serde(rename = "type")]
    pub channel_type: String,

    /// The category this channel belongs to (record-key).
    pub category: String,

    /// The community this channel belongs to (record-key).
    pub community: String,

    /// Whether only the owner can post.
    /// Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_only: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriMessage {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// The message content. Max 2048 characters.
    pub text: String,

    /// Annotations of sections of the text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<Value>>,

    /// When the message was sent (ISO 8601 datetime).
    pub created_at: String,

    /// The channel this message was sent in (record-key).
    pub channel: String,

    /// Whether this message has been edited.
    /// Default: false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited: Option<bool>,

    /// The record key of a message this message is replying to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,

    /// An array of attachment objects for this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<Value>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriReaction {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// The emoji of the reaction. Supports custom strings.
    pub emoji: String,

    /// The message this relation belongs to.
    /// Format: record-key
    pub target_message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriMembership {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// AT-URI of the social.colibri.community record being joined
    pub community: String,

    /// Format: datetime
    pub created_at: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriApproval {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// AT-URI of the user's social.colibri.membership record
    pub membership: String,

    /// AT-URI of the social.colibri.community record being joined
    pub community: String,

    /// Format: datetime
    pub created_at: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriApprovalOrMembership {
    #[serde(rename = "$type")]
    pub r#type: String,

    /// AT-URI of the user's social.colibri.membership record
    pub membership: Option<String>,

    /// AT-URI of the social.colibri.community record being joined
    pub community: String,

    /// Format: datetime
    pub created_at: String,
}
