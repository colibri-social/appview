use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColibriActorData {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

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

    /// The message this reaction belongs to.
    /// Format: record-key
    #[serde(rename = "targetMessage", alias = "parent")]
    pub parent: String,
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
pub struct ColibriRoleChannelOverride {
    /// The channel rkey this override applies to.
    pub channel: String,
    /// Permissions explicitly granted in this channel.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Permissions explicitly denied in this channel.
    #[serde(default)]
    pub deny: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriRole {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    /// Display name of the role. 1-32 characters.
    pub name: String,

    /// Optional hex color (`#rrggbb`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,

    /// Granted permissions. See `permissions::Permission` for the enumerated set.
    #[serde(default)]
    pub permissions: Vec<String>,

    /// Hierarchy position. Higher values sit higher in the hierarchy.
    pub position: i64,

    /// Whether members of this role are displayed separately in the member list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hoisted: Option<bool>,

    /// Whether `@role` style mentions resolve to this role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mentionable: Option<bool>,

    /// Whether this role is protected from modification or deletion. System
    /// roles like the bootstrap "Owner" set this to true so role-management
    /// endpoints can refuse mutating operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protected: Option<bool>,

    /// Per-channel permission overrides for this role.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channel_overrides: Vec<ColibriRoleChannelOverride>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriMember {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    /// The member's DID.
    pub subject: String,

    /// Role rkeys assigned to this member, stored on the same community repo.
    #[serde(default)]
    pub roles: Vec<String>,

    /// Format: datetime
    pub joined_at: String,

    /// Optional per-community display-name override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,

    /// Optional AT-URI of the user-side `social.colibri.membership` declaration
    /// that this member record was admitted from. Audit trail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_membership: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriModerationSubject {
    /// DID of the subject for user-targeted actions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub did: Option<String>,
    /// AT-URI of the subject for content-targeted actions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColibriModeration {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    /// `ban` | `unban` | `hideMessage` | `unhideMessage` | `kick`
    pub action: String,

    pub subject: ColibriModerationSubject,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// DID of the issuer.
    pub created_by: String,

    /// Format: datetime
    pub created_at: String,
}
