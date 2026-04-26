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
pub struct ColibriCommunityRecord {
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
