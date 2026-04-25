use serde::{Deserialize, Serialize};

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
