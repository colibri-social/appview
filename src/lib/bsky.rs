use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorProfile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pronouns: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub website: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<SelfLabels>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub joined_via_starter_pack: Option<StrongRef>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned_post: Option<StrongRef>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

impl ActorProfile {
    /// Whether this profile carries the `bot` self-label convention
    /// documented at docs.bsky.app for marking automated accounts.
    pub fn is_bot(&self) -> bool {
        self.labels
            .as_ref()
            .is_some_and(|l| l.values.iter().any(|v| v.val == "bot"))
    }
}

/// com.atproto.repo.strongRef
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrongRef {
    pub uri: String,
    pub cid: String,
}

/// com.atproto.label.defs#selfLabels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfLabels {
    #[serde(rename = "$type")]
    pub label_type: String, // "com.atproto.label.defs#selfLabels"
    pub values: Vec<SelfLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfLabel {
    pub val: String,
}
