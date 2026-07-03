use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::bsky::ActorProfile;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColibriActorData {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    // Required by the lexicon, but defaulted here so older actor.data records
    // written before this field existed still deserialize. Community DIDs in
    // the user's preferred sidebar order.
    #[serde(default)]
    pub communities: Vec<String>,
}

/// Two-color gradient theme for a Colibri profile (`social.colibri.actor.profile`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColibriProfileGradient {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary: Option<String>,
}

/// Colibri-only profile theming. Always sourced from the Colibri profile record
/// regardless of `syncBluesky`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColibriProfileTheme {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accent_color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gradient: Option<ColibriProfileGradient>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner_color: Option<String>,
}

/// The Colibri-specific profile record (`social.colibri.actor.profile`, key
/// `self`). Kept separate from `app.bsky.actor.profile` so Colibri never needs
/// write access to the Bluesky record. All mirrored fields are optional: a
/// "from scratch" profile may be essentially empty, and a synced profile omits
/// them entirely (Bluesky stays the live source).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColibriActorProfile {
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,

    /// When true, the AppView serves the four mirrored fields from the user's
    /// `app.bsky.actor.profile` record instead of from this record.
    #[serde(default)]
    pub sync_bluesky: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme: Option<ColibriProfileTheme>,
}

/// The profile a viewer should see for a user, after applying the Colibri
/// profile / Bluesky / sync rules. Produced by [`resolve_effective_profile`].
#[derive(Debug, Clone, Default)]
pub struct EffectiveProfile {
    pub display_name: Option<String>,
    pub avatar: Option<Value>,
    pub banner: Option<Value>,
    pub description: Option<String>,
    pub theme: Option<ColibriProfileTheme>,
    pub sync_bluesky: bool,
    // Surfaced for callers/tests to distinguish onboarded users; unread in prod today
    #[allow(dead_code)]
    pub has_colibri_profile: bool,
}

/// Computes the effective profile for a user from their optional Colibri profile
/// record and optional Bluesky profile record:
///
/// - Colibri profile present, not synced → its own mirrored fields + theme.
/// - Colibri profile present, `syncBluesky` → Bluesky mirrored fields + Colibri theme.
/// - Colibri profile absent (un-onboarded) → Bluesky fields, no theme.
///
/// Note this does not derive `is_bot`/`handle`; those always come from the
/// Bluesky record / identity respectively and are filled by callers.
pub fn resolve_effective_profile(
    colibri_profile: Option<&ColibriActorProfile>,
    bsky_profile: Option<&ActorProfile>,
) -> EffectiveProfile {
    let from_bsky = |theme: Option<ColibriProfileTheme>, sync: bool, has: bool| EffectiveProfile {
        display_name: bsky_profile.and_then(|b| b.display_name.clone()),
        avatar: bsky_profile.and_then(|b| b.avatar.clone()),
        banner: bsky_profile.and_then(|b| b.banner.clone()),
        description: bsky_profile.and_then(|b| b.description.clone()),
        theme,
        sync_bluesky: sync,
        has_colibri_profile: has,
    };

    match colibri_profile {
        Some(p) if !p.sync_bluesky => EffectiveProfile {
            display_name: p.display_name.clone(),
            avatar: p.avatar.clone(),
            banner: p.banner.clone(),
            description: p.description.clone(),
            theme: p.theme.clone(),
            sync_bluesky: false,
            has_colibri_profile: true,
        },
        Some(p) => from_bsky(p.theme.clone(), true, true),
        None => from_bsky(None, false, false),
    }
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

    /// Set on a legacy community once it has been migrated. Points at the new
    /// community record that replaces it. Consumers treat this community as
    /// retired and hide it. Format: at-uri.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrated_to: Option<String>,

    /// Set on a community created by migrating a legacy one. Points at the
    /// legacy community record it replaces. Format: at-uri.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrated_from: Option<String>,

    /// DID of the AppView that administers this community and acts as its
    /// off-protocol hub (Humming relay + voice SFU host). Written by the
    /// credential-holding AppView. Read via `community_hub_did`, which applies
    /// the legacy fallback for records predating this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub appview: Option<String>,
}

/// The DID of the AppView that hubs a community's off-protocol traffic (Humming
/// presence relay and, in future, the voice SFU). Falls back to the canonical
/// `did:web:api.colibri.social` for records written before the `appview` field
/// existed — those communities live on colibri.social, so that is their real
/// hub. Never defaults to the reading AppView's own identity.
pub fn community_hub_did(community: &ColibriCommunity) -> String {
    community
        .appview
        .as_deref()
        .filter(|d| !d.is_empty())
        .unwrap_or("did:web:api.colibri.social")
        .to_string()
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

    /// Role record-keys allowed to post in this channel. Empty/absent means
    /// no role restriction (everyone may post, subject to `owner_only`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_roles: Vec<String>,

    /// Member DIDs explicitly allowed to post in this channel, in addition
    /// to `allowed_roles`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_members: Vec<String>,

    /// Set on a channel created by migrating a legacy community. Points at the
    /// legacy channel record it replaces, so its message history can be
    /// surfaced here at read time. Format: at-uri.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migrated_from: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bsky(display_name: &str) -> ActorProfile {
        serde_json::from_value(serde_json::json!({
            "displayName": display_name,
            "description": "from bsky",
            "avatar": { "ref": "bsky-blob" },
        }))
        .unwrap()
    }

    #[test]
    fn unsynced_colibri_profile_uses_its_own_fields() {
        let profile = ColibriActorProfile {
            display_name: Some(String::from("Colibri Alice")),
            description: Some(String::from("colibri bio")),
            sync_bluesky: false,
            theme: Some(ColibriProfileTheme {
                accent_color: Some(String::from("#ff0000")),
                ..Default::default()
            }),
            ..Default::default()
        };

        let effective = resolve_effective_profile(Some(&profile), Some(&bsky("Bsky Alice")));

        assert_eq!(effective.display_name.as_deref(), Some("Colibri Alice"));
        assert_eq!(effective.description.as_deref(), Some("colibri bio"));
        assert!(!effective.sync_bluesky);
        assert!(effective.has_colibri_profile);
        assert_eq!(
            effective.theme.unwrap().accent_color.as_deref(),
            Some("#ff0000")
        );
    }

    #[test]
    fn synced_colibri_profile_uses_bsky_fields_but_colibri_theme() {
        let profile = ColibriActorProfile {
            // Mirrored fields intentionally omitted while syncing.
            sync_bluesky: true,
            theme: Some(ColibriProfileTheme {
                banner_color: Some(String::from("#00ff00")),
                ..Default::default()
            }),
            ..Default::default()
        };

        let effective = resolve_effective_profile(Some(&profile), Some(&bsky("Bsky Alice")));

        assert_eq!(effective.display_name.as_deref(), Some("Bsky Alice"));
        assert!(effective.sync_bluesky);
        assert!(effective.has_colibri_profile);
        assert_eq!(
            effective.theme.unwrap().banner_color.as_deref(),
            Some("#00ff00")
        );
    }

    #[test]
    fn missing_colibri_profile_falls_back_to_bsky() {
        let effective = resolve_effective_profile(None, Some(&bsky("Bsky Alice")));

        assert_eq!(effective.display_name.as_deref(), Some("Bsky Alice"));
        assert!(!effective.sync_bluesky);
        assert!(!effective.has_colibri_profile);
        assert!(effective.theme.is_none());
    }
}
