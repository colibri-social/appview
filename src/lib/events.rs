use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::colibri::ColibriProfileTheme;

// -- Server -> Client

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    #[serde(
        rename = "requiresApprovalToJoin",
        skip_serializing_if = "Option::is_none"
    )]
    pub requires_approval_to_join: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberEventMemberStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberEventMemberData {
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "isBot")]
    pub is_bot: bool,
    #[serde(rename = "onlineState")]
    pub online_state: String,
    pub status: MemberEventMemberStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberEventData {
    pub event: String,
    pub community: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub membership: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberEventMember>,
    /// DID of the member who left — present only on `leave` events so
    /// remaining clients can remove the right entry from their member list.
    #[serde(rename = "memberDid", skip_serializing_if = "Option::is_none")]
    pub member_did: Option<String>,
}

/// Sent for changes to the moderator-facing pending-applications queue for a
/// `requiresApprovalToJoin` community. Broadcast to all clients; scope by
/// `community`.
///
/// | `event` | When | `did`/`handle`/`createdAt`/`data` |
/// |---|---|---|
/// | `create` | A new (or re-surfaced — e.g. a kicked member whose original `social.colibri.membership` is still on file) pending application | Always present |
/// | `resolve` | The application was admitted (`approveMembership`) | Absent |
/// | `dismiss` | A moderator hid the application from the active queue (AppView-only, off-protocol) | Absent |
/// | `undismiss` | A dismissed application was restored to the active queue | Absent |
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApplicationEventData {
    pub event: String,
    pub community: String,
    pub membership: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub did: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<MemberEventMemberData>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    #[serde(rename = "ownerOnly", skip_serializing_if = "Option::is_none")]
    pub owner_only: Option<bool>,
    #[serde(rename = "allowedRoles", skip_serializing_if = "Option::is_none")]
    pub allowed_roles: Option<Vec<String>>,
    #[serde(rename = "allowedMembers", skip_serializing_if = "Option::is_none")]
    pub allowed_members: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEventAuthorStatus {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEventAuthorData {
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "isBot")]
    pub is_bot: bool,
    #[serde(rename = "onlineState")]
    pub online_state: String,
    pub status: MessageEventAuthorStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageEventAuthor {
    pub did: String,
    pub handle: String,
    pub data: MessageEventAuthorData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RoleEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub community: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hoisted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mentionable: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReactionEventData {
    pub event: String,
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserEventStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    pub text: String,
    pub state: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserEventProfile {
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "isBot")]
    pub is_bot: bool,
    pub handle: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme: Option<ColibriProfileTheme>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserEventData {
    pub did: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<UserEventStatus>,
    pub profile: UserEventProfile,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TypingEventData {
    pub event: String,
    pub channel: String,
    pub did: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoicePresenceEventData {
    pub event: String,
    pub channel: String,
    pub did: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoiceStateEventData {
    pub channel: String,
    pub did: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deafened: Option<bool>,
    #[serde(
        rename = "serverMuted",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub server_muted: Option<bool>,
    #[serde(
        rename = "serverDeafened",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub server_deafened: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationEventData {
    pub id: i64,
    pub kind: String,
    #[serde(rename = "messageUri")]
    pub message_uri: String,
    #[serde(rename = "authorDid")]
    pub author_did: String,
    #[serde(rename = "channelUri")]
    pub channel_uri: String,
    #[serde(rename = "indexedAt")]
    pub indexed_at: String,
    pub message: NotificationEventMessage,
}

/// Payload of a `seen_event` — a per-user signal that the caller's own read
/// state changed on one device, pushed to their other connected clients so
/// unread badges update live. `event` is `"channel_read"` (the read cursor
/// advanced; clients clear the channel's white dot) or `"message_seen"` (a
/// message's ping was cleared; clients decrement the channel's ping count by
/// `cleared`).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SeenEventData {
    pub event: String,
    #[serde(rename = "channelUri")]
    pub channel_uri: String,
    #[serde(rename = "messageUri", skip_serializing_if = "Option::is_none")]
    pub message_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleared: Option<u64>,
}

/// Broadcast envelope for a `seen_event`. Carries the target DID so the
/// subscribe handler can deliver it only to that user's own connections,
/// mirroring how `IndexedNotification` carries `recipient_did`.
#[derive(Debug, Clone)]
pub struct SeenEvent {
    pub recipient_did: String,
    pub data: SeenEventData,
}

/// Payload of a `mute_event` — a per-user signal that the caller muted or
/// unmuted a channel or community on one device, pushed to their other
/// connected clients so the mute set stays in sync without a reload. `event`
/// is `"muted"` (a `social.colibri.actor.mute` record was created) or
/// `"unmuted"` (the record was deleted); `subject` is the muted channel or
/// community AT-URI.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MuteEventData {
    pub event: String,
    pub subject: String,
}

/// Broadcast envelope for a `mute_event`. Carries the target DID so the
/// subscribe handler can deliver it only to that user's own connections,
/// mirroring `SeenEvent`.
#[derive(Debug, Clone)]
pub struct MuteEvent {
    pub recipient_did: String,
    pub data: MuteEventData,
}

/// Client-facing payload of a `community_creation_progress` event — a coarse,
/// best-effort hint emitted while a (BYO) community is bootstrapped on the
/// user's PDS. `step` is a stable identifier (`connecting`, `creating`,
/// `registering`) the client maps to a label. The recipient DID lives on the
/// broadcast envelope, not here — this event is delivered only to the creating
/// user's own connections.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommunityCreationProgressData {
    pub step: String,
}

/// Broadcast envelope for a `community_creation_progress` event. Carries the
/// target DID so the subscribe handler delivers it only to that user's own
/// connections, mirroring `SeenEvent`/`MuteEvent`. Emitted by
/// `community.create` while bootstrapping a BYO community.
#[derive(Debug, Clone)]
pub struct CommunityCreationProgressEvent {
    pub recipient_did: String,
    pub data: CommunityCreationProgressData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ColibriServerEventData {
    Community(CommunityEventData),
    Member(MemberEventData),
    Application(ApplicationEventData),
    Category(CategoryEventData),
    Channel(ChannelEventData),
    Role(RoleEventData),
    Message(MessageEventData),
    Reaction(ReactionEventData),
    User(UserEventData),
    Typing(TypingEventData),
    VoicePresence(VoicePresenceEventData),
    VoiceState(VoiceStateEventData),
    Notification(NotificationEventData),
    Seen(SeenEventData),
    Mute(MuteEventData),
    CommunityCreationProgress(CommunityCreationProgressData),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColibriServerEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<ColibriServerEventData>,
}

impl ColibriServerEvent {
    pub fn serialize(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

// -- AppView <-> AppView (Humming)

/// The only event payloads a Hum may carry — the off-protocol subset. Tagged on
/// the same `type`/`data` shape as a server event, so an on-protocol event
/// (message/member/role/…) fails to deserialize into this enum and is rejected
/// before it can be relayed. This is the structural guard that keeps a forged
/// Hum from ever manufacturing on-protocol state.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum HumEvent {
    #[serde(rename = "user_event")]
    User(Box<UserEventData>),
    #[serde(rename = "typing_event")]
    Typing(TypingEventData),
    #[serde(rename = "voice_presence_event")]
    VoicePresence(VoicePresenceEventData),
    #[serde(rename = "voice_state_event")]
    VoiceState(VoiceStateEventData),
}

/// An off-protocol event relayed between AppViews. Authenticated via
/// inter-service auth (the JWT issuer must equal `origin`, and `origin` must
/// equal `subject`'s declared `presenceService`). See the `sendHum` /
/// `subscribeHums` handlers.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HumEnvelope {
    pub origin: String,
    pub id: String,
    pub ttl: u8,
    pub subject: String,
    pub community: String,
    pub event: HumEvent,
}

// -- Client -> Server

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoiceChannelData {
    pub channel: String,
    pub community: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoiceStateData {
    pub channel: String,
    pub community: String,
    pub muted: bool,
    pub deafened: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ViewData {
    pub channel: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TypingMessageData {
    pub channel: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ColibriClientEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    /// Raw payload, shape depends on `event_type`. Deliberately not a typed
    /// enum: `TypingMessageData` and `ViewData` are structurally identical
    /// (`{ channel: String }`), so an untagged enum can't disambiguate them
    /// by shape — it would always pick the first matching variant regardless
    /// of `event_type`. Callers deserialize into the concrete struct their
    /// `event_type` arm expects.
    pub data: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hum_event_accepts_offprotocol_types() {
        let json = r#"{"type":"typing_event","data":{"event":"start","channel":"at://c/social.colibri.channel/x","did":"did:plc:me"}}"#;
        let ev: HumEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(ev, HumEvent::Typing(_)));
    }

    #[test]
    fn hum_event_rejects_onprotocol_types() {
        // A message_event must never deserialize into a HumEvent — the structural
        // guard that stops a forged Hum from injecting on-protocol state.
        let json = r#"{"type":"message_event","data":{"event":"upsert","uri":"at://x","channel":"at://c"}}"#;
        assert!(serde_json::from_str::<HumEvent>(json).is_err());
    }

    #[test]
    fn hum_envelope_roundtrips() {
        let json = r#"{"origin":"did:web:a","id":"abc","ttl":1,"subject":"did:plc:me","community":"at://did:plc:c/social.colibri.community/self","event":{"type":"user_event","data":{"did":"did:plc:me","profile":{"isBot":false,"handle":"me.test"}}}}"#;
        let envelope: HumEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.origin, "did:web:a");
        assert_eq!(envelope.ttl, 1);
        assert!(matches!(envelope.event, HumEvent::User(_)));
    }
}
