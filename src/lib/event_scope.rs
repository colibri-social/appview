//! Audience scoping for realtime server events.
//!
//! Every record from the upstream tap firehose is mapped **once** in the tap
//! loop (`run_connection`) into one or more [`ColibriServerEvent`]s, each tagged
//! with an [`EventScope`] describing who should receive it. The mapped event is
//! serialized once, wrapped in an [`std::sync::Arc`] ([`SharedScopedEvent`]) and
//! broadcast to every connected WS subscriber. Each subscriber then performs a
//! cheap in-memory check (is my user in this event's audience?) and forwards the
//! already-serialized payload — no per-client mapping or database work.
//!
//! This is the mechanism that stops a user in community A from receiving every
//! message/member/role/channel change for every other community on the network.
//!
//! [`ColibriServerEvent`]: crate::lib::events::ColibriServerEvent

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::Deserialize;

use crate::models::record_data;

const MESSAGE_NSID: &str = "social.colibri.message";
const CHANNEL_NSID: &str = "social.colibri.channel";

/// Audience scope for a mapped server event. Computed once in the tap loop;
/// each connection checks it against the connected user's community set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventScope {
    /// Deliver only to members of this community DID.
    Community(String),
    /// Deliver only to this DID's own connections (membership changes that
    /// affect a single user: their own join, leave, or kick).
    User(String),
    /// Deliver to everyone (low-frequency presence/profile updates).
    Global,
}

/// A mapped, pre-serialized server event plus its audience scope. Serialized
/// exactly once in the tap loop and shared via `Arc` across all subscribers, so
/// fan-out is a cheap refcount bump rather than a clone of the JSON string.
#[derive(Debug)]
pub struct ScopedEvent {
    pub scope: EventScope,
    pub payload: String,
}

pub type SharedScopedEvent = Arc<ScopedEvent>;

#[derive(Deserialize)]
struct StoredMessageChannel {
    channel: String,
}

/// Resolves the owning community DID for a message or channel, with an
/// in-memory cache. A channel's (and therefore a message's) community DID is
/// immutable for a given rkey, so cached entries never go stale.
///
/// Lives for the lifetime of the tap connection loop; one instance is shared
/// across all records flowing through `run_connection`.
#[derive(Default)]
pub struct CommunityResolver {
    /// channel rkey -> community DID.
    channel_to_community: Mutex<HashMap<String, String>>,
    /// message rkey -> community DID.
    message_to_community: Mutex<HashMap<String, String>>,
}

impl CommunityResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the community DID hosting the given channel rkey, or `None` if no
    /// channel record with that rkey is indexed yet. Cached after the first hit.
    pub async fn community_for_channel(
        &self,
        db: &DatabaseConnection,
        channel_rkey: &str,
    ) -> Option<String> {
        {
            let cache = self.channel_to_community.lock().unwrap();
            if let Some(community) = cache.get(channel_rkey) {
                return Some(community.clone());
            }
        }

        let record = record_data::Entity::find()
            .filter(record_data::Column::Nsid.eq(CHANNEL_NSID))
            .filter(record_data::Column::Rkey.eq(channel_rkey))
            .one(db)
            .await
            .ok()??;
        let community = record.did;

        self.channel_to_community
            .lock()
            .unwrap()
            .insert(channel_rkey.to_string(), community.clone());
        Some(community)
    }

    /// Returns the community DID owning the given message rkey by resolving
    /// message -> channel -> community, or `None` if the message/channel record
    /// is not indexed. Cached after the first hit.
    pub async fn community_for_message(
        &self,
        db: &DatabaseConnection,
        message_rkey: &str,
    ) -> Option<String> {
        {
            let cache = self.message_to_community.lock().unwrap();
            if let Some(community) = cache.get(message_rkey) {
                return Some(community.clone());
            }
        }

        let record = record_data::Entity::find()
            .filter(record_data::Column::Nsid.eq(MESSAGE_NSID))
            .filter(record_data::Column::Rkey.eq(message_rkey))
            .one(db)
            .await
            .ok()??;
        let channel_rkey = serde_json::from_value::<StoredMessageChannel>(record.data)
            .ok()?
            .channel;

        let community = self.community_for_channel(db, &channel_rkey).await?;
        self.message_to_community
            .lock()
            .unwrap()
            .insert(message_rkey.to_string(), community.clone());
        Some(community)
    }

    /// Test seam: pre-populate the channel cache so resolution never touches the
    /// database. `#[cfg(test)]`-only.
    #[cfg(test)]
    pub fn seed_channel(&self, channel_rkey: &str, community_did: &str) {
        self.channel_to_community
            .lock()
            .unwrap()
            .insert(channel_rkey.to_string(), community_did.to_string());
    }

    /// Test seam: pre-populate the message cache so resolution never touches the
    /// database. `#[cfg(test)]`-only.
    #[cfg(test)]
    pub fn seed_message(&self, message_rkey: &str, community_did: &str) {
        self.message_to_community
            .lock()
            .unwrap()
            .insert(message_rkey.to_string(), community_did.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn seeded_channel_resolves_without_db() {
        let resolver = CommunityResolver::new();
        resolver.seed_channel("chan-1", "did:plc:community");
        let db = mock_db();
        assert_eq!(
            resolver.community_for_channel(&db, "chan-1").await,
            Some(String::from("did:plc:community"))
        );
    }

    #[tokio::test]
    async fn seeded_message_resolves_without_db() {
        let resolver = CommunityResolver::new();
        resolver.seed_message("msg-1", "did:plc:community");
        let db = mock_db();
        assert_eq!(
            resolver.community_for_message(&db, "msg-1").await,
            Some(String::from("did:plc:community"))
        );
    }
}
