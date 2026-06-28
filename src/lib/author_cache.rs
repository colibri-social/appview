//! Per-connection cache of author profile enrichment for the tap pipeline.
//!
//! Enriching a `social.colibri.message` event attaches the author's
//! `app.bsky.actor.profile` and `social.colibri.actor.data` records (see
//! `map_tap_event`). During a single repo's backfill **every** message has the
//! same author, so without a cache those two lookups (each a DB read, and on a
//! miss a live PDS fetch) are repeated for every message. This cache collapses
//! them to one lookup per author for the lifetime of the connection.
//!
//! Presence (`online_state`) is deliberately *not* cached — it changes
//! constantly and is a cheap indexed query, so it stays per-event.
//!
//! Staleness is handled by invalidation: when an `app.bsky.actor.profile`,
//! `social.colibri.actor.profile`, or `social.colibri.actor.data` record flows
//! through the firehose, the tap loop calls [`AuthorCache::invalidate`] for that
//! DID so the next enrichment re-reads. One instance is shared (via `Arc`)
//! across all shard workers.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{ColibriActorData, ColibriActorProfile};

/// The cached author records. `None` for a field means "looked up, not present"
/// — cached too, so authors without a profile/actor.data aren't re-fetched on
/// every message. `profile` is the raw `app.bsky.actor.profile`; `colibri_profile`
/// is the raw `social.colibri.actor.profile`. The two are resolved into an
/// effective profile at enrichment time (see `map_tap_event::cached_enrichment`)
/// so member/author surfaces honour `syncBluesky` just like `getData`.
#[derive(Clone, Default)]
pub struct AuthorEnrichment {
    pub profile: Option<ActorProfile>,
    pub colibri_profile: Option<ColibriActorProfile>,
    pub actor_data: Option<ColibriActorData>,
}

/// DID -> enrichment. Append-mostly with explicit invalidation; the `Mutex` is
/// held only for the brief get/put/remove.
#[derive(Default)]
pub struct AuthorCache {
    entries: Mutex<HashMap<String, AuthorEnrichment>>,
}

impl AuthorCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the cached enrichment for `did`, or `None` on a miss.
    pub fn get(&self, did: &str) -> Option<AuthorEnrichment> {
        self.entries.lock().unwrap().get(did).cloned()
    }

    /// Stores (or replaces) the enrichment for `did`.
    pub fn put(&self, did: &str, enrichment: AuthorEnrichment) {
        self.entries
            .lock()
            .unwrap()
            .insert(did.to_string(), enrichment);
    }

    /// Drops the cached entry for `did` so the next lookup re-fetches. Called
    /// when the author's profile/actor.data changes on the firehose.
    pub fn invalidate(&self, did: &str) {
        self.entries.lock().unwrap().remove(did);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile_named(name: &str) -> ActorProfile {
        serde_json::from_value(serde_json::json!({ "displayName": name })).unwrap()
    }

    #[test]
    fn get_returns_none_until_put() {
        let cache = AuthorCache::new();
        assert!(cache.get("did:plc:alice").is_none());

        cache.put(
            "did:plc:alice",
            AuthorEnrichment {
                profile: Some(profile_named("Alice")),
                colibri_profile: None,
                actor_data: None,
            },
        );

        let hit = cache.get("did:plc:alice").expect("expected a cache hit");
        assert_eq!(hit.profile.unwrap().display_name.as_deref(), Some("Alice"));
    }

    #[test]
    fn invalidate_evicts_only_the_target() {
        let cache = AuthorCache::new();
        cache.put("did:plc:alice", AuthorEnrichment::default());
        cache.put("did:plc:bob", AuthorEnrichment::default());

        cache.invalidate("did:plc:alice");

        assert!(cache.get("did:plc:alice").is_none());
        assert!(cache.get("did:plc:bob").is_some());
    }
}
