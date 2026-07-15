//! A bounded in-memory TTL cache for link-embed metadata. Keyed by the
//! (normalized) requested URL so a link that scrolls in and out of view (or
//! appears in several messages) only triggers one outbound fetch per TTL
//! window. Metadata only — proxied images are streamed uncached.
//!
//! Bounded by entry count (LRU eviction), mirroring `BlobCache`'s bound-by-size
//! approach — otherwise an authenticated caller could grow the cache without
//! limit by requesting distinct/garbage URLs.

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use lru::LruCache;
use reqwest::Url;

use crate::lib::embed_fetch::EmbedMetadata;

/// How long a cached entry stays fresh.
const TTL: Duration = Duration::from_secs(30 * 60);

/// Default entry cap when `EMBED_CACHE_MAX_ENTRIES` is unset/invalid.
pub const DEFAULT_MAX_ENTRIES: usize = 10_000;

/// Normalizes a URL for cache-key purposes so trivial variants (fragment,
/// scheme/host casing) share one entry. Falls back to the raw string for
/// anything that doesn't parse — the fetch path will reject it anyway.
fn normalize_key(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => url.to_string(),
    }
}

pub struct EmbedCache {
    entries: Mutex<LruCache<String, (Instant, EmbedMetadata)>>,
}

impl Default for EmbedCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES)
    }
}

impl EmbedCache {
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            entries: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Builds a cache sized from the `EMBED_CACHE_MAX_ENTRIES` env var,
    /// falling back to [`DEFAULT_MAX_ENTRIES`].
    pub fn from_env() -> Self {
        let cap = std::env::var("EMBED_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_ENTRIES);
        log::info!("Embed cache capacity: {cap} entries");
        Self::new(cap)
    }

    /// Returns the cached metadata for `url` if present and still within TTL.
    /// Expired entries are evicted on read.
    pub async fn get(&self, url: &str) -> Option<EmbedMetadata> {
        let key = normalize_key(url);
        let mut map = self.entries.lock().unwrap();
        match map.get(&key) {
            Some((stored_at, meta)) if stored_at.elapsed() < TTL => Some(meta.clone()),
            Some(_) => {
                map.pop(&key);
                None
            }
            None => None,
        }
    }

    /// Stores (or replaces) the metadata for `url`, evicting the
    /// least-recently-used entry if the cache is at capacity.
    pub async fn insert(&self, url: String, meta: EmbedMetadata) {
        let key = normalize_key(&url);
        self.entries
            .lock()
            .unwrap()
            .put(key, (Instant::now(), meta));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::embed_fetch::EmbedMetadata;
    use rocket::tokio;

    fn sample() -> EmbedMetadata {
        EmbedMetadata {
            title: Some("t".into()),
            description: None,
            site_name: None,
            theme_color: None,
            image: None,
            large_image: None,
        }
    }

    #[tokio::test]
    async fn round_trips_a_value() {
        let cache = EmbedCache::default();
        assert!(cache.get("https://x.test").await.is_none());
        cache.insert("https://x.test".into(), sample()).await;
        assert_eq!(cache.get("https://x.test").await, Some(sample()));
    }

    #[tokio::test]
    async fn normalizes_fragment_differences_to_one_entry() {
        let cache = EmbedCache::default();
        cache.insert("https://x.test/a#one".into(), sample()).await;
        assert_eq!(cache.get("https://x.test/a#two").await, Some(sample()));
    }

    #[tokio::test]
    async fn evicts_least_recently_used_over_cap() {
        let cache = EmbedCache::new(2);
        cache.insert("https://a.test".into(), sample()).await;
        cache.insert("https://b.test".into(), sample()).await;
        // Touch "a" so "b" becomes the LRU victim.
        assert!(cache.get("https://a.test").await.is_some());
        cache.insert("https://c.test".into(), sample()).await;
        assert!(cache.get("https://a.test").await.is_some());
        assert!(cache.get("https://c.test").await.is_some());
        assert!(cache.get("https://b.test").await.is_none());
    }
}
