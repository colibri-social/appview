//! A tiny in-memory TTL cache for link-embed metadata. Keyed by the requested
//! URL so a link that scrolls in and out of view (or appears in several
//! messages) only triggers one outbound fetch per TTL window. Metadata only —
//! proxied images are streamed uncached.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rocket::tokio::sync::RwLock;

use crate::lib::embed_fetch::EmbedMetadata;

/// How long a cached entry stays fresh.
const TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Default)]
pub struct EmbedCache {
    entries: RwLock<HashMap<String, (Instant, EmbedMetadata)>>,
}

impl EmbedCache {
    /// Returns the cached metadata for `url` if present and still within TTL.
    /// Expired entries are evicted lazily on read.
    pub async fn get(&self, url: &str) -> Option<EmbedMetadata> {
        {
            let map = self.entries.read().await;
            if let Some((stored_at, meta)) = map.get(url) {
                if stored_at.elapsed() < TTL {
                    return Some(meta.clone());
                }
            } else {
                return None;
            }
        }
        // Fell through: entry exists but is stale — drop it.
        self.entries.write().await.remove(url);
        None
    }

    /// Stores (or replaces) the metadata for `url`.
    pub async fn insert(&self, url: String, meta: EmbedMetadata) {
        self.entries
            .write()
            .await
            .insert(url, (Instant::now(), meta));
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
}
