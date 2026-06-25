//! A bounded in-memory LRU cache for blob bytes.
//!
//! Blobs are content-addressed by CID (immutable), so a fetched blob is valid
//! forever and can be shared across DIDs. The PDS upstream doesn't honour HTTP
//! Range, so to serve cheap byte-ranges (seeking, media duration) the AppView
//! holds the full bytes locally and slices them. Keeping recently-served blobs
//! in RAM means the PDS is hit at most once per CID while it stays hot, and
//! every range request after that is a zero-copy `Bytes::slice`.
//!
//! Eviction is by total byte size (least-recently-used first), not entry count.

use std::sync::Mutex;

use bytes::Bytes;
use lru::LruCache;

/// Default cache ceiling when `BLOB_CACHE_MAX_BYTES` is unset/invalid.
pub const DEFAULT_CAP_BYTES: u64 = 256 * 1024 * 1024;

/// One cached blob: its bytes plus the content-type the PDS reported. Cloning
/// is cheap — `Bytes` is reference-counted.
#[derive(Clone)]
pub struct CacheEntry {
    pub bytes: Bytes,
    pub content_type: String,
}

struct Inner {
    map: LruCache<String, CacheEntry>,
    /// Running sum of cached blob sizes, kept in step with `map`.
    total: u64,
}

/// Thread-safe, size-bounded LRU of blob bytes keyed by CID.
pub struct BlobCache {
    inner: Mutex<Inner>,
    cap: u64,
}

impl BlobCache {
    /// Creates a cache that holds at most `cap_bytes` of blob data.
    pub fn new(cap_bytes: u64) -> Self {
        BlobCache {
            inner: Mutex::new(Inner {
                // Unbounded by entry count; we evict by total bytes ourselves.
                map: LruCache::unbounded(),
                total: 0,
            }),
            cap: cap_bytes.max(1),
        }
    }

    /// Builds a cache sized from the `BLOB_CACHE_MAX_BYTES` env var, falling
    /// back to [`DEFAULT_CAP_BYTES`]. Logs the resolved capacity at boot.
    pub fn from_env() -> Self {
        let cap = std::env::var("BLOB_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_CAP_BYTES);
        log::info!("Blob cache capacity: {cap} bytes");
        Self::new(cap)
    }

    /// Returns the cached blob for `cid`, marking it most-recently-used.
    pub fn get(&self, cid: &str) -> Option<CacheEntry> {
        self.inner.lock().unwrap().map.get(cid).cloned()
    }

    /// Inserts (or replaces) `cid`'s blob, then evicts least-recently-used
    /// entries until the cache is back within its byte ceiling. A blob larger
    /// than the whole ceiling is not cached at all, so it can't evict
    /// everything else for a single use.
    pub fn insert(&self, cid: &str, entry: CacheEntry) {
        let size = entry.bytes.len() as u64;
        if size > self.cap {
            return;
        }

        let mut guard = self.inner.lock().unwrap();
        let inner = &mut *guard;

        if let Some(old) = inner.map.put(cid.to_string(), entry) {
            inner.total = inner.total.saturating_sub(old.bytes.len() as u64);
        }
        inner.total += size;

        while inner.total > self.cap {
            match inner.map.pop_lru() {
                Some((_, evicted)) => {
                    inner.total = inner.total.saturating_sub(evicted.bytes.len() as u64);
                }
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(size: usize) -> CacheEntry {
        CacheEntry {
            bytes: Bytes::from(vec![0u8; size]),
            content_type: "application/octet-stream".into(),
        }
    }

    #[test]
    fn round_trips_a_value() {
        let cache = BlobCache::new(1024);
        assert!(cache.get("a").is_none());
        cache.insert("a", entry(100));
        let got = cache.get("a").expect("present");
        assert_eq!(got.bytes.len(), 100);
    }

    #[test]
    fn evicts_least_recently_used_over_cap() {
        let cache = BlobCache::new(250);
        cache.insert("a", entry(100));
        cache.insert("b", entry(100));
        // Touch "a" so "b" becomes the LRU victim.
        assert!(cache.get("a").is_some());
        cache.insert("c", entry(100)); // total would be 300 > 250 → evict LRU ("b")
        assert!(cache.get("a").is_some());
        assert!(cache.get("c").is_some());
        assert!(cache.get("b").is_none());
    }

    #[test]
    fn skips_blob_larger_than_cap() {
        let cache = BlobCache::new(100);
        cache.insert("big", entry(500));
        assert!(cache.get("big").is_none());
    }

    #[test]
    fn replacing_key_updates_total() {
        let cache = BlobCache::new(250);
        cache.insert("a", entry(100));
        cache.insert("a", entry(200)); // replace, total = 200 (not 300)
        cache.insert("b", entry(50)); // total = 250, still within cap
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_some());
    }
}
