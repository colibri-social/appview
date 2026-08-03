//! A bounded in-memory cache of intrinsic image dimensions, keyed by blob CID.

use std::num::NonZeroUsize;
use std::sync::Mutex;

use lru::LruCache;
use serde_json::Value;

/// Default entry cap when `BLOB_DIMENSION_CACHE_MAX_ENTRIES` is unset/invalid.
pub const DEFAULT_MAX_ENTRIES: usize = 50_000;

/// Intrinsic pixel size of an image blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dimensions {
    pub width: u32,
    pub height: u32,
}

/// Thread-safe LRU of dimension readings keyed by blob CID.
pub struct DimensionCache {
    entries: Mutex<LruCache<String, Option<Dimensions>>>,
}

impl DimensionCache {
    /// Creates a cache holding at most `max_entries` readings.
    pub fn new(max_entries: usize) -> Self {
        DimensionCache {
            entries: Mutex::new(LruCache::new(
                NonZeroUsize::new(max_entries.max(1)).expect("max_entries is at least 1"),
            )),
        }
    }

    /// Builds a cache sized from `BLOB_DIMENSION_CACHE_MAX_ENTRIES`, falling
    /// back to [`DEFAULT_MAX_ENTRIES`].
    pub fn from_env() -> Self {
        let cap = std::env::var("BLOB_DIMENSION_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|parsed| *parsed > 0)
            .unwrap_or(DEFAULT_MAX_ENTRIES);

        log::info!("Blob dimension cache capacity: {cap} entries");
        DimensionCache::new(cap)
    }

    /// `None` when nothing is known about `cid` yet, `Some(None)` when it is
    /// known not to be a decodable image, `Some(Some(_))` when it is.
    pub fn get(&self, cid: &str) -> Option<Option<Dimensions>> {
        let mut entries = self.entries.lock().ok()?;
        entries.get(cid).copied()
    }

    /// Records what `cid` turned out to be, including a negative result.
    pub fn remember(&self, cid: &str, dimensions: Option<Dimensions>) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.put(cid.to_string(), dimensions);
        }
    }
}

impl Default for DimensionCache {
    fn default() -> Self {
        DimensionCache::new(DEFAULT_MAX_ENTRIES)
    }
}

/// Pulls the CID and MIME type out of a lexicon blob value, accepting both the
/// current `{ ref: { $link } }` shape and the legacy `{ cid }` one.
pub fn blob_cid_and_mime(blob: &Value) -> Option<(String, String)> {
    let cid = blob
        .get("ref")
        .and_then(|reference| reference.get("$link"))
        .and_then(Value::as_str)
        .or_else(|| blob.get("cid").and_then(Value::as_str))?;

    let mime = blob
        .get("mimeType")
        .and_then(Value::as_str)
        .unwrap_or_default();

    Some((cid.to_string(), mime.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn distinguishes_unknown_from_known_non_image() {
        let cache = DimensionCache::new(8);
        assert_eq!(cache.get("bafyunknown"), None);

        cache.remember("bafynotimage", None);
        assert_eq!(cache.get("bafynotimage"), Some(None));
    }

    #[test]
    fn round_trips_a_reading() {
        let cache = DimensionCache::new(8);
        cache.remember(
            "bafyimage",
            Some(Dimensions {
                width: 800,
                height: 600,
            }),
        );

        assert_eq!(
            cache.get("bafyimage"),
            Some(Some(Dimensions {
                width: 800,
                height: 600
            }))
        );
    }

    #[test]
    fn evicts_the_least_recently_used_reading() {
        let cache = DimensionCache::new(2);
        cache.remember("a", None);
        cache.remember("b", None);
        cache.remember("c", None);

        assert_eq!(cache.get("a"), None);
        assert_eq!(cache.get("c"), Some(None));
    }

    #[test]
    fn reads_the_current_blob_shape() {
        let blob = json!({
            "$type": "blob",
            "ref": { "$link": "bafycurrent" },
            "mimeType": "image/png",
            "size": 12,
        });

        assert_eq!(
            blob_cid_and_mime(&blob),
            Some((String::from("bafycurrent"), String::from("image/png")))
        );
    }

    #[test]
    fn reads_the_legacy_blob_shape() {
        let blob = json!({ "cid": "bafylegacy", "mimeType": "image/jpeg" });

        assert_eq!(
            blob_cid_and_mime(&blob),
            Some((String::from("bafylegacy"), String::from("image/jpeg")))
        );
    }

    #[test]
    fn rejects_a_blob_without_a_cid() {
        assert_eq!(blob_cid_and_mime(&json!({ "mimeType": "image/png" })), None);
    }
}
