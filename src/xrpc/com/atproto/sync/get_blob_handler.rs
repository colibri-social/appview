use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, LazyLock, Mutex};

use bytes::Bytes;
use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::tokio::sync::Mutex as AsyncMutex;
use rocket::{State, get, response};

use sea_orm::DatabaseConnection;

use crate::lib::blob_cache::{BlobCache, CacheEntry};
use crate::lib::embed_fetch;
use crate::lib::http::HTTP;
use crate::lib::image_variant::{self, Variant};
use crate::lib::range::{RangeResult, parse_range};
use crate::lib::repo_endpoint::{self, RepoEndpoint};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use rocket::serde::json::Json;

/// Blobs are content-addressed (immutable), so they can be cached aggressively.
const CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
/// Hard ceiling on a single blob fetch, independent of the cache's own byte
/// budget — bounds worst-case memory during the read itself.
const MAX_BLOB_BYTES: usize = 100 * 1024 * 1024;

async fn read_capped(mut resp: reqwest::Response, max_bytes: usize) -> Result<Bytes, String> {
    let mut bytes: Vec<u8> = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
        bytes.extend_from_slice(&chunk);
        if bytes.len() > max_bytes {
            bytes.truncate(max_bytes);
            break;
        }
    }
    Ok(Bytes::from(bytes))
}

pub enum GetBlobResponse {
    Blob { bytes: Bytes, content_type: String },
    NotFound,
    Upstream(ErrorResponse),
}

impl<'r> Responder<'r, 'static> for GetBlobResponse {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        match self {
            // Bytes are in memory, so the whole Range decision is synchronous:
            // parse the request header, then either serve the full blob (200)
            // or a zero-copy slice (206). 416 for an unsatisfiable range.
            GetBlobResponse::Blob {
                bytes,
                content_type,
            } => {
                let ct = content_type
                    .parse::<ContentType>()
                    .unwrap_or(ContentType::Binary);
                let total = bytes.len() as u64;

                match parse_range(req.headers().get_one("Range"), total) {
                    RangeResult::Full => Response::build()
                        .header(ct)
                        .raw_header("Accept-Ranges", "bytes")
                        .raw_header("Cache-Control", CACHE_CONTROL)
                        .sized_body(bytes.len(), Cursor::new(bytes))
                        .ok(),
                    RangeResult::Partial { start, end } => {
                        let part = bytes.slice(start as usize..=end as usize);
                        Response::build()
                            .status(Status::PartialContent)
                            .header(ct)
                            .raw_header("Accept-Ranges", "bytes")
                            .raw_header("Content-Range", format!("bytes {start}-{end}/{total}"))
                            .raw_header("Cache-Control", CACHE_CONTROL)
                            .sized_body(part.len(), Cursor::new(part))
                            .ok()
                    }
                    RangeResult::Unsatisfiable => Response::build()
                        .status(Status::RangeNotSatisfiable)
                        .raw_header("Content-Range", format!("bytes */{total}"))
                        .ok(),
                }
            }
            GetBlobResponse::NotFound => Response::build()
                .status(Status::NotFound)
                .header(ContentType::JSON)
                .sized_body(27, Cursor::new(r#"{"error":"BlobNotFound"}"#))
                .ok(),
            GetBlobResponse::Upstream(e) => e.respond_to(req),
        }
    }
}

/// The cache key for one rendition of a blob: the content-addressed CID (which
/// also dedupes the same blob across DIDs), plus the variant when the caller
/// asked for a resized one.
pub fn cache_key(cid: &str, variant: Option<Variant>) -> String {
    match variant {
        Some(variant) => format!("{cid}@{}", variant.as_str()),
        None => cid.to_string(),
    }
}

/// One in-flight fetch per CID. not per cache key, because a miss on any
/// variant is satisfied by the same upstream fetch. Without this, a member list
/// loading cold sends one request per avatar per rendition to the PDS at once.
static BLOB_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn blob_lock(cid: &str) -> Arc<AsyncMutex<()>> {
    BLOB_LOCKS
        .lock()
        .unwrap()
        .entry(cid.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

/// Drops `cid`'s lock once nobody is holding or waiting on it. There are far
/// more distinct CIDs than DIDs, so the registry has to be pruned rather than
/// grown forever. Losing a race here only costs a redundant fetch, never
/// correctness.
fn release_blob_lock(cid: &str) {
    let mut locks = BLOB_LOCKS.lock().unwrap();
    if locks
        .get(cid)
        .is_some_and(|lock| Arc::strong_count(lock) == 1)
    {
        locks.remove(cid);
    }
}

async fn get_blob_inner(
    did: &str,
    cid: &str,
    variant: Option<Variant>,
    cache: &BlobCache,
    db: &DatabaseConnection,
) -> GetBlobResponse {
    let key = cache_key(cid, variant);

    if let Some(entry) = cache.get(&key) {
        return GetBlobResponse::Blob {
            bytes: entry.bytes,
            content_type: entry.content_type,
        };
    }

    let lock = blob_lock(cid);
    let response = {
        let _guard = lock.lock().await;
        fetch_and_cache(did, cid, variant, &key, cache, db).await
    };
    drop(lock);
    release_blob_lock(cid);

    response
}

async fn fetch_and_cache(
    did: &str,
    cid: &str,
    variant: Option<Variant>,
    key: &str,
    cache: &BlobCache,
    db: &DatabaseConnection,
) -> GetBlobResponse {
    // Whoever held the lock before us may have already populated this key.
    if let Some(entry) = cache.get(key) {
        return GetBlobResponse::Blob {
            bytes: entry.bytes,
            content_type: entry.content_type,
        };
    }

    let (bytes, content_type) = match fetch_upstream(did, cid, db).await {
        Ok(fetched) => fetched,
        Err(response) => return response,
    };

    // A variant request that we can actually decode: derive every size from the
    // one decode and cache those instead of the full-resolution original, so a
    // 250 KB avatar never occupies the cache (or the wire) to render a 40 px row.
    if variant.is_some()
        && image_variant::is_resizable(&content_type)
        && let Some(mut rendered) = render_variants(cid, bytes.clone(), cache).await
        && let Some(entry) = rendered.remove(key)
    {
        return GetBlobResponse::Blob {
            bytes: entry.bytes,
            content_type: entry.content_type,
        };
    }

    cache.insert(
        &cache_key(cid, None),
        CacheEntry {
            bytes: bytes.clone(),
            content_type: content_type.clone(),
        },
    );

    GetBlobResponse::Blob {
        bytes,
        content_type,
    }
}

/// Decodes `bytes` once on a blocking worker, caches every variant, and returns
/// them. `None` when the bytes turn out not to be a decodable image, in which
/// case the caller falls back to serving the original.
async fn render_variants(
    cid: &str,
    bytes: Bytes,
    cache: &BlobCache,
) -> Option<std::collections::HashMap<String, CacheEntry>> {
    let rendered = rocket::tokio::task::spawn_blocking(move || image_variant::render_all(&bytes))
        .await
        .map_err(|e| e.to_string())
        .and_then(|result| result.map_err(|e| e.to_string()));

    let rendered = match rendered {
        Ok(rendered) => rendered,
        Err(e) => {
            log::warn!("Failed to render image variants for {cid}: {e}");
            return None;
        }
    };

    let mut entries = std::collections::HashMap::new();
    for (variant, out) in rendered {
        let entry = CacheEntry {
            bytes: out.bytes,
            content_type: out.content_type,
        };
        let key = cache_key(cid, Some(variant));
        cache.insert(&key, entry.clone());
        entries.insert(key, entry);
    }

    Some(entries)
}

async fn fetch_upstream(
    did: &str,
    cid: &str,
    db: &DatabaseConnection,
) -> Result<(Bytes, String), GetBlobResponse> {
    // For a community this AppView provisioned, the endpoint comes straight off
    // its credentials row
    let endpoint = match repo_endpoint::resolve(db, did).await {
        Ok(e) => e,
        Err(e) => {
            return Err(GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Failed to resolve DID: {e}"),
                }),
            }));
        }
    };

    let url = format!(
        "{endpoint}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}",
        endpoint = endpoint.as_str()
    );
    let fetched = match &endpoint {
        RepoEndpoint::Trusted(_) => HTTP
            .clone()
            .get(url)
            .send()
            .await
            .map_err(|e| e.to_string()),
        RepoEndpoint::Untrusted(_) => {
            embed_fetch::guarded_get_with_timeout(&url, embed_fetch::BLOB_FETCH_TIMEOUT)
                .await
                .map_err(|e| e.to_string())
        }
    };
    let resp = match fetched {
        Ok(r) => r,
        Err(e) => {
            return Err(GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Blob fetch failed: {e}"),
                }),
            }));
        }
    };

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(GetBlobResponse::NotFound);
    }

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(GetBlobResponse::Upstream(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("UpstreamError"),
                message: format!("PDS returned {status}: {body}"),
            }),
        }));
    }

    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    let bytes = match read_capped(resp, MAX_BLOB_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            return Err(GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Failed to read blob bytes: {e}"),
                }),
            }));
        }
    };

    Ok((bytes, content_type))
}

#[get("/xrpc/com.atproto.sync.getBlob?<did>&<cid>&<variant>")]
/// Proxies a blob fetch to the PDS that hosts the given DID, caching the bytes
/// in memory and serving HTTP Range requests itself (the PDS doesn't), so media
/// players can read duration up front and seek.
///
/// `variant` (`small` / `base` / `large`) asks for a square, downscaled
/// rendition instead of the original.
pub async fn get_blob(
    did: &str,
    cid: &str,
    variant: Option<&str>,
    cache: &State<BlobCache>,
    db: &State<DatabaseConnection>,
) -> GetBlobResponse {
    let variant = variant.and_then(Variant::parse);
    get_blob_inner(did, cid, variant, cache.inner(), db.inner()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    const CID: &str = "bafkreiexamplecid";

    #[test]
    fn variants_and_originals_get_distinct_cache_keys() {
        let original = cache_key(CID, None);
        let small = cache_key(CID, Some(Variant::Small));
        let large = cache_key(CID, Some(Variant::Large));

        assert_eq!(original, CID);
        assert_ne!(small, original);
        assert_ne!(small, large);
    }

    #[tokio::test]
    async fn a_held_lock_serializes_the_next_caller() {
        let cid = "bafkrei-serialized";
        let first = blob_lock(cid);
        let guard = first.lock().await;

        let second = blob_lock(cid);
        assert!(
            second.try_lock().is_err(),
            "a second caller must wait rather than fetch upstream in parallel"
        );

        drop(guard);
        assert!(second.try_lock().is_ok());

        drop(first);
        drop(second);
        release_blob_lock(cid);
    }

    #[test]
    fn releasing_prunes_the_registry() {
        let cid = "bafkrei-pruned";

        let lock = blob_lock(cid);
        assert!(BLOB_LOCKS.lock().unwrap().contains_key(cid));

        drop(lock);
        release_blob_lock(cid);

        assert!(
            !BLOB_LOCKS.lock().unwrap().contains_key(cid),
            "the registry must not grow one entry per CID ever served"
        );
    }

    #[test]
    fn releasing_keeps_a_lock_someone_else_still_holds() {
        let cid = "bafkrei-contended";

        let held = blob_lock(cid);
        let mine = blob_lock(cid);

        drop(mine);
        release_blob_lock(cid);

        assert!(BLOB_LOCKS.lock().unwrap().contains_key(cid));

        drop(held);
        release_blob_lock(cid);
        assert!(!BLOB_LOCKS.lock().unwrap().contains_key(cid));
    }
}
