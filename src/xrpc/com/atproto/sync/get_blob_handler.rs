use std::io::Cursor;

use bytes::Bytes;
use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::{State, get, response};

use sea_orm::DatabaseConnection;

use crate::lib::blob_cache::{BlobCache, CacheEntry};
use crate::lib::embed_fetch;
use crate::lib::http::HTTP;
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

async fn get_blob_inner(
    did: &str,
    cid: &str,
    cache: &BlobCache,
    db: &DatabaseConnection,
) -> GetBlobResponse {
    // Cache hit: serve straight from memory (keyed by the content-addressed CID,
    // which also dedupes the same blob across DIDs).
    if let Some(entry) = cache.get(cid) {
        return GetBlobResponse::Blob {
            bytes: entry.bytes,
            content_type: entry.content_type,
        };
    }

    // For a community this AppView provisioned, the endpoint comes straight off
    // its credentials row
    let endpoint = match repo_endpoint::resolve(db, did).await {
        Ok(e) => e,
        Err(e) => {
            return GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Failed to resolve DID: {e}"),
                }),
            });
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
        RepoEndpoint::Untrusted(_) => embed_fetch::guarded_get(&url)
            .await
            .map_err(|e| e.to_string()),
    };
    let resp = match fetched {
        Ok(r) => r,
        Err(e) => {
            return GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Blob fetch failed: {e}"),
                }),
            });
        }
    };

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return GetBlobResponse::NotFound;
    }

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return GetBlobResponse::Upstream(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("UpstreamError"),
                message: format!("PDS returned {status}: {body}"),
            }),
        });
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
            return GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Failed to read blob bytes: {e}"),
                }),
            });
        }
    };

    cache.insert(
        cid,
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

#[get("/xrpc/com.atproto.sync.getBlob?<did>&<cid>")]
/// Proxies a blob fetch to the PDS that hosts the given DID, caching the bytes
/// in memory and serving HTTP Range requests itself (the PDS doesn't), so media
/// players can read duration up front and seek.
pub async fn get_blob(
    did: &str,
    cid: &str,
    cache: &State<BlobCache>,
    db: &State<DatabaseConnection>,
) -> GetBlobResponse {
    get_blob_inner(did, cid, cache.inner(), db.inner()).await
}
