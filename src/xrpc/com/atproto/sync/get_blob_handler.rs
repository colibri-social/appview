use std::io::Cursor;

use bytes::Bytes;
use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::{State, get, response};

use crate::lib::blob_cache::{BlobCache, CacheEntry};
use crate::lib::did_document::DidDocument;
use crate::lib::embed_fetch::{self, FetchError};
use crate::lib::range::{RangeResult, parse_range};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use rocket::serde::json::Json;

/// Blobs are content-addressed (immutable), so they can be cached aggressively.
const CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
/// Hard ceiling on a single blob fetch, independent of the cache's own byte
/// budget — bounds worst-case memory during the read itself.
const MAX_BLOB_BYTES: usize = 100 * 1024 * 1024;

/// Resolves a DID to its DID document by fetching from PLC directory or
/// the did:web well-known URL. Mirrors the logic in `resolve_did_handler`.
async fn fetch_did_document(did: &str) -> Result<DidDocument, FetchError> {
    let url = if did.starts_with("did:web:") {
        let host = did.trim_start_matches("did:web:");
        format!("https://{host}/.well-known/did.json")
    } else {
        format!("https://plc.directory/{did}")
    };
    embed_fetch::guarded_get(&url)
        .await?
        .json::<DidDocument>()
        .await
        .map_err(|e| FetchError::Upstream(e.to_string()))
}

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

/// Extracts the `serviceEndpoint` from the `#atproto_pds` service entry in a
/// DID document. Returns `None` if no such entry exists.
fn pds_endpoint(doc: &DidDocument) -> Option<&str> {
    doc.service
        .iter()
        .find(|s| s.id == "#atproto_pds" || s.service_type == "AtprotoPersonalDataServer")
        .map(|s| s.service_endpoint.as_str())
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

async fn get_blob_inner(did: &str, cid: &str, cache: &BlobCache) -> GetBlobResponse {
    // Cache hit: serve straight from memory (keyed by the content-addressed CID,
    // which also dedupes the same blob across DIDs).
    if let Some(entry) = cache.get(cid) {
        return GetBlobResponse::Blob {
            bytes: entry.bytes,
            content_type: entry.content_type,
        };
    }

    let doc = match fetch_did_document(did).await {
        Ok(d) => d,
        Err(e) => {
            return GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Failed to resolve DID: {e}"),
                }),
            });
        }
    };

    let endpoint = match pds_endpoint(&doc) {
        Some(e) => e.trim_end_matches('/').to_string(),
        None => {
            return GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("InvalidRequest"),
                    message: String::from("DID document has no AtprotoPersonalDataServer service."),
                }),
            });
        }
    };

    let url = format!("{endpoint}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}");
    let resp = match embed_fetch::guarded_get(&url).await {
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
pub async fn get_blob(did: &str, cid: &str, cache: &State<BlobCache>) -> GetBlobResponse {
    get_blob_inner(did, cid, cache.inner()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::did_document::Service;

    fn doc_with_pds(endpoint: &str) -> DidDocument {
        DidDocument {
            context: vec![],
            id: String::from("did:plc:test"),
            also_known_as: None,
            verification_method: vec![],
            service: vec![Service {
                id: String::from("#atproto_pds"),
                service_type: String::from("AtprotoPersonalDataServer"),
                service_endpoint: endpoint.to_string(),
            }],
        }
    }

    #[test]
    fn pds_endpoint_finds_atproto_pds_service() {
        let doc = doc_with_pds("https://pds.example.com");
        assert_eq!(pds_endpoint(&doc), Some("https://pds.example.com"));
    }

    #[test]
    fn pds_endpoint_returns_none_when_absent() {
        let doc = DidDocument {
            context: vec![],
            id: String::from("did:plc:test"),
            also_known_as: None,
            verification_method: vec![],
            service: vec![],
        };
        assert!(pds_endpoint(&doc).is_none());
    }

    #[test]
    fn pds_endpoint_matches_by_service_type_too() {
        let doc = DidDocument {
            context: vec![],
            id: String::from("did:plc:test"),
            also_known_as: None,
            verification_method: vec![],
            service: vec![Service {
                id: String::from("#other"),
                service_type: String::from("AtprotoPersonalDataServer"),
                service_endpoint: String::from("https://pds.example.com"),
            }],
        };
        assert_eq!(pds_endpoint(&doc), Some("https://pds.example.com"));
    }
}
