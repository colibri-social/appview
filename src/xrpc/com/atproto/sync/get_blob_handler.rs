use std::io::Cursor;

use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Response, Responder};
use rocket::{get, response};

use crate::lib::did_document::DidDocument;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use rocket::serde::json::Json;

/// Resolves a DID to its DID document by fetching from PLC directory or
/// the did:web well-known URL. Mirrors the logic in `resolve_did_handler`.
async fn fetch_did_document(did: &str) -> Result<DidDocument, reqwest::Error> {
    if did.starts_with("did:web:") {
        let host = did.trim_start_matches("did:web:");
        reqwest::get(format!("https://{host}/.well-known/did.json"))
            .await?
            .json::<DidDocument>()
            .await
    } else {
        reqwest::get(format!("https://plc.directory/{did}"))
            .await?
            .json::<DidDocument>()
            .await
    }
}

/// Extracts the `serviceEndpoint` from the `#atproto_pds` service entry in a
/// DID document. Returns `None` if no such entry exists.
fn pds_endpoint(doc: &DidDocument) -> Option<&str> {
    doc.service
        .iter()
        .find(|s| s.id == "#atproto_pds" || s.service_type == "AtprotoPersonalDataServer")
        .map(|s| s.service_endpoint.as_str())
}

pub struct BlobResponse {
    bytes: Vec<u8>,
    content_type: String,
}

impl<'r> Responder<'r, 'static> for BlobResponse {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let ct = self
            .content_type
            .parse::<ContentType>()
            .unwrap_or(ContentType::Binary);
        Response::build()
            .header(ct)
            .sized_body(self.bytes.len(), Cursor::new(self.bytes))
            .ok()
    }
}

pub enum GetBlobResponse {
    Blob(BlobResponse),
    NotFound,
    Upstream(ErrorResponse),
}

impl<'r> Responder<'r, 'static> for GetBlobResponse {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        match self {
            GetBlobResponse::Blob(b) => b.respond_to(req),
            GetBlobResponse::NotFound => Response::build()
                .status(Status::NotFound)
                .header(ContentType::JSON)
                .sized_body(
                    27,
                    Cursor::new(r#"{"error":"BlobNotFound"}"#),
                )
                .ok(),
            GetBlobResponse::Upstream(e) => e.respond_to(req),
        }
    }
}

async fn get_blob_inner(did: &str, cid: &str) -> GetBlobResponse {
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
    let resp = match reqwest::get(&url).await {
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

    let bytes = match resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            return GetBlobResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: format!("Failed to read blob bytes: {e}"),
                }),
            });
        }
    };

    GetBlobResponse::Blob(BlobResponse {
        bytes,
        content_type,
    })
}

#[get("/xrpc/com.atproto.sync.getBlob?<did>&<cid>")]
/// Proxies a blob fetch to the PDS that hosts the given DID. The blob is not
/// stored locally — every call resolves the PDS endpoint from the DID document
/// and streams the bytes back transparently.
pub async fn get_blob(did: &str, cid: &str) -> GetBlobResponse {
    get_blob_inner(did, cid).await
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
