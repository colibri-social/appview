use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use rocket::get;
use rocket::serde::json::Json;

use futures::FutureExt;
use futures::future::{BoxFuture, select_ok};
use serde::{Deserialize, Serialize};

use crate::lib::did_document::DidDocument;
use crate::lib::http::HTTP;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use trust_dns_resolver::TokioAsyncResolver;
use trust_dns_resolver::config::*;

const HANDLE_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

static HANDLE_CACHE: LazyLock<Mutex<HashMap<String, (String, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn handle_cache_get(handle: &str) -> Option<String> {
    let mut cache = HANDLE_CACHE.lock().unwrap();
    match cache.get(handle) {
        Some((did, stored_at)) if stored_at.elapsed() < HANDLE_CACHE_TTL => Some(did.clone()),
        Some(_) => {
            cache.remove(handle);
            None
        }
        None => None,
    }
}

fn handle_cache_put(handle: &str, did: &str) {
    HANDLE_CACHE
        .lock()
        .unwrap()
        .insert(handle.to_string(), (did.to_string(), Instant::now()));
}

#[derive(Serialize, Deserialize)]
pub struct DidResponse {
    pub did: String,
}

#[get("/xrpc/com.atproto.identity.resolveHandle?<handle>")]
/// Resolves an atproto handle to a DID
pub async fn resolve_handle(handle: &str) -> Result<Json<DidResponse>, ErrorResponse> {
    let handle = handle.to_string();

    if let Some(did) = handle_cache_get(&handle) {
        return Ok(Json(DidResponse { did }));
    }

    let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());

    let did_json_future: BoxFuture<'_, Result<String, ErrorResponse>> = {
        let handle = handle.clone();
        async move {
            let resp = HTTP
                .get(format!("https://{handle}/.well-known/did.json"))
                .send()
                .await?;
            let doc = resp.json::<DidDocument>().await?;
            Ok(doc.id)
        }
        .boxed()
    };

    let atproto_did_future: BoxFuture<'_, Result<String, ErrorResponse>> = {
        let handle = handle.clone();
        async move {
            let resp = HTTP
                .get(format!("https://{handle}/.well-known/atproto-did"))
                .send()
                .await?;
            let text = resp.text().await?;
            if text.starts_with("did:") {
                Ok(text)
            } else {
                Err(ErrorResponse {
                    body: Json(ErrorBody {
                        error: String::from("UpstreamError"),
                        message: String::from("Invalid DID from atproto-did"),
                    }),
                })
            }
        }
        .boxed()
    };

    let dns_did_future: BoxFuture<'_, Result<String, ErrorResponse>> = {
        let handle = handle.clone();
        async move {
            let response = resolver.txt_lookup(format!("_atproto.{handle}")).await?;
            let record_data = response.iter().next().and_then(|r| r.txt_data().first());
            let record_bytes = record_data.ok_or_else(|| ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: String::from("Unable to read DNS record"),
                }),
            })?;
            let did_entry = String::from_utf8_lossy(record_bytes);
            let did = did_entry.replace("did=", "");
            if did.starts_with("did:") {
                Ok(did)
            } else {
                Err(ErrorResponse {
                    body: Json(ErrorBody {
                        error: String::from("UpstreamError"),
                        message: String::from("Invalid DID from DNS record"),
                    }),
                })
            }
        }
        .boxed()
    };

    let futures: Vec<BoxFuture<'_, Result<String, ErrorResponse>>> =
        vec![did_json_future, atproto_did_future, dns_did_future];

    let did = match select_ok(futures).await {
        Ok((did, _)) => did,
        Err(_) => {
            return Err(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: String::from("Unable to resolve handle"),
                }),
            });
        }
    };

    handle_cache_put(&handle, &did);
    Ok(Json(DidResponse { did }))
}

#[cfg(test)]
mod tests {
    use rocket::tokio;

    use super::*;

    #[tokio::test]
    #[ignore = "hits the live network (HTTP/DNS); run explicitly with --ignored"]
    async fn resolve_handle_handles_did_json() {
        let result = resolve_handle("api.bsky.app").await;

        assert!(result.is_ok());

        let ok_result = result.unwrap();

        assert!(ok_result.did == "did:web:api.bsky.app")
    }

    #[tokio::test]
    #[ignore = "hits the live network (HTTP/DNS); run explicitly with --ignored"]
    async fn resolve_handle_handles_atproto_did() {
        let result = resolve_handle("colibri.social").await;

        assert!(result.is_ok());

        let ok_result = result.unwrap();

        assert!(ok_result.did == "did:plc:mprdjqjluoswa7awzggaggj3")
    }

    #[tokio::test]
    #[ignore = "hits the live network (HTTP/DNS); run explicitly with --ignored"]
    async fn resolve_handle_handles_dns_record() {
        let result = resolve_handle("lou.gg").await;

        assert!(result.is_ok());

        let ok_result = result.unwrap();

        assert!(ok_result.did == "did:plc:w64dlsa4zwjv2wljlvmymldc")
    }
}
