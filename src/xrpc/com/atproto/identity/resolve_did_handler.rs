use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use rocket::tokio::sync::Mutex as AsyncMutex;
use rocket::{get, serde::json::Json};

use crate::lib::http::HTTP;
use crate::lib::{
    did_document::DidDocument,
    responses::{ErrorBody, ErrorResponse},
};

/// How long a resolved DID document stays fresh in the cache. DID documents
/// (handle, PDS endpoint) change rarely, so a few minutes bounds staleness
/// while sparing the hot path repeated `plc.directory` round-trips — `resolve_did`
/// is called per firehose event (directly for the handle, and again inside every
/// record-fetch cache miss).
const DID_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

/// Process-wide cache of resolved DID documents. Successes only; misses/errors
/// are never cached so a transient failure self-heals on the next call.
static DID_CACHE: LazyLock<Mutex<HashMap<String, (DidDocument, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Returns a cached DID document if present and still within its TTL; evicts and
/// returns `None` on a stale entry.
fn did_cache_get(did: &str) -> Option<DidDocument> {
    let mut cache = DID_CACHE.lock().unwrap();
    match cache.get(did) {
        Some((doc, stored_at)) if stored_at.elapsed() < DID_CACHE_TTL => Some(doc.clone()),
        Some(_) => {
            cache.remove(did);
            None
        }
        None => None,
    }
}

fn did_cache_put(did: &str, doc: &DidDocument) {
    DID_CACHE
        .lock()
        .unwrap()
        .insert(did.to_string(), (doc.clone(), Instant::now()));
}

static DID_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn did_lock(did: &str) -> Arc<AsyncMutex<()>> {
    DID_LOCKS
        .lock()
        .unwrap()
        .entry(did.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

#[get("/xrpc/com.atproto.identity.resolveDid?<did>")]
/// Resolves a DID (did:web or did:plc) to a DID document, backed by a
/// short-TTL process-wide cache (see [`DID_CACHE_TTL`]).
pub async fn resolve_did(did: &str) -> Result<Json<DidDocument>, ErrorResponse> {
    if !did.starts_with("did:") {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid DID given."),
            }),
        });
    }

    if let Some(doc) = did_cache_get(did) {
        return Ok(Json(doc));
    }

    let lock = did_lock(did);
    let _guard = lock.lock().await;

    if let Some(doc) = did_cache_get(did) {
        return Ok(Json(doc));
    }

    let resp = if did.starts_with("did:web:") {
        let host = did.replace("did:web:", "");

        HTTP.get(format!("https://{host}/.well-known/did.json"))
            .send()
            .await?
            .json::<DidDocument>()
            .await?
    } else {
        HTTP.get(format!("https://plc.directory/{did}"))
            .send()
            .await?
            .json::<DidDocument>()
            .await?
    };

    did_cache_put(did, &resp);
    Ok(Json(resp))
}

#[cfg(test)]
mod tests {
    use rocket::tokio;

    use super::*;

    #[tokio::test]
    async fn resolve_did_handles_web_did() {
        let result = resolve_did("did:web:api.bsky.app").await;

        assert!(result.is_ok());

        let ok_result = result.unwrap();

        assert!(ok_result.id == *"did:web:api.bsky.app")
    }

    #[tokio::test]
    async fn resolve_did_handles_plc_did() {
        let result = resolve_did("did:plc:w64dlsa4zwjv2wljlvmymldc").await;

        assert!(result.is_ok());

        let ok_result = result.unwrap();

        assert!(ok_result.id == *"did:plc:w64dlsa4zwjv2wljlvmymldc")
    }

    #[tokio::test]
    async fn resolve_did_fails_for_invalid_did() {
        let result = resolve_did("123").await;

        assert!(result.is_err())
    }

    #[tokio::test]
    async fn resolve_did_fails_unknown_did() {
        let result = resolve_did("did:plc:invalid").await;

        assert!(result.is_err())
    }
}
