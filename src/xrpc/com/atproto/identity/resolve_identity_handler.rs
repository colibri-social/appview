use futures::future::BoxFuture;
use rocket::get;
use rocket::serde::json::Json;

use crate::lib::did_document::DidDocument;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::xrpc::com::atproto::identity::resolve_handle_handler::DidResponse;
use crate::xrpc::com::atproto::identity::{resolve_did, resolve_handle};

type ResolveDidFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Json<DidDocument>, ErrorResponse>> + Send + Sync;
type ResolveHandleFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Json<DidResponse>, ErrorResponse>> + Send + Sync;

async fn resolve_identity_with(
    identity: String,
    resolve_did_fn: &ResolveDidFn,
    resolve_handle_fn: &ResolveHandleFn,
) -> Result<Json<DidDocument>, ErrorResponse> {
    if identity.starts_with("did:") {
        return resolve_did_fn(identity).await;
    } else {
        let did_res = resolve_handle_fn(identity).await;

        if did_res.is_err() {
            return Err(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: String::from("Unable to resolve handle"),
                }),
            });
        }

        let did = did_res.unwrap();

        return resolve_did_fn(did.did.clone()).await;
    }
}

fn resolve_did_boxed(did: String) -> BoxFuture<'static, Result<Json<DidDocument>, ErrorResponse>> {
    Box::pin(async move { resolve_did(&did).await })
}

fn resolve_handle_boxed(
    handle: String,
) -> BoxFuture<'static, Result<Json<DidResponse>, ErrorResponse>> {
    Box::pin(async move { resolve_handle(&handle).await })
}

#[get("/xrpc/com.atproto.identity.resolveIdentity?<identity>")]
/// Resolves an ATproto handle or DID to a DID Document
pub async fn resolve_identity(identity: &str) -> Result<Json<DidDocument>, ErrorResponse> {
    resolve_identity_with(
        identity.to_string(),
        &resolve_did_boxed,
        &resolve_handle_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    fn sample_did_doc(did: &str) -> DidDocument {
        DidDocument {
            context: vec![String::from("https://www.w3.org/ns/did/v1")],
            id: did.to_string(),
            also_known_as: None,
            verification_method: vec![],
            service: vec![],
        }
    }

    #[tokio::test]
    async fn resolves_direct_did() {
        let result = resolve_identity_with(
            String::from("did:plc:abc"),
            &|_| Box::pin(async { Ok(Json(sample_did_doc("did:plc:abc"))) }),
            &|_| Box::pin(async { panic!("should not resolve handle") }),
        )
        .await
        .unwrap();

        assert_eq!(result.id, "did:plc:abc");
    }

    #[tokio::test]
    async fn resolves_handle_then_did() {
        let result = resolve_identity_with(
            String::from("alice.test"),
            &|did| Box::pin(async move { Ok(Json(sample_did_doc(&did))) }),
            &|_| {
                Box::pin(async {
                    Ok(Json(DidResponse {
                        did: String::from("did:plc:from-handle"),
                    }))
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.id, "did:plc:from-handle");
    }

    #[tokio::test]
    async fn returns_upstream_error_when_handle_resolution_fails() {
        let result = resolve_identity_with(
            String::from("alice.test"),
            &|_| Box::pin(async { panic!("should not resolve did") }),
            &|_| {
                Box::pin(async {
                    Err(ErrorResponse {
                        body: Json(ErrorBody {
                            error: String::from("UpstreamError"),
                            message: String::from("boom"),
                        }),
                    })
                })
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().message,
            "Unable to resolve handle"
        );
    }
}
