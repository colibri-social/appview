use rocket::get;
use rocket::serde::json::Json;

use crate::lib::did_document::DidDocument;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::xrpc::com::atproto::identity::{resolve_did, resolve_handle};

#[get("/xrpc/com.atproto.identity.resolveIdentity?<identity>")]
/// Resolves an ATproto handle or DID to a DID Document
pub async fn resolve_identity(identity: &str) -> Result<Json<DidDocument>, ErrorResponse> {
    if identity.starts_with("did:") {
        return resolve_did(identity).await;
    } else {
        let did_res = resolve_handle(identity).await;

        if did_res.is_err() {
            return Err(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("UpstreamError"),
                    message: String::from("Unable to resolve handle"),
                }),
            });
        }

        let did = did_res.unwrap();

        return resolve_did(&did.did).await;
    }
}
