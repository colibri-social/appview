use rocket::get;
use rocket::serde::json::Json;

use crate::lib::did_document::DidDocument;
use crate::lib::responses::{ErrorBody, ErrorResponse, ResponseEnum};

#[get("/xrpc/com.atproto.identity.resolveDid?<did>")]
pub async fn resolve_did(did: &str) -> Result<ResponseEnum<DidDocument>, ErrorResponse> {
    if !did.starts_with("did:") {
        return Ok(ResponseEnum::Error(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid DID given."),
            }),
        }));
    }

    if did.starts_with("did:web:") {
        let host = did.replace("did:web:", "");
        let resp = reqwest::get(format!("https://{host}/.well-known/did.json"))
            .await?
            .json::<DidDocument>()
            .await?;

        return Ok(ResponseEnum::Success(Json(resp)));
    } else {
        let resp = reqwest::get(format!("https://plc.directory/{did}"))
            .await?
            .json::<DidDocument>()
            .await?;

        return Ok(ResponseEnum::Success(Json(resp)));
    }
}

#[get("/xrpc/com.atproto.identity.resolveHandle?<handle>")]
pub fn resolve_handle(handle: &str) -> &'static str {
    "Hello, world!"
}

#[get("/xrpc/com.atproto.identity.resolveIdentity?<identity>")]
pub fn resolve_identity(identity: &str) -> &'static str {
    "Hello, world!"
}
