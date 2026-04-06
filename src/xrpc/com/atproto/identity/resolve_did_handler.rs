use rocket::{get, serde::json::Json};

use crate::lib::{
    did_document::DidDocument,
    responses::{ErrorBody, ErrorResponse},
};

#[get("/xrpc/com.atproto.identity.resolveDid?<did>")]
/// Resolves a DID (did:web or did:plc) to a DID document.
pub async fn resolve_did(did: &str) -> Result<Json<DidDocument>, ErrorResponse> {
    if !did.starts_with("did:") {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid DID given."),
            }),
        });
    }

    if did.starts_with("did:web:") {
        let host = did.replace("did:web:", "");

        let resp = reqwest::get(format!("https://{host}/.well-known/did.json"))
            .await?
            .json::<DidDocument>()
            .await?;

        Ok(Json(resp))
    } else {
        let resp = reqwest::get(format!("https://plc.directory/{did}"))
            .await?
            .json::<DidDocument>()
            .await?;

        Ok(Json(resp))
    }
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

        assert!(ok_result.id == String::from("did:web:api.bsky.app"))
    }

    #[tokio::test]
    async fn resolve_did_handles_plc_did() {
        let result = resolve_did("did:plc:w64dlsa4zwjv2wljlvmymldc").await;

        assert!(result.is_ok());

        let ok_result = result.unwrap();

        assert!(ok_result.id == String::from("did:plc:w64dlsa4zwjv2wljlvmymldc"))
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
