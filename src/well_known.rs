use rocket::{get, serde::json::Json};

use crate::lib::did_document::{DidDocument, Service, VerificationMethod};
use k256::ecdsa::{SigningKey, VerifyingKey};
use std::env;

#[get("/.well-known/did.json")]
pub fn did_json() -> Json<DidDocument> {
    let raw_key = env::var("K256_PRIVATE_KEY").expect("K256_PRIVATE_KEY not found in .env");

    let bytes = hex::decode(raw_key).unwrap();
    let signing_key = SigningKey::from_slice(&bytes).unwrap();
    let verifying_key = VerifyingKey::from(&signing_key);
    let compressed_point = verifying_key.to_encoded_point(true);

    Json(DidDocument {
        context: vec![
            String::from("https://www.w3.org/ns/did/v1"),
            String::from("https://w3id.org/security/multikey/v1"),
        ],
        id: String::from("did:web:api.colibri.social"),
        also_known_as: None,
        verification_method: vec![VerificationMethod {
            id: String::from("did:web:api.colibri.social#atproto"),
            verification_type: String::from("Multikey"),
            controller: String::from("did:web:api.colibri.social"),
            public_key_multibase: Some(hex::encode(compressed_point)),
            public_key_jwk: None,
        }],
        service: vec![Service {
            id: String::from("#colibri_appview"),
            service_endpoint: String::from("https://api.colibri.social"),
            service_type: String::from("ColibriAppView"),
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_colibri_did_document() {
        unsafe {
            env::set_var(
                "K256_PRIVATE_KEY",
                "0000000000000000000000000000000000000000000000000000000000000001",
            );
        }

        let did = did_json();
        assert_eq!(did.id, "did:web:api.colibri.social");
        assert_eq!(did.verification_method.len(), 1);
        assert!(did.verification_method[0].public_key_multibase.is_some());
        assert_eq!(did.service.len(), 1);
    }
}
