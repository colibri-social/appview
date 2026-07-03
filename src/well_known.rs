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

    // Publish the secp256k1 public key as proper multibase (z-base58btc of the
    // multicodec-prefixed compressed point), matching what `service_auth`'s
    // verifier decodes. A peer AppView resolves this doc to verify Hums we sign.
    let mut multicodec = vec![0xe7, 0x01];
    multicodec.extend_from_slice(compressed_point.as_bytes());
    let public_key_multibase = format!("z{}", bs58::encode(multicodec).into_string());

    let appview_did = crate::lib::service_auth::appview_did();
    let host = appview_did
        .strip_prefix("did:web:")
        .unwrap_or("api.colibri.social")
        .replace("%3A", ":");
    let endpoint = format!("https://{host}");

    Json(DidDocument {
        context: vec![
            String::from("https://www.w3.org/ns/did/v1"),
            String::from("https://w3id.org/security/multikey/v1"),
        ],
        id: appview_did.clone(),
        also_known_as: None,
        verification_method: vec![VerificationMethod {
            id: format!("{appview_did}#atproto"),
            verification_type: String::from("Multikey"),
            controller: appview_did.clone(),
            public_key_multibase: Some(public_key_multibase),
            public_key_jwk: None,
        }],
        service: vec![
            Service {
                id: String::from("#colibri_appview"),
                service_endpoint: endpoint.clone(),
                service_type: String::from("ColibriAppView"),
            },
            Service {
                id: String::from("#colibri_notif"),
                service_endpoint: endpoint.clone(),
                service_type: String::from("ColibriNotificationService"),
            },
        ],
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
        assert_eq!(did.service.len(), 2);
    }
}
