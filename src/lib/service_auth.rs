use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::lib::did_document::DidDocument;

#[derive(Debug, Deserialize, Serialize)]
struct ServiceAuthClaims {
    iss: String,
    aud: String,
    exp: u64,
    lxm: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceAuthError {
    #[error("invalid token format")]
    InvalidFormat,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("token expired")]
    Expired,
    #[error("invalid audience")]
    InvalidAudience,
    #[error("invalid lxm")]
    InvalidLxm,
    #[error("could not resolve DID: {0}")]
    DidResolution(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

const YOUR_APPVIEW_DID: &str = "did:web:api.colibri.social";

/// Verifies a service auth key issued by the user's PDS for the Colibri AppView. Returns the DID if successful.
pub async fn verify_service_auth(
    token: &str,
    expected_lxm: &str,
) -> Result<String, ServiceAuthError> {
    // Split JWT
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err(ServiceAuthError::InvalidFormat);
    }
    let (header_b64, claims_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    // Decode header for algorithm
    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|_| ServiceAuthError::InvalidFormat)?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|_| ServiceAuthError::InvalidFormat)?;
    let alg = header["alg"]
        .as_str()
        .ok_or(ServiceAuthError::InvalidFormat)?;

    // Decode unverified claims
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(claims_b64)
        .map_err(|_| ServiceAuthError::InvalidFormat)?;
    let claims: ServiceAuthClaims =
        serde_json::from_slice(&claims_bytes).map_err(|_| ServiceAuthError::InvalidFormat)?;

    // Validate audience
    if claims.aud != YOUR_APPVIEW_DID {
        return Err(ServiceAuthError::InvalidAudience);
    }

    // Validate expiry
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if claims.exp < now {
        return Err(ServiceAuthError::Expired);
    }

    // Validate lxm
    if let Some(lxm) = &claims.lxm {
        if lxm != expected_lxm {
            return Err(ServiceAuthError::InvalidLxm);
        }
    }

    // Resolve signing key
    let key_bytes = resolve_did_key_bytes(&claims.iss).await?;

    // Verify signature over "header.claims"
    let signing_input = format!("{}.{}", header_b64, claims_b64);
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| ServiceAuthError::InvalidFormat)?;

    let valid = match alg {
        "ES256K" => verify_es256k(&signing_input, &sig_bytes, &key_bytes),
        "ES256" => verify_es256(&signing_input, &sig_bytes, &key_bytes),
        _ => return Err(ServiceAuthError::InvalidFormat),
    };

    if !valid {
        return Err(ServiceAuthError::InvalidSignature);
    }

    Ok(claims.iss)
}

/// Verifies an ES256K-signed key.
fn verify_es256k(msg: &str, sig: &[u8], key: &[u8]) -> bool {
    use k256::ecdsa::signature::hazmat::PrehashVerifier;
    use k256::ecdsa::{Signature, VerifyingKey};

    let Ok(vk) = VerifyingKey::from_sec1_bytes(key) else {
        return false;
    };
    let Ok(sig) = Signature::from_slice(sig) else {
        return false;
    };
    let hash = Sha256::digest(msg.as_bytes());
    vk.verify_prehash(&hash, &sig).is_ok()
}

/// Verifies an ES256-signed key.
fn verify_es256(msg: &str, sig: &[u8], key: &[u8]) -> bool {
    use p256::ecdsa::signature::hazmat::PrehashVerifier;
    use p256::ecdsa::{Signature, VerifyingKey};

    let Ok(vk) = VerifyingKey::from_sec1_bytes(key) else {
        return false;
    };
    let Ok(sig) = Signature::from_slice(sig) else {
        return false;
    };
    let hash = Sha256::digest(msg.as_bytes());
    vk.verify_prehash(&hash, &sig).is_ok()
}

/// Resolves the DID document and extracts the associated verification method.
async fn resolve_did_key_bytes(did: &str) -> Result<Vec<u8>, ServiceAuthError> {
    let did_doc_url = if did.starts_with("did:web:") {
        let host = did.strip_prefix("did:web:").unwrap();
        format!("https://{}/.well-known/did.json", host)
    } else if did.starts_with("did:plc:") {
        format!("https://plc.directory/{}", did)
    } else {
        return Err(ServiceAuthError::DidResolution(format!(
            "unsupported DID method: {}",
            did
        )));
    };

    let doc: DidDocument = reqwest::get(&did_doc_url)
        .await?
        .json()
        .await
        .map_err(|e| ServiceAuthError::DidResolution(e.to_string()))?;

    let vm = doc
        .verification_method
        .into_iter()
        .find(|vm| vm.public_key_multibase.is_some() || vm.public_key_jwk.is_some())
        .ok_or_else(|| ServiceAuthError::DidResolution("no verification method found".into()))?;

    if let Some(multibase) = vm.public_key_multibase {
        let bytes = multibase_decode(&multibase).map_err(ServiceAuthError::DidResolution)?;
        // Strip 2-byte multicodec prefix to get raw SEC1 key bytes
        Ok(bytes[2..].to_vec())
    } else if let Some(jwk) = vm.public_key_jwk {
        jwk_to_sec1(&jwk).map_err(ServiceAuthError::DidResolution)
    } else {
        Err(ServiceAuthError::DidResolution(
            "no usable key found".into(),
        ))
    }
}

/// Decodes the multibase key.
fn multibase_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.starts_with('z') {
        return Err(format!("unsupported multibase prefix: {}", &s[..1]));
    }
    bs58::decode(&s[1..]).into_vec().map_err(|e| e.to_string())
}

/// Converts a JWK to a SEC1.
fn jwk_to_sec1(jwk: &serde_json::Value) -> Result<Vec<u8>, String> {
    let x = jwk["x"].as_str().ok_or("missing x")?;
    let y = jwk["y"].as_str().ok_or("missing y")?;
    let x = URL_SAFE_NO_PAD.decode(x).map_err(|e| e.to_string())?;
    let y = URL_SAFE_NO_PAD.decode(y).map_err(|e| e.to_string())?;
    // Uncompressed SEC1 point: 0x04 || x || y
    let mut sec1 = vec![0x04];
    sec1.extend_from_slice(&x);
    sec1.extend_from_slice(&y);
    Ok(sec1)
}
