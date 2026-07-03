use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use k256::ecdsa::{SigningKey, signature::hazmat::PrehashSigner};
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
    #[error("signing error: {0}")]
    Signing(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

const DEFAULT_APPVIEW_DID: &str = "did:web:api.colibri.social";

/// The `did:web` this AppView identifies as. Configurable via `APPVIEW_DID` so
/// self-hosted deployments verify service-auth `aud` (and sign outbound tokens)
/// against their own identity rather than the canonical one.
pub fn appview_did() -> String {
    std::env::var("APPVIEW_DID").unwrap_or_else(|_| DEFAULT_APPVIEW_DID.to_string())
}

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
    if !claims.aud.starts_with(&appview_did()) {
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
    if let Some(lxm) = &claims.lxm
        && lxm != expected_lxm
    {
        return Err(ServiceAuthError::InvalidLxm);
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

/// Verifies an inter-service auth JWT issued by a *peer AppView* (Humming).
/// Verification is identical to [`verify_service_auth`]; the returned DID is the
/// peer AppView's identity (`iss`). Callers MUST treat it only as the origin
/// AppView and perform a separate authorization check (e.g. matching it against
/// a user's declared `presenceService`) before acting on any claim it makes
/// about a user — a valid signature proves who sent the Hum, not who they may
/// speak for.
pub async fn verify_appview_auth(
    token: &str,
    expected_lxm: &str,
) -> Result<String, ServiceAuthError> {
    verify_service_auth(token, expected_lxm).await
}

/// Mints an inter-service auth JWT signed by this AppView's `K256_PRIVATE_KEY`
/// (ES256K), for calling a peer AppView. `iss` is this AppView's `did:web`,
/// `aud` the peer's DID, `lxm` the target method NSID. Short-lived (60s).
pub fn mint_appview_auth(aud: &str, lxm: &str) -> Result<String, ServiceAuthError> {
    let raw_key = std::env::var("K256_PRIVATE_KEY")
        .map_err(|_| ServiceAuthError::Signing("K256_PRIVATE_KEY not set".into()))?;
    let bytes = hex::decode(raw_key).map_err(|e| ServiceAuthError::Signing(e.to_string()))?;
    let signing_key =
        SigningKey::from_slice(&bytes).map_err(|e| ServiceAuthError::Signing(e.to_string()))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let header_b64 = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K","typ":"JWT"}"#);
    let claims = ServiceAuthClaims {
        iss: appview_did(),
        aud: aud.to_string(),
        exp: now + 60,
        lxm: Some(lxm.to_string()),
    };
    let claims_b64 = URL_SAFE_NO_PAD.encode(
        serde_json::to_string(&claims).map_err(|e| ServiceAuthError::Signing(e.to_string()))?,
    );

    let signing_input = format!("{header_b64}.{claims_b64}");
    let hash = Sha256::digest(signing_input.as_bytes());
    let signature: k256::ecdsa::Signature = signing_key
        .sign_prehash(&hash)
        .map_err(|e| ServiceAuthError::Signing(e.to_string()))?;
    // atproto expects low-S normalized signatures.
    let signature = signature.normalize_s().unwrap_or(signature);
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    Ok(format!("{signing_input}.{sig_b64}"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use rocket::tokio;
    use serde_json::json;

    fn make_token(claims: serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(claims.to_string());
        format!("{header}.{claims}.sig")
    }

    #[tokio::test]
    async fn rejects_invalid_token_format() {
        let err = verify_service_auth("invalid", "social.colibri.actor.getData")
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceAuthError::InvalidFormat));
    }

    #[tokio::test]
    async fn rejects_invalid_audience_before_network_lookup() {
        let token = make_token(json!({
            "iss":"did:plc:abc",
            "aud":"did:web:not-colibri",
            "exp": 4102444800u64
        }));

        let err = verify_service_auth(&token, "social.colibri.actor.getData")
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceAuthError::InvalidAudience));
    }

    #[tokio::test]
    async fn rejects_expired_token_before_network_lookup() {
        let token = make_token(json!({
            "iss":"did:plc:abc",
            "aud":"did:web:api.colibri.social",
            "exp": 1u64
        }));

        let err = verify_service_auth(&token, "social.colibri.actor.getData")
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceAuthError::Expired));
    }

    #[tokio::test]
    async fn rejects_invalid_lxm_before_network_lookup() {
        let token = make_token(json!({
            "iss":"did:plc:abc",
            "aud":"did:web:api.colibri.social",
            "exp": 4102444800u64,
            "lxm":"social.colibri.actor.setState"
        }));

        let err = verify_service_auth(&token, "social.colibri.actor.getData")
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceAuthError::InvalidLxm));
    }

    #[test]
    fn decodes_multibase_base58_payload() {
        let raw = vec![0xe7, 0x01, 0x01, 0x02, 0x03];
        let encoded = format!("z{}", bs58::encode(raw.clone()).into_string());
        assert_eq!(multibase_decode(&encoded).unwrap(), raw);
    }

    #[test]
    fn mint_signature_roundtrips_with_es256k_verifier() {
        // The signing path used by `mint_appview_auth` must produce a signature
        // the inbound verifier (`verify_es256k`) accepts against the SEC1 public
        // key a peer would recover from our published DID doc.
        let sk = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let signing_input = "eyJhbGciOiJFUzI1NksifQ.eyJpc3MiOiJkaWQ6d2ViOmEifQ";
        let hash = Sha256::digest(signing_input.as_bytes());
        let sig: k256::ecdsa::Signature = sk.sign_prehash(&hash).unwrap();
        let sig = sig.normalize_s().unwrap_or(sig);

        let pub_sec1 = sk.verifying_key().to_encoded_point(false);
        assert!(verify_es256k(
            signing_input,
            &sig.to_bytes(),
            pub_sec1.as_bytes()
        ));
    }

    #[test]
    fn converts_jwk_to_uncompressed_sec1() {
        let x = URL_SAFE_NO_PAD.encode([1u8; 32]);
        let y = URL_SAFE_NO_PAD.encode([2u8; 32]);
        let sec1 = jwk_to_sec1(&json!({ "x": x, "y": y })).unwrap();
        assert_eq!(sec1.len(), 65);
        assert_eq!(sec1[0], 0x04);
        assert_eq!(sec1[1], 1);
        assert_eq!(sec1[33], 2);
    }
}
