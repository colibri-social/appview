use jsonwebtoken::{DecodingKey, Validation, decode, decode_header};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::lib::did_document::DidDocument;

#[derive(Debug, Deserialize, Serialize)]
struct ServiceAuthClaims {
    iss: String,
    aud: String,
    exp: u64,
    lxm: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VerificationMethod {
    #[serde(rename = "publicKeyMultibase")]
    public_key_multibase: Option<String>,
    #[serde(rename = "publicKeyJwk")]
    public_key_jwk: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceAuthError {
    #[error("missing or invalid token")]
    Unauthorized,
    #[error("token expired")]
    Expired,
    #[error("invalid audience")]
    InvalidAudience,
    #[error("invalid lxm")]
    InvalidLxm,
    #[error("could not resolve DID: {0}")]
    DidResolution(String),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
}

const YOUR_APPVIEW_DID: &str = "did:web:api.colibri.social";
const EXPECTED_LXM: &str = "social.colibri.sync.subscribeEvents";

pub async fn verify_service_auth(token: &str) -> Result<String, ServiceAuthError> {
    // Determine the algorithm
    let header = decode_header(token)?;
    let alg = header.alg;

    // Decode without verification first to get the `iss` (user DID)
    let mut no_verify = Validation::new(alg);
    no_verify.insecure_disable_signature_validation();
    no_verify.set_audience(&[YOUR_APPVIEW_DID]);
    no_verify.validate_exp = false;

    let unverified =
        decode::<ServiceAuthClaims>(token, &DecodingKey::from_secret(&[]), &no_verify)?;
    let iss = &unverified.claims.iss;

    // Resolve the DID document and extract the signing key
    let decoding_key = resolve_did_key(iss).await?;

    // Full verification
    let mut validation = Validation::new(alg);
    validation.set_audience(&[YOUR_APPVIEW_DID]);
    validation.validate_exp = true;

    let verified = decode::<ServiceAuthClaims>(token, &decoding_key, &validation)?;
    let claims = verified.claims;

    // Validate lxm
    if let Some(lxm) = &claims.lxm {
        if lxm != EXPECTED_LXM {
            return Err(ServiceAuthError::InvalidLxm);
        }
    }

    Ok(claims.iss)
}

async fn resolve_did_key(did: &str) -> Result<DecodingKey, ServiceAuthError> {
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
        // multibase: 'z' prefix = base58btc encoded
        let bytes = multibase_decode(&multibase).map_err(|e| ServiceAuthError::DidResolution(e))?;
        // First 2 bytes are a multicodec prefix (0xe7 0x01 = secp256k1, 0x80 0x24 = p-256)
        let key_bytes = &bytes[2..];
        Ok(DecodingKey::from_ec_der(key_bytes))
    } else if let Some(jwk) = vm.public_key_jwk {
        let jwk_str = serde_json::to_string(&jwk).unwrap();
        Ok(DecodingKey::from_ec_pem(jwk_str.as_bytes())
            .map_err(|e| ServiceAuthError::DidResolution(e.to_string()))?)
    } else {
        Err(ServiceAuthError::DidResolution(
            "no usable key found".into(),
        ))
    }
}

fn multibase_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.starts_with('z') {
        return Err(format!("unsupported multibase prefix: {}", &s[..1]));
    }
    bs58::decode(&s[1..]).into_vec().map_err(|e| e.to_string())
}
