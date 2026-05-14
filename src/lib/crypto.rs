//! AES-256-GCM authenticated encryption for at-rest secrets like the PDS app
//! passwords stored in `community_credentials`. The master key is loaded once
//! from the `CREDENTIAL_ENCRYPTION_KEY` env var (32 bytes, base64 encoded).
//!
//! Each ciphertext is paired with a freshly generated 12-byte random nonce.

use std::sync::OnceLock;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;
use thiserror::Error;

const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

/// Global at-process-start master key. Set once in `main.rs` after env load;
/// accessed via [`master_key`] from anywhere that needs to encrypt/decrypt
/// at-rest secrets. Using a global keeps the per-call argument list small and
/// matches how at-rest secret keys are usually loaded.
static MASTER_KEY: OnceLock<Vec<u8>> = OnceLock::new();

/// Records the master key for the lifetime of the process. Returns an error if
/// called more than once or if the supplied key is the wrong length.
pub fn install_master_key(bytes: Vec<u8>) -> Result<(), CryptoError> {
    if bytes.len() != KEY_LEN {
        return Err(CryptoError::WrongKeyLength(bytes.len()));
    }
    MASTER_KEY
        .set(bytes)
        .map_err(|_| CryptoError::AlreadyInstalled)
}

/// Returns the previously installed master key. Panics if [`install_master_key`]
/// has not been called — only handlers that already require the key to be set
/// should call this.
pub fn master_key() -> &'static [u8] {
    MASTER_KEY
        .get()
        .expect("CREDENTIAL_ENCRYPTION_KEY not installed; call install_master_key at startup")
}

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("missing CREDENTIAL_ENCRYPTION_KEY env var")]
    MissingKey,
    #[error("CREDENTIAL_ENCRYPTION_KEY is not valid base64: {0}")]
    InvalidKeyEncoding(String),
    #[error("CREDENTIAL_ENCRYPTION_KEY must decode to exactly 32 bytes, got {0}")]
    WrongKeyLength(usize),
    #[error("nonce must be exactly 12 bytes, got {0}")]
    WrongNonceLength(usize),
    #[error("encryption failed")]
    EncryptFailed,
    #[error("decryption failed (wrong key, tampering, or corrupt ciphertext)")]
    DecryptFailed,
    #[error("master key has already been installed")]
    AlreadyInstalled,
}

/// Loads the AES-256-GCM master key from the `CREDENTIAL_ENCRYPTION_KEY` env
/// var. Returns an owned `Vec<u8>` so callers can pass it by reference into
/// `encrypt`/`decrypt`.
pub fn load_master_key_from_env() -> Result<Vec<u8>, CryptoError> {
    let encoded =
        std::env::var("CREDENTIAL_ENCRYPTION_KEY").map_err(|_| CryptoError::MissingKey)?;
    decode_master_key(&encoded)
}

/// Decodes a base64-encoded master key string into raw bytes.
pub fn decode_master_key(encoded: &str) -> Result<Vec<u8>, CryptoError> {
    let bytes = BASE64
        .decode(encoded.trim())
        .map_err(|e| CryptoError::InvalidKeyEncoding(e.to_string()))?;
    if bytes.len() != KEY_LEN {
        return Err(CryptoError::WrongKeyLength(bytes.len()));
    }
    Ok(bytes)
}

/// Encrypts `plaintext` with a fresh random nonce. Returns
/// `(ciphertext, nonce)`. The nonce must be stored alongside the ciphertext.
pub fn encrypt(plaintext: &[u8], master_key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), CryptoError> {
    if master_key.len() != KEY_LEN {
        return Err(CryptoError::WrongKeyLength(master_key.len()));
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(master_key));

    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| CryptoError::EncryptFailed)?;

    Ok((ciphertext, nonce_bytes.to_vec()))
}

/// Decrypts `ciphertext` using the stored `nonce` and the master key.
pub fn decrypt(ciphertext: &[u8], nonce: &[u8], master_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if master_key.len() != KEY_LEN {
        return Err(CryptoError::WrongKeyLength(master_key.len()));
    }
    if nonce.len() != NONCE_LEN {
        return Err(CryptoError::WrongNonceLength(nonce.len()));
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(master_key));
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| CryptoError::DecryptFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> Vec<u8> {
        let mut key = vec![0u8; KEY_LEN];
        for (i, slot) in key.iter_mut().enumerate() {
            *slot = i as u8;
        }
        key
    }

    #[test]
    fn round_trip_recovers_plaintext() {
        let key = test_key();
        let plaintext = b"super-secret-app-password";
        let (ct, nonce) = encrypt(plaintext, &key).unwrap();
        let decrypted = decrypt(&ct, &nonce, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn each_encrypt_uses_fresh_nonce() {
        let key = test_key();
        let plaintext = b"hello";
        let (ct1, n1) = encrypt(plaintext, &key).unwrap();
        let (ct2, n2) = encrypt(plaintext, &key).unwrap();
        assert_ne!(n1, n2, "nonces must differ between calls");
        assert_ne!(ct1, ct2, "ciphertexts must differ with different nonces");
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        let key = test_key();
        let mut wrong_key = test_key();
        wrong_key[0] ^= 0xff;
        let (ct, nonce) = encrypt(b"x", &key).unwrap();
        assert!(matches!(
            decrypt(&ct, &nonce, &wrong_key),
            Err(CryptoError::DecryptFailed)
        ));
    }

    #[test]
    fn decrypt_with_tampered_ciphertext_fails() {
        let key = test_key();
        let (mut ct, nonce) = encrypt(b"abcdef", &key).unwrap();
        ct[0] ^= 0xff;
        assert!(matches!(
            decrypt(&ct, &nonce, &key),
            Err(CryptoError::DecryptFailed)
        ));
    }

    #[test]
    fn rejects_short_key() {
        let bad = vec![0u8; 16];
        assert!(matches!(
            encrypt(b"x", &bad),
            Err(CryptoError::WrongKeyLength(16))
        ));
    }

    #[test]
    fn rejects_short_nonce() {
        let key = test_key();
        let ct = vec![0u8; 16];
        assert!(matches!(
            decrypt(&ct, &[0u8; 8], &key),
            Err(CryptoError::WrongNonceLength(8))
        ));
    }

    #[test]
    fn decode_master_key_accepts_valid_base64() {
        let key = test_key();
        let encoded = BASE64.encode(&key);
        assert_eq!(decode_master_key(&encoded).unwrap(), key);
    }

    #[test]
    fn decode_master_key_rejects_wrong_length() {
        let encoded = BASE64.encode([0u8; 16]);
        assert!(matches!(
            decode_master_key(&encoded),
            Err(CryptoError::WrongKeyLength(16))
        ));
    }
}
