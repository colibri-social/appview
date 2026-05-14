//! Encrypted at-rest store for community-repo credentials.
//!
//! Each row in `community_credentials` holds the PDS endpoint, identifier, and
//! AES-256-GCM-encrypted app password for a single community DID. The AppView
//! uses these credentials to write on-protocol records (moderation events,
//! member admissions, etc.) onto the community's PDS.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};

use crate::lib::crypto::{self, CryptoError};
use crate::lib::time::current_iso8601_utc;
use crate::models::community_credentials::{
    self, ActiveModel as CredentialsModel, Entity as Credentials, Model as CredentialsRow,
};

/// `appview_managed` — DID was minted on the AppView's own PDS via createAccount.
pub const SOURCE_APPVIEW_MANAGED: &str = "appview_managed";
/// `byo` — user submitted credentials for a DID hosted on an external PDS.
pub const SOURCE_BYO: &str = "byo";

/// A decrypted credential bundle ready for use against a PDS.
///
/// `community_did` and `source` are populated for completeness — callers that
/// only need the PDS-talk fields (`pds_endpoint`, `identifier`, `password`)
/// ignore them, but they're useful for audit logging and admin tooling.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CommunityCredentials {
    pub community_did: String,
    pub pds_endpoint: String,
    pub identifier: String,
    pub password: String,
    pub source: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    #[error("database error: {0}")]
    Db(#[from] DbErr),
    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),
    #[error("stored ciphertext is not valid base64: {0}")]
    BadCiphertextEncoding(String),
    #[error("stored nonce is not valid base64: {0}")]
    BadNonceEncoding(String),
    #[error("password is not valid UTF-8 after decryption")]
    InvalidUtf8,
}

/// Encrypts `password` and upserts the credential row keyed by `community_did`.
/// On conflict every field is replaced — useful when a BYO user rotates their
/// app password.
pub async fn upsert_credentials(
    db: &DatabaseConnection,
    master_key: &[u8],
    community_did: &str,
    pds_endpoint: &str,
    identifier: &str,
    password: &str,
    source: &str,
) -> Result<(), CredentialsError> {
    let (ciphertext, nonce) = crypto::encrypt(password.as_bytes(), master_key)?;

    let row = CredentialsModel {
        community_did: ActiveValue::Set(community_did.to_string()),
        pds_endpoint: ActiveValue::Set(pds_endpoint.to_string()),
        identifier: ActiveValue::Set(identifier.to_string()),
        password_ciphertext_b64: ActiveValue::Set(BASE64.encode(&ciphertext)),
        password_nonce_b64: ActiveValue::Set(BASE64.encode(&nonce)),
        source: ActiveValue::Set(source.to_string()),
        created_at: ActiveValue::Set(current_iso8601_utc()),
    };

    Credentials::insert(row)
        .on_conflict(
            sea_query::OnConflict::column(community_credentials::Column::CommunityDid)
                .update_columns([
                    community_credentials::Column::PdsEndpoint,
                    community_credentials::Column::Identifier,
                    community_credentials::Column::PasswordCiphertextB64,
                    community_credentials::Column::PasswordNonceB64,
                    community_credentials::Column::Source,
                    community_credentials::Column::CreatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;

    Ok(())
}

/// Loads and decrypts the credentials for a given community DID. Returns
/// `Ok(None)` if no row exists.
pub async fn load_credentials(
    db: &DatabaseConnection,
    master_key: &[u8],
    community_did: &str,
) -> Result<Option<CommunityCredentials>, CredentialsError> {
    let Some(row) = fetch_row(db, community_did).await? else {
        return Ok(None);
    };
    Ok(Some(decrypt_row(row, master_key)?))
}

async fn fetch_row(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<Option<CredentialsRow>, DbErr> {
    Credentials::find()
        .filter(community_credentials::Column::CommunityDid.eq(community_did))
        .one(db)
        .await
}

fn decrypt_row(
    row: CredentialsRow,
    master_key: &[u8],
) -> Result<CommunityCredentials, CredentialsError> {
    let ciphertext = BASE64
        .decode(&row.password_ciphertext_b64)
        .map_err(|e| CredentialsError::BadCiphertextEncoding(e.to_string()))?;
    let nonce = BASE64
        .decode(&row.password_nonce_b64)
        .map_err(|e| CredentialsError::BadNonceEncoding(e.to_string()))?;

    let plaintext = crypto::decrypt(&ciphertext, &nonce, master_key)?;
    let password = String::from_utf8(plaintext).map_err(|_| CredentialsError::InvalidUtf8)?;

    Ok(CommunityCredentials {
        community_did: row.community_did,
        pds_endpoint: row.pds_endpoint,
        identifier: row.identifier,
        password,
        source: row.source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> Vec<u8> {
        let mut k = vec![0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn make_row(password: &str, key: &[u8], source: &str) -> CredentialsRow {
        let (ct, nonce) = crypto::encrypt(password.as_bytes(), key).unwrap();
        CredentialsRow {
            community_did: String::from("did:plc:test"),
            pds_endpoint: String::from("https://pds.example"),
            identifier: String::from("test.community"),
            password_ciphertext_b64: BASE64.encode(&ct),
            password_nonce_b64: BASE64.encode(&nonce),
            source: source.to_string(),
            created_at: String::from("2026-05-15T00:00:00.000Z"),
        }
    }

    #[test]
    fn decrypt_row_round_trips_password() {
        let key = test_key();
        let row = make_row("hunter2", &key, SOURCE_APPVIEW_MANAGED);
        let decrypted = decrypt_row(row, &key).unwrap();
        assert_eq!(decrypted.password, "hunter2");
        assert_eq!(decrypted.community_did, "did:plc:test");
        assert_eq!(decrypted.source, SOURCE_APPVIEW_MANAGED);
    }

    #[test]
    fn decrypt_row_fails_with_wrong_key() {
        let key = test_key();
        let mut wrong = test_key();
        wrong[0] ^= 0xff;
        let row = make_row("x", &key, SOURCE_BYO);
        assert!(matches!(
            decrypt_row(row, &wrong),
            Err(CredentialsError::Crypto(CryptoError::DecryptFailed))
        ));
    }

    #[test]
    fn decrypt_row_fails_with_malformed_base64() {
        let key = test_key();
        let mut row = make_row("x", &key, SOURCE_BYO);
        row.password_ciphertext_b64 = String::from("not!valid!base64!");
        assert!(matches!(
            decrypt_row(row, &key),
            Err(CredentialsError::BadCiphertextEncoding(_))
        ));
    }
}
