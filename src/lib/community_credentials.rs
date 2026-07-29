//! Encrypted at-rest store for community-repo credentials.
//!
//! Each row in `community_credentials` holds the PDS endpoint, identifier, and
//! AES-256-GCM-encrypted app password for a single community DID. The AppView
//! uses these credentials to write on-protocol records (moderation events,
//! member admissions, etc.) onto the community's PDS.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};

use crate::lib::community_session_cache;
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
#[derive(Debug, Clone)]
pub struct CommunityCredentials {
    pub community_did: String,
    pub pds_endpoint: String,
    pub identifier: String,
    pub password: String,
    pub source: String,
}

impl CommunityCredentials {
    /// The identifier to present to `com.atproto.server.createSession`
    pub fn login_identifier(&self) -> &str {
        if self.source == SOURCE_APPVIEW_MANAGED {
            &self.community_did
        } else {
            &self.identifier
        }
    }
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

impl CredentialsError {
    /// Whether the stored password can never be read back — a crypto failure, bad
    /// base64, or non-UTF-8 plaintext. The row exists; its secret is lost, so the
    /// only way forward is to replace the password outright.
    ///
    /// Excludes `Db` on purpose: a transient database error must never be
    /// mistaken for unreadable ciphertext, or a blip would trigger a password
    /// rotation.
    pub fn is_undecryptable(&self) -> bool {
        matches!(
            self,
            CredentialsError::Crypto(_)
                | CredentialsError::BadCiphertextEncoding(_)
                | CredentialsError::BadNonceEncoding(_)
                | CredentialsError::InvalidUtf8
        )
    }
}

const MISSING_CREDENTIALS_MARKER: &str = "no credentials registered for community ";

/// The error every write path returns when this AppView holds no credentials
/// for `community_did`.
pub fn missing_credentials_err(community_did: &str) -> DbErr {
    DbErr::Custom(format!("{MISSING_CREDENTIALS_MARKER}{community_did}"))
}

/// Whether `err` is the missing-credentials case, without logging. Callers that
/// need the one-shot warning should use [`warn_missing_credentials_once`].
pub fn is_missing_credentials(err: &DbErr) -> bool {
    err.to_string().contains(MISSING_CREDENTIALS_MARKER)
}

/// Community DIDs already warned about
static WARNED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Reports whether `err` is the missing-credentials case, logging a single
/// warning the first time it's seen for a given community
pub fn warn_missing_credentials_once(err: &DbErr) -> bool {
    let message = err.to_string();
    let Some(idx) = message.find(MISSING_CREDENTIALS_MARKER) else {
        return false;
    };
    let community_did = message[idx + MISSING_CREDENTIALS_MARKER.len()..]
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();

    if WARNED.lock().unwrap().insert(community_did.clone()) {
        log::warn!(
            "this AppView holds no credentials for community {community_did}; skipping \
             community-side writes for it"
        );
    }
    true
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

    community_session_cache::invalidate(community_did);
    Ok(())
}

/// Re-encrypts and stores just the password for an existing row, leaving
/// `pds_endpoint`, `identifier` and `source` untouched. Returns the number of
/// rows updated; `0` means no row exists, which callers must read as "this
/// community went away" rather than re-creating it.
///
/// Deliberately not [`upsert_credentials`]: that replaces every column and
/// inserts when absent, so using it here would let a bug in the recovery path
/// flip a row's `source` or resurrect a community mid-deletion.
pub async fn update_password(
    db: &DatabaseConnection,
    master_key: &[u8],
    community_did: &str,
    password: &str,
) -> Result<u64, CredentialsError> {
    let (ciphertext, nonce) = crypto::encrypt(password.as_bytes(), master_key)?;

    let res = Credentials::update_many()
        .col_expr(
            community_credentials::Column::PasswordCiphertextB64,
            sea_query::Expr::value(BASE64.encode(&ciphertext)),
        )
        .col_expr(
            community_credentials::Column::PasswordNonceB64,
            sea_query::Expr::value(BASE64.encode(&nonce)),
        )
        .col_expr(
            community_credentials::Column::CreatedAt,
            sea_query::Expr::value(current_iso8601_utc()),
        )
        .filter(community_credentials::Column::CommunityDid.eq(community_did))
        .exec(db)
        .await?;

    community_session_cache::invalidate(community_did);
    Ok(res.rows_affected)
}

/// Removes the stored credential row for `community_did`. Returns the number
/// of rows deleted (0 if none was stored). Called when a community is deleted
/// so the AppView stops holding credentials it no longer needs.
pub async fn delete_credentials(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<u64, DbErr> {
    let res = Credentials::delete_many()
        .filter(community_credentials::Column::CommunityDid.eq(community_did))
        .exec(db)
        .await?;

    community_session_cache::invalidate(community_did);
    Ok(res.rows_affected)
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

/// The stored PDS endpoint for `community_did` plus the `source` it was
/// registered under, or `None` when this AppView holds no credentials for it.
pub async fn stored_pds_endpoint(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<Option<(String, String)>, DbErr> {
    Ok(fetch_row(db, community_did)
        .await?
        .map(|row| (row.pds_endpoint, row.source)))
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
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

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
    fn missing_credentials_err_is_recognised_and_warned_once() {
        let err = missing_credentials_err("did:plc:notours");

        // First sighting warns, later ones are silent
        assert!(warn_missing_credentials_once(&err));
        assert!(warn_missing_credentials_once(&err));
        assert!(WARNED.lock().unwrap().contains("did:plc:notours"));
    }

    #[test]
    fn unrelated_errors_are_not_swallowed() {
        let err = DbErr::Custom(String::from("pds write failed: 500"));
        assert!(!warn_missing_credentials_once(&err));
    }

    /// A managed community logs in by DID, so a handle that changed or failed
    /// re-verification can never masquerade as a broken password.
    #[test]
    fn managed_rows_log_in_by_did_and_byo_rows_by_identifier() {
        let key = test_key();

        let managed = decrypt_row(make_row("pw", &key, SOURCE_APPVIEW_MANAGED), &key).unwrap();
        assert_eq!(managed.login_identifier(), "did:plc:test");

        let byo = decrypt_row(make_row("pw", &key, SOURCE_BYO), &key).unwrap();
        assert_eq!(byo.login_identifier(), "test.community");
    }

    /// Rotation must not be able to change a row's provenance. This is what makes
    /// `update_password` worth having instead of reusing `upsert_credentials`,
    /// whose `ON CONFLICT` replaces every column.
    #[tokio::test]
    async fn update_password_touches_only_the_secret_columns() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 0,
                rows_affected: 1,
            }])
            .into_connection();

        let rows = update_password(&db, &test_key(), "did:plc:test", "rotated")
            .await
            .unwrap();
        assert_eq!(rows, 1);

        let stmt = format!("{:?}", db.into_transaction_log()[0]);
        assert!(stmt.contains("UPDATE"), "{stmt}");
        assert!(stmt.contains("password_ciphertext_b64"), "{stmt}");
        assert!(stmt.contains("password_nonce_b64"), "{stmt}");
        assert!(!stmt.contains("\"source\""), "source must survive: {stmt}");
        assert!(
            !stmt.contains("pds_endpoint"),
            "endpoint must survive: {stmt}"
        );
        assert!(
            !stmt.contains("\"identifier\""),
            "identifier must survive: {stmt}"
        );
    }

    /// A database blip must not be mistaken for lost ciphertext, or it would
    /// trigger a password rotation.
    #[test]
    fn only_crypto_failures_count_as_undecryptable() {
        assert!(CredentialsError::Crypto(CryptoError::DecryptFailed).is_undecryptable());
        assert!(CredentialsError::BadCiphertextEncoding(String::from("x")).is_undecryptable());
        assert!(CredentialsError::BadNonceEncoding(String::from("x")).is_undecryptable());
        assert!(CredentialsError::InvalidUtf8.is_undecryptable());

        assert!(
            !CredentialsError::Db(DbErr::Custom(String::from("connection reset")))
                .is_undecryptable()
        );
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
