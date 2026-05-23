//! `social.colibri.community.registerCredentials` — Variant B registration.
//!
//! Lets a caller submit PDS endpoint + identifier + app password for a
//! community DID hosted on a PDS the AppView doesn't manage. We verify
//! proof-of-control by performing a `createSession` against the supplied PDS
//! with the supplied credentials — if the PDS issues a session whose `did`
//! matches the claimed community DID, we accept the credentials and store
//! them encrypted.
//!
//! No bootstrap happens here: BYO callers are expected to provision the
//! community / role / member records themselves (or already have).

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::community_credentials::{self, SOURCE_BYO};
use crate::lib::crypto;
use crate::lib::pds_client::{self, PdsError, PdsSession};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};

#[derive(Serialize, Debug)]
pub struct RegisterCredentialsResponse {
    pub did: String,
    pub source: String,
}

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type CreateSessionFn = dyn Fn(String, String, String) -> BoxFuture<'static, Result<PdsSession, PdsError>>
    + Send
    + Sync;
type UpsertFn =
    dyn Fn(String, String, String, String) -> BoxFuture<'static, Result<(), String>> + Send + Sync;

#[allow(clippy::too_many_arguments)]
async fn register_with(
    auth: String,
    community_did: String,
    pds_endpoint: String,
    identifier: String,
    password: String,
    verify_auth_fn: &VerifyAuthFn,
    create_session_fn: &CreateSessionFn,
    upsert_fn: &UpsertFn,
) -> Result<Json<RegisterCredentialsResponse>, ErrorResponse> {
    // Service auth proves who is *submitting* the credentials. We don't
    // require the caller's DID to match the community DID — the AppView, not
    // the caller, will be using the stored credentials going forward.
    let _caller_did = verify_auth_fn(
        auth,
        String::from("social.colibri.community.registerCredentials"),
    )
    .await
    .map_err(auth_error)?;

    // Proof-of-control: if the PDS accepts these credentials and returns a
    // session for the claimed DID, the caller has demonstrated effective
    // control over the community repo.
    let session = create_session_fn(pds_endpoint.clone(), identifier.clone(), password.clone())
        .await
        .map_err(|e| upstream_error(format!("createSession failed: {e}")))?;

    if session.did != community_did {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: format!(
                    "credentials authenticate as {} but registration claimed {}",
                    session.did, community_did
                ),
            }),
        });
    }

    upsert_fn(community_did.clone(), pds_endpoint, identifier, password)
        .await
        .map_err(|e| internal_error(format!("failed to persist credentials: {e}")))?;

    Ok(Json(RegisterCredentialsResponse {
        did: community_did,
        source: SOURCE_BYO.to_string(),
    }))
}

fn auth_error(err: ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

fn upstream_error(message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("UpstreamError"),
            message,
        }),
    }
}

fn internal_error(message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InternalServerError"),
            message,
        }),
    }
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn create_session_boxed(
    pds_endpoint: String,
    identifier: String,
    password: String,
) -> BoxFuture<'static, Result<PdsSession, PdsError>> {
    Box::pin(async move { pds_client::create_session(&pds_endpoint, &identifier, &password).await })
}

#[post(
    "/xrpc/social.colibri.community.registerCredentials?<did>&<pds>&<identifier>&<password>&<auth>"
)]
/// Stores BYO credentials for a community DID after verifying via createSession.
#[allow(clippy::too_many_arguments)]
pub async fn register_credentials(
    did: &str,
    pds: &str,
    identifier: &str,
    password: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<RegisterCredentialsResponse>, ErrorResponse> {
    let db_for_upsert = db.inner().clone();
    let upsert = move |community_did: String,
                       pds_endpoint: String,
                       identifier: String,
                       password: String|
          -> BoxFuture<'static, Result<(), String>> {
        let db = db_for_upsert.clone();
        Box::pin(async move {
            community_credentials::upsert_credentials(
                &db,
                crypto::master_key(),
                &community_did,
                &pds_endpoint,
                &identifier,
                &password,
                SOURCE_BYO,
            )
            .await
            .map_err(|e| e.to_string())
        })
    };

    let response = register_with(
        auth.to_string(),
        did.to_string(),
        pds.to_string(),
        identifier.to_string(),
        password.to_string(),
        &verify_auth_boxed,
        &create_session_boxed,
        &upsert,
    )
    .await?;

    // Register the BYO community DID with Tap so the firehose starts
    // delivering its records into `record_data`. Tap's backfill path picks
    // up everything already on the PDS at the moment of registration, so a
    // community that pre-existed the AppView still gets indexed in full.
    //
    // Done in the handler (not in `register_with`) so unit tests don't
    // trip on the env-var reads (`TAP_HOSTNAME`, `TAP_ADMIN_PASSWORD`)
    // inside `register_dids`.
    crate::lib::tap::register_dids(vec![response.did.clone()]).await;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn session_for(did: &str) -> PdsSession {
        PdsSession {
            access_jwt: String::from("jwt"),
            did: did.to_string(),
            handle: None,
        }
    }

    #[tokio::test]
    async fn stores_credentials_when_proof_of_control_succeeds() {
        let captured: Arc<Mutex<Option<(String, String, String, String)>>> =
            Arc::new(Mutex::new(None));
        let cap = captured.clone();

        let upsert = move |did: String,
                           pds: String,
                           identifier: String,
                           password: String|
              -> BoxFuture<'static, Result<(), String>> {
            let cap = cap.clone();
            Box::pin(async move {
                *cap.lock().unwrap() = Some((did, pds, identifier, password));
                Ok(())
            })
        };
        let result = register_with(
            String::from("token"),
            String::from("did:plc:community"),
            String::from("https://pds.example"),
            String::from("community.example"),
            String::from("app-password"),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| Box::pin(async { Ok(session_for("did:plc:community")) }),
            &upsert,
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:community");
        assert_eq!(result.source, SOURCE_BYO);
        let saved = captured.lock().unwrap().take().unwrap();
        assert_eq!(saved.0, "did:plc:community");
        assert_eq!(saved.3, "app-password");
    }

    #[tokio::test]
    async fn rejects_when_session_did_does_not_match_claim() {
        let result = register_with(
            String::from("token"),
            String::from("did:plc:community"),
            String::from("https://pds.example"),
            String::from("community.example"),
            String::from("app-password"),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| Box::pin(async { Ok(session_for("did:plc:imposter")) }),
            &|_, _, _, _| Box::pin(async { panic!("should not persist on did mismatch") }),
        )
        .await;

        assert!(result.is_err());
        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "InvalidRequest");
        assert!(body.message.contains("imposter"));
    }

    #[tokio::test]
    async fn rejects_when_pds_session_fails() {
        let result = register_with(
            String::from("token"),
            String::from("did:plc:community"),
            String::from("https://pds.example"),
            String::from("community.example"),
            String::from("app-password"),
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| {
                Box::pin(async {
                    Err(PdsError::BadStatus {
                        status: 401,
                        body: String::from("bad password"),
                    })
                })
            },
            &|_, _, _, _| Box::pin(async { panic!("should not persist on session failure") }),
        )
        .await;

        assert!(result.is_err());
        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "UpstreamError");
        assert!(body.message.contains("createSession"));
    }

    #[tokio::test]
    async fn rejects_when_auth_fails() {
        let result = register_with(
            String::from("token"),
            String::from("did:plc:community"),
            String::from("https://pds.example"),
            String::from("community.example"),
            String::from("app-password"),
            &|_, _| Box::pin(async { Err(ServiceAuthError::InvalidSignature) }),
            &|_, _, _| Box::pin(async { panic!("should not call") }),
            &|_, _, _, _| Box::pin(async { panic!("should not call") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
