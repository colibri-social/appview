//! Repairing this AppView's own write access to a community it provisioned.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use rocket::tokio::sync::Mutex as AsyncMutex;
use sea_orm::{DatabaseConnection, DbErr};

use crate::lib::community_credentials::{self, SOURCE_APPVIEW_MANAGED};
use crate::lib::community_session_cache;
use crate::lib::crypto;
use crate::lib::pds_client::{self, PdsError, PdsSession};
use crate::lib::repo_endpoint;
use crate::lib::responses;

/// The record that distinguishes a community repo from a person's account.
const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_SELF_RKEY: &str = "self";

/// Minimum spacing between recovery attempts for one community, applied whether
/// the previous attempt succeeded or failed.
const ATTEMPT_INTERVAL: Duration = Duration::from_secs(30);

/// How long to leave a community alone after rotating its password failed to
/// restore access. That means something a retry cannot fix
const POISON_INTERVAL: Duration = Duration::from_secs(600);

/// Per-community serialisation for recovery, so a burst of writes all failing at
/// once mints one password rather than one each.
static DID_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Earliest time another recovery attempt is allowed, per community DID.
static NEXT_ATTEMPT: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Whether the missing-`PDS_ADMIN_PASS` warning has already been logged.
static WARNED_NO_ADMIN_PASS: AtomicBool = AtomicBool::new(false);

/// The lock guarding recovery for `community_did`.
fn did_lock(community_did: &str) -> Arc<AsyncMutex<()>> {
    let mut locks = DID_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    locks.entry(community_did.to_string()).or_default().clone()
}

/// Whether `community_did` may be attempted now, recording the next-allowed time
/// when it may. A community never seen before is always allowed: firehose paths do
/// not redeliver, so suppressing a first attempt loses a record for good.
fn claim_attempt(community_did: &str) -> bool {
    let now = Instant::now();
    let mut gates = NEXT_ATTEMPT.lock().unwrap_or_else(|e| e.into_inner());

    if gates.get(community_did).is_some_and(|next| *next > now) {
        return false;
    }
    gates.insert(community_did.to_string(), now + ATTEMPT_INTERVAL);
    true
}

/// Holds off further attempts for `community_did` for the longer poison window.
fn poison(community_did: &str) {
    NEXT_ATTEMPT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(community_did.to_string(), Instant::now() + POISON_INTERVAL);
}

/// Whether this AppView administers the PDS hosting `did`, answered **without**
/// relying on a usable credentials row. `Some(pds_loc)` is the configured
/// `PDS_LOC`, ready to address admin calls to, `None` means hands off.
pub async fn hosted_on_managed_pds(
    db: &DatabaseConnection,
    did: &str,
) -> Result<Option<String>, DbErr> {
    // A row's `pds_endpoint` and `source` are plaintext, so this answers even when
    // the password beside them cannot be decrypted.
    if let Some((endpoint, source)) = community_credentials::stored_pds_endpoint(db, did).await? {
        return Ok(repo_endpoint::own_pds_endpoint(&endpoint, &source));
    }

    // No row at all, so fall back to the network: the DID document says where the
    // repo lives, and the repo itself says whether it is a community.
    let Some(pds_loc) = did_document_points_at_us(did).await else {
        return Ok(None);
    };

    if !hosts_community_record(&pds_loc, did).await {
        log::warn!(
            "{did} is hosted on our own PDS but carries no {COMMUNITY_NSID}/{COMMUNITY_SELF_RKEY} \
             record, so it is not a community this AppView manages; refusing to touch its password"
        );
        return Ok(None);
    }

    Ok(Some(pds_loc))
}

/// The configured `PDS_LOC` when `did`'s DID document names it as the repo's home.
async fn did_document_points_at_us(did: &str) -> Option<String> {
    let doc = crate::xrpc::com::atproto::identity::resolve_did(did)
        .await
        .ok()?;
    repo_endpoint::matches_own_pds(doc.pds_endpoint()?)
}

/// Whether the repo at `did` carries a community record, i.e. it is a community
/// rather than somebody's personal account. This is the gate that keeps recovery
/// from resetting a *user's* password on a deployment that shares one PDS between
/// users and communities.
async fn hosts_community_record(pds_loc: &str, did: &str) -> bool {
    matches!(
        pds_client::get_record_trusted(pds_loc, did, COMMUNITY_NSID, COMMUNITY_SELF_RKEY).await,
        Ok(Some(_))
    )
}

/// The PDS admin password, or `None` when it isn't configured — in which case
/// recovery is simply unavailable and callers keep their original error.
fn admin_password() -> Option<String> {
    let configured = std::env::var("PDS_ADMIN_PASS")
        .ok()
        .filter(|pass| !pass.trim().is_empty());

    if configured.is_none() && !WARNED_NO_ADMIN_PASS.swap(true, Ordering::Relaxed) {
        log::warn!(
            "PDS_ADMIN_PASS is not set, so this AppView cannot repair its own credentials for \
             communities on its PDS; community-side writes will fail permanently if a password is lost"
        );
    }
    configured
}

/// How to store the password a recovery attempt just minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Persist {
    /// A row exists, replace only its secret, and treat "no row updated" as the
    /// community having been deleted mid-recovery.
    ExistingRow,
    /// No row exists; establish one as `appview_managed`, identified by DID.
    NewRow,
}

/// What one recovery attempt concluded.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Access restored `(pds_endpoint, access_jwt)`.
    Session(String, String),
    /// Suppressed by the attempt gate; the caller keeps its original error.
    Skipped,
    /// The credentials row vanished while we worked, so the community is being
    /// deleted and must not be written to.
    Vanished,
    /// The password was rotated but logging in still failed. Terminal.
    Unrecoverable,
}

// (community_did, new_password)
type RotateFn = dyn Fn(String, String) -> BoxFuture<'static, Result<(), PdsError>> + Send + Sync;
// (community_did, password) — managed communities always log in by DID.
type LoginFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<PdsSession, PdsError>> + Send + Sync;
// (community_did, new_password) -> rows affected
type PersistFn = dyn Fn(String, String) -> BoxFuture<'static, Result<u64, DbErr>> + Send + Sync;
// (community_did) -> the password currently stored, if any
type ReloadFn = dyn Fn(String) -> BoxFuture<'static, Result<Option<String>, DbErr>> + Send + Sync;

/// Rotates the password for a community whose stored one no longer works (or can
/// no longer be read) and returns a live session. `Ok(None)` means recovery was
/// not applicable or was suppressed, and the caller should keep whatever error it
/// already had.
pub async fn recover_session(
    db: &DatabaseConnection,
    community_did: &str,
    failed_password: Option<&str>,
) -> Result<Option<(String, String)>, DbErr> {
    recover(db, community_did, failed_password, Persist::ExistingRow).await
}

/// Establishes credentials for a community whose row is gone entirely, provided it
/// still lives on our PDS. This is what makes a lost or restored-from-backup
/// `community_credentials` table self-healing.
pub async fn adopt_orphan_session(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<Option<(String, String)>, DbErr> {
    recover(db, community_did, None, Persist::NewRow).await
}

/// Recovers `community_did` whether or not a row survives for it, for callers that
/// should not have to know which — the operator endpoint, chiefly.
pub async fn force_recovery(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<Option<(String, String)>, DbErr> {
    let persist = match community_credentials::stored_pds_endpoint(db, community_did).await? {
        Some(_) => Persist::ExistingRow,
        None => Persist::NewRow,
    };
    recover(db, community_did, None, persist).await
}

async fn recover(
    db: &DatabaseConnection,
    community_did: &str,
    failed_password: Option<&str>,
    persist: Persist,
) -> Result<Option<(String, String)>, DbErr> {
    let Some(pds_loc) = hosted_on_managed_pds(db, community_did).await? else {
        return Ok(None);
    };
    let Some(admin_password) = admin_password() else {
        return Ok(None);
    };

    let outcome = recover_with(
        community_did,
        &pds_loc,
        failed_password,
        &rotate_fn(&pds_loc, &admin_password),
        &login_fn(&pds_loc),
        &persist_fn(db, &pds_loc, persist),
        &reload_fn(db),
    )
    .await?;

    match outcome {
        Outcome::Session(endpoint, jwt) => Ok(Some((endpoint, jwt))),
        Outcome::Skipped | Outcome::Vanished => Ok(None),
        Outcome::Unrecoverable => {
            Err(unrecoverable_err(community_did, &pds_loc, &admin_password).await)
        }
    }
}

/// The recovery state machine, with its side effects injected so the sequencing,
/// locking and gating are testable without a PDS or a database.
async fn recover_with(
    community_did: &str,
    pds_endpoint: &str,
    failed_password: Option<&str>,
    rotate_fn: &RotateFn,
    login_fn: &LoginFn,
    persist_fn: &PersistFn,
    reload_fn: &ReloadFn,
) -> Result<Outcome, DbErr> {
    // Serialise per community: a burst of firehose-driven writes all failing at
    // once must produce one rotation, not one per write.
    let lock = did_lock(community_did);
    let _guard = lock.lock().await;

    // A peer may have finished recovering while we queued for the lock. Its
    // session is as good as one we would mint, and reusing it is what lets a whole
    // burst of failed writes succeed off a single rotation
    if let Some((endpoint, jwt)) = community_session_cache::get(community_did) {
        return Ok(Outcome::Session(endpoint, jwt));
    }

    if let Some(session) =
        peer_already_rotated(community_did, failed_password, login_fn, reload_fn).await?
    {
        return Ok(Outcome::Session(
            pds_endpoint.to_string(),
            session.access_jwt,
        ));
    }

    if !claim_attempt(community_did) {
        log::debug!("skipping credential recovery for {community_did}: attempted too recently");
        return Ok(Outcome::Skipped);
    }

    let new_password = pds_client::generate_strong_password();

    // Rotate on the PDS *before* storing anything. The other order would destroy
    // the only working password if the PDS call then failed.
    rotate_fn(community_did.to_string(), new_password.clone())
        .await
        .map_err(|e| {
            // The response body from this endpoint carries no secret, but a
            // rejected admin password deserves to be named as such.
            if e.is_invalid_credentials() {
                log::error!(
                    "PDS admin auth was rejected while recovering {community_did}; is \
                     PDS_ADMIN_PASS correct?"
                );
            }
            DbErr::Custom(format!(
                "{}could not mint a new password for community {community_did}: {e}",
                responses::CREDENTIALS_UNRECOVERABLE_MARKER
            ))
        })?;

    // The only authoritative check. `updateAccountPassword` applies an UPDATE
    // keyed on the DID with no rows-affected check, so it answers 200 even for a
    // DID the PDS doesn't host.
    let session = match login_fn(community_did.to_string(), new_password.clone()).await {
        Ok(session) => session,
        Err(_) => {
            poison(community_did);
            return Ok(Outcome::Unrecoverable);
        }
    };

    match persist_fn(community_did.to_string(), new_password).await {
        Ok(0) => return Ok(Outcome::Vanished),
        Ok(_) => {}
        Err(e) => {
            // Rotation already happened and we hold a working session, so the
            // write in flight should still go through; the next one will find the
            // stored password dead and rotate again. Same reasoning as the
            // optimistic cache updates in `community_write`.
            log::error!(
                "recovered access to community {community_did} but could not store the new \
                 password: {e} (the next write will rotate again)"
            );
        }
    }

    log::info!(
        "restored write access to community {community_did} by minting a new password on the PDS \
         this AppView administers"
    );

    // Publish before releasing the lock, so peers queued behind us see the session
    // rather than racing into the attempt gate and failing their writes.
    community_session_cache::put(community_did, pds_endpoint, &session.access_jwt);

    Ok(Outcome::Session(
        pds_endpoint.to_string(),
        session.access_jwt,
    ))
}

/// If a peer task rotated while we waited for the lock, the stored password now
/// differs from the one that just failed, so try it before spending a rotation of
/// our own. Compares the password *value* rather than a timestamp, which makes the
/// check unambiguous.
async fn peer_already_rotated(
    community_did: &str,
    failed_password: Option<&str>,
    login_fn: &LoginFn,
    reload_fn: &ReloadFn,
) -> Result<Option<PdsSession>, DbErr> {
    let Some(failed) = failed_password else {
        return Ok(None);
    };
    let Some(current) = reload_fn(community_did.to_string()).await? else {
        return Ok(None);
    };
    if current == failed {
        return Ok(None);
    }
    Ok(login_fn(community_did.to_string(), current).await.ok())
}

/// Builds the terminal error, first asking the PDS *why* so the log names the
/// actual cause rather than leaving an operator to guess.
async fn unrecoverable_err(community_did: &str, pds_loc: &str, admin_password: &str) -> DbErr {
    let cause =
        match pds_client::admin_get_account_info(pds_loc, admin_password, community_did).await {
            Ok(None) => String::from("the PDS no longer hosts this account"),
            Ok(Some(_)) => {
                String::from("the account exists but rejects the password we just set for it")
            }
            Err(e) => format!("and the account could not be inspected either ({e})"),
        };

    log::error!(
        "could not recover write access to community {community_did}: {cause}. Community-side \
         writes for it will keep failing until this is resolved."
    );

    DbErr::Custom(format!(
        "{}unable to recover credentials for community {community_did}: {cause}",
        responses::CREDENTIALS_UNRECOVERABLE_MARKER
    ))
}

// ---- Production wiring -----------------------------------------------------

fn rotate_fn(pds_loc: &str, admin_password: &str) -> Box<RotateFn> {
    let pds_loc = pds_loc.to_string();
    let admin_password = admin_password.to_string();
    Box::new(move |community_did, new_password| {
        let pds_loc = pds_loc.clone();
        let admin_password = admin_password.clone();
        Box::pin(async move {
            pds_client::admin_update_account_password(
                &pds_loc,
                &admin_password,
                &community_did,
                &new_password,
            )
            .await
        })
    })
}

fn login_fn(pds_loc: &str) -> Box<LoginFn> {
    let pds_loc = pds_loc.to_string();
    Box::new(move |community_did, password| {
        let pds_loc = pds_loc.clone();
        Box::pin(
            async move { pds_client::create_session(&pds_loc, &community_did, &password).await },
        )
    })
}

fn persist_fn(db: &DatabaseConnection, pds_loc: &str, persist: Persist) -> Box<PersistFn> {
    let db = db.clone();
    let pds_loc = pds_loc.to_string();
    Box::new(move |community_did, new_password| {
        let db = db.clone();
        let pds_loc = pds_loc.clone();
        Box::pin(async move {
            let master_key = crypto::master_key();
            match persist {
                Persist::ExistingRow => {
                    community_credentials::update_password(
                        &db,
                        master_key,
                        &community_did,
                        &new_password,
                    )
                    .await
                }
                Persist::NewRow => {
                    // Establish the row ourselves rather than routing through
                    // `registerCredentials`, which hardcodes `byo` and would
                    // permanently mis-classify a community we own.
                    community_credentials::upsert_credentials(
                        &db,
                        master_key,
                        &community_did,
                        &pds_loc,
                        &community_did,
                        &new_password,
                        SOURCE_APPVIEW_MANAGED,
                    )
                    .await
                    .map(|()| 1)
                }
            }
            .map_err(|e| DbErr::Custom(format!("could not store recovered credentials: {e}")))
        })
    })
}

fn reload_fn(db: &DatabaseConnection) -> Box<ReloadFn> {
    let db = db.clone();
    Box::new(move |community_did| {
        let db = db.clone();
        Box::pin(async move {
            match community_credentials::load_credentials(&db, crypto::master_key(), &community_did)
                .await
            {
                Ok(row) => Ok(row.map(|creds| creds.password)),
                // Unreadable ciphertext is not a reason to fail the whole
                // recovery, it just means there is nothing to compare against.
                Err(e) if e.is_undecryptable() => Ok(None),
                Err(e) => Err(DbErr::Custom(format!(
                    "could not re-read credentials for {community_did}: {e}"
                ))),
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use std::sync::atomic::AtomicUsize;

    /// Each test uses a distinct DID: the lock map and attempt gate are
    /// process-global, and cargo runs these on parallel threads.
    fn unique_did(tag: &str) -> String {
        format!("did:plc:recovery-{tag}")
    }

    fn session() -> PdsSession {
        PdsSession {
            access_jwt: String::from("fresh-jwt"),
            did: String::from("did:plc:whatever"),
            handle: None,
        }
    }

    fn ok_rotate(counter: Arc<AtomicUsize>) -> Box<RotateFn> {
        Box::new(move |_did, _pw| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        })
    }

    fn never_rotate() -> Box<RotateFn> {
        Box::new(|_did, _pw| Box::pin(async { panic!("rotation must not be attempted") }))
    }

    fn ok_login() -> Box<LoginFn> {
        Box::new(|_did, _pw| Box::pin(async { Ok(session()) }))
    }

    fn rejecting_login() -> Box<LoginFn> {
        Box::new(|_did, _pw| {
            Box::pin(async {
                Err(PdsError::BadStatus {
                    status: 401,
                    body: String::from(r#"{"error":"AuthenticationRequired"}"#),
                })
            })
        })
    }

    fn persist_rows(rows: u64) -> Box<PersistFn> {
        Box::new(move |_did, _pw| Box::pin(async move { Ok(rows) }))
    }

    fn failing_persist() -> Box<PersistFn> {
        Box::new(|_did, _pw| Box::pin(async { Err(DbErr::Custom(String::from("disk on fire"))) }))
    }

    fn reload_none() -> Box<ReloadFn> {
        Box::new(|_did| Box::pin(async { Ok(None) }))
    }

    fn reload_password(password: &'static str) -> Box<ReloadFn> {
        Box::new(move |_did| Box::pin(async move { Ok(Some(String::from(password))) }))
    }

    #[tokio::test]
    async fn rotates_once_and_returns_a_live_session() {
        let did = unique_did("happy");
        let rotations = Arc::new(AtomicUsize::new(0));

        let outcome = recover_with(
            &did,
            "https://pds.example",
            Some("stale"),
            &ok_rotate(rotations.clone()),
            &ok_login(),
            &persist_rows(1),
            &reload_password("stale"),
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            Outcome::Session(
                String::from("https://pds.example"),
                String::from("fresh-jwt")
            )
        );
        assert_eq!(rotations.load(Ordering::SeqCst), 1);
    }

    /// Rotating but still failing to log in means the account is gone; that must
    /// be terminal rather than looping.
    #[tokio::test]
    async fn a_rotation_that_does_not_restore_access_is_terminal() {
        let did = unique_did("gone");
        let rotations = Arc::new(AtomicUsize::new(0));

        let outcome = recover_with(
            &did,
            "https://pds.example",
            None,
            &ok_rotate(rotations.clone()),
            &rejecting_login(),
            &persist_rows(1),
            &reload_none(),
        )
        .await
        .unwrap();

        assert_eq!(outcome, Outcome::Unrecoverable);
        assert_eq!(rotations.load(Ordering::SeqCst), 1);
    }

    /// Availability over consistency: the rotation happened and we hold a working
    /// session, so the write in flight should still succeed.
    #[tokio::test]
    async fn a_failed_persist_still_yields_the_session() {
        let did = unique_did("persist-failed");
        let rotations = Arc::new(AtomicUsize::new(0));

        let outcome = recover_with(
            &did,
            "https://pds.example",
            None,
            &ok_rotate(rotations.clone()),
            &ok_login(),
            &failing_persist(),
            &reload_none(),
        )
        .await
        .unwrap();

        assert!(matches!(outcome, Outcome::Session(..)));
    }

    /// Zero rows updated means the community was deleted while we worked, so it
    /// must not be resurrected or written to.
    #[tokio::test]
    async fn a_vanished_row_is_not_resurrected() {
        let did = unique_did("vanished");

        let outcome = recover_with(
            &did,
            "https://pds.example",
            None,
            &ok_rotate(Arc::new(AtomicUsize::new(0))),
            &ok_login(),
            &persist_rows(0),
            &reload_none(),
        )
        .await
        .unwrap();

        assert_eq!(outcome, Outcome::Vanished);
    }

    /// A peer that already rotated leaves a different password behind; using it
    /// must skip rotation entirely.
    #[tokio::test]
    async fn a_password_changed_by_a_peer_is_tried_before_rotating() {
        let did = unique_did("peer-rotated");

        let outcome = recover_with(
            &did,
            "https://pds.example",
            Some("what-just-failed"),
            &never_rotate(),
            &ok_login(),
            &persist_rows(1),
            &reload_password("already-rotated-by-a-peer"),
        )
        .await
        .unwrap();

        assert!(matches!(outcome, Outcome::Session(..)));
    }

    #[tokio::test]
    async fn a_second_attempt_inside_the_cooldown_is_skipped() {
        let did = unique_did("cooldown");
        let rotations = Arc::new(AtomicUsize::new(0));

        let mut outcomes = Vec::new();
        for _ in 0..2 {
            outcomes.push(
                recover_with(
                    &did,
                    "https://pds.example",
                    None,
                    &ok_rotate(rotations.clone()),
                    &ok_login(),
                    &persist_rows(1),
                    &reload_none(),
                )
                .await
                .unwrap(),
            );
            // Drop the published session, so the second pass has to reach the
            // attempt gate instead of being served straight from cache.
            community_session_cache::invalidate(&did);
        }

        assert!(matches!(outcomes[0], Outcome::Session(..)));
        assert_eq!(
            outcomes[1],
            Outcome::Skipped,
            "a second attempt inside the cooldown must not rotate again"
        );
        assert_eq!(rotations.load(Ordering::SeqCst), 1);
    }

    /// The other half of the stampede fix: once a peer has published a session,
    /// later callers take it rather than rotating or being gated.
    #[tokio::test]
    async fn a_session_published_by_a_peer_short_circuits_recovery() {
        let did = unique_did("peer-session");
        community_session_cache::put(&did, "https://pds.example", "peer-jwt");

        let outcome = recover_with(
            &did,
            "https://pds.example",
            Some("stale"),
            &never_rotate(),
            &ok_login(),
            &persist_rows(1),
            &reload_password("stale"),
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            Outcome::Session(
                String::from("https://pds.example"),
                String::from("peer-jwt")
            )
        );

        community_session_cache::invalidate(&did);
    }

    /// The stampede case that actually happens: a reconciler sweep hitting one
    /// credential-less community for every half-joined user at once.
    #[tokio::test]
    async fn concurrent_recoveries_for_one_community_rotate_once() {
        let did = unique_did("stampede");
        let rotations = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let did = did.clone();
            let rotations = rotations.clone();
            handles.push(tokio::spawn(async move {
                recover_with(
                    &did,
                    "https://pds.example",
                    None,
                    &ok_rotate(rotations),
                    &ok_login(),
                    &persist_rows(1),
                    &reload_none(),
                )
                .await
                .unwrap()
            }));
        }

        let mut sessions = 0;
        for handle in handles {
            if matches!(handle.await.unwrap(), Outcome::Session(..)) {
                sessions += 1;
            }
        }

        assert_eq!(
            rotations.load(Ordering::SeqCst),
            1,
            "8 concurrent failures must not mint 8 passwords"
        );
        // And every one of them must come away with a usable session. Without the
        // post-lock cache check the seven losers would hit the attempt gate and
        // turn into seven failed writes.
        assert_eq!(sessions, 8, "every caller should get a session");

        community_session_cache::invalidate(&did);
    }

    #[test]
    fn the_first_attempt_for_a_community_is_never_gated() {
        let did = unique_did("first-attempt");
        assert!(claim_attempt(&did), "a fresh community must be allowed");
        assert!(!claim_attempt(&did), "an immediate retry must be gated");
    }

    #[test]
    fn poisoning_extends_the_gate() {
        let did = unique_did("poisoned");
        poison(&did);
        assert!(
            !claim_attempt(&did),
            "a poisoned community must stay untouched"
        );
    }
}
