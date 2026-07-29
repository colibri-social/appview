//! In-process cache of PDS access tokens, keyed by community DID.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;

/// How far ahead of a token's real expiry we stop trusting it. Covers clock skew
/// against the PDS plus the flight time of the request we are about to make with
/// it.
const EXPIRY_SKEW: Duration = Duration::from_secs(60);

/// Lifetime assumed for a token whose `exp` we could not read. Well under the
/// PDS's actual 120 minutes, so guessing wrong costs one extra login rather than
/// a failed write, and the stale-token retry would cover it regardless.
const UNKNOWN_EXPIRY_TTL: Duration = Duration::from_secs(600);

/// Upper bound on retained entries. Real deployments hold at most thousands of
/// communities; this exists so a pathological one cannot grow the map without
/// limit, unlike `community_credentials::WARNED`.
const MAX_ENTRIES: usize = 4096;

struct CachedSession {
    pds_endpoint: String,
    access_jwt: String,
    /// When this token stops being usable, skew already subtracted.
    good_until: SystemTime,
}

static SESSIONS: LazyLock<Mutex<HashMap<String, CachedSession>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A poisoned lock here means a panic while holding it, which can only have been
/// mid-map-operation. The cache carries no invariants worth preserving across
/// that, so recover rather than propagate.
fn sessions() -> MutexGuard<'static, HashMap<String, CachedSession>> {
    SESSIONS.lock().unwrap_or_else(|e| e.into_inner())
}

/// A live `(pds_endpoint, access_jwt)` for `community_did`, or `None` when there
/// is no entry or the one there is too close to expiry to use.
pub fn get(community_did: &str) -> Option<(String, String)> {
    let now = SystemTime::now();
    let mut sessions = sessions();

    let live = sessions
        .get(community_did)
        .filter(|entry| entry.good_until > now)
        .map(|entry| (entry.pds_endpoint.clone(), entry.access_jwt.clone()));

    if live.is_none() {
        // Drop an expired entry now rather than leaving it for the next insert.
        sessions.remove(community_did);
    }
    live
}

/// Stores a freshly minted session, replacing any existing entry.
pub fn put(community_did: &str, pds_endpoint: &str, access_jwt: &str) {
    let good_until =
        token_expiry(access_jwt).unwrap_or_else(|| SystemTime::now() + UNKNOWN_EXPIRY_TTL);

    let mut sessions = sessions();
    prune(&mut sessions);
    sessions.insert(
        community_did.to_string(),
        CachedSession {
            pds_endpoint: pds_endpoint.to_string(),
            access_jwt: access_jwt.to_string(),
            good_until,
        },
    );
}

/// Forgets any cached session for `community_did`. Called whenever the stored
/// credentials change and whenever a write reports the token stale.
pub fn invalidate(community_did: &str) {
    sessions().remove(community_did);
}

/// Drops expired entries, then makes room for one more if the cap is reached by
/// evicting whatever expires soonest.
fn prune(sessions: &mut HashMap<String, CachedSession>) {
    let now = SystemTime::now();
    sessions.retain(|_, entry| entry.good_until > now);

    while sessions.len() >= MAX_ENTRIES {
        let Some(soonest) = sessions
            .iter()
            .min_by_key(|(_, entry)| entry.good_until)
            .map(|(did, _)| did.clone())
        else {
            break;
        };
        sessions.remove(&soonest);
    }
}

/// The `exp` claim of `access_jwt`, less [`EXPIRY_SKEW`].
fn token_expiry(access_jwt: &str) -> Option<SystemTime> {
    #[derive(Deserialize)]
    struct Claims {
        exp: u64,
    }

    let claims_b64 = access_jwt.split('.').nth(1)?;
    let claims_bytes = URL_SAFE_NO_PAD.decode(claims_b64).ok()?;
    let claims: Claims = serde_json::from_slice(&claims_bytes).ok()?;

    (UNIX_EPOCH + Duration::from_secs(claims.exp)).checked_sub(EXPIRY_SKEW)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A JWT whose `exp` is `secs` from now. Only the claims segment is real —
    /// nothing here verifies signatures.
    fn jwt_expiring_in(secs: u64) -> String {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + secs;
        let claims = URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{exp}}}"#));
        format!("header.{claims}.signature")
    }

    #[test]
    fn a_stored_session_can_be_read_back_repeatedly() {
        let did = "did:plc:cache-roundtrip";
        put(did, "https://pds.example", &jwt_expiring_in(7200));

        let (endpoint, jwt) = get(did).expect("a fresh session should be live");
        assert_eq!(endpoint, "https://pds.example");
        // Reading does not consume: a second write must still hit the cache.
        assert_eq!(get(did).expect("still live").1, jwt);
    }

    #[test]
    fn expiry_comes_from_the_token_itself() {
        let live = "did:plc:cache-live";
        put(live, "https://pds.example", &jwt_expiring_in(7200));
        assert!(get(live).is_some());

        // Inside the 60s skew, so unusable even though `exp` is still ahead of us.
        let nearly = "did:plc:cache-nearly-expired";
        put(nearly, "https://pds.example", &jwt_expiring_in(30));
        assert!(
            get(nearly).is_none(),
            "a token inside the skew window must not be handed out"
        );
    }

    #[test]
    fn an_unreadable_token_still_gets_cached_conservatively() {
        // No `exp` to parse, so the fallback TTL applies rather than refusing to
        // cache at all.
        let did = "did:plc:cache-opaque";
        put(did, "https://pds.example", "not-a-jwt");
        assert!(get(did).is_some());
    }

    #[test]
    fn invalidate_forgets_the_entry() {
        let did = "did:plc:cache-invalidated";
        put(did, "https://pds.example", &jwt_expiring_in(7200));
        assert!(get(did).is_some());

        invalidate(did);
        assert!(get(did).is_none());
        // Invalidating something absent is a no-op, not a panic.
        invalidate("did:plc:cache-never-stored");
        assert!(get("did:plc:cache-never-stored").is_none());
    }

    #[test]
    fn entries_are_isolated_per_community() {
        let (a, b) = ("did:plc:cache-iso-a", "did:plc:cache-iso-b");
        put(a, "https://a.example", &jwt_expiring_in(7200));
        put(b, "https://b.example", &jwt_expiring_in(7200));

        invalidate(a);
        assert!(get(a).is_none());
        assert_eq!(get(b).expect("b is untouched").0, "https://b.example");
    }

    #[test]
    fn prune_drops_expired_entries() {
        let mut map = HashMap::new();
        map.insert(
            String::from("did:plc:stale"),
            CachedSession {
                pds_endpoint: String::from("https://pds.example"),
                access_jwt: String::from("jwt"),
                good_until: SystemTime::now() - Duration::from_secs(1),
            },
        );
        map.insert(
            String::from("did:plc:fresh"),
            CachedSession {
                pds_endpoint: String::from("https://pds.example"),
                access_jwt: String::from("jwt"),
                good_until: SystemTime::now() + Duration::from_secs(3600),
            },
        );

        prune(&mut map);

        assert!(!map.contains_key("did:plc:stale"));
        assert!(map.contains_key("did:plc:fresh"));
    }

    #[test]
    fn prune_evicts_the_soonest_to_expire_at_capacity() {
        let now = SystemTime::now();
        let mut map = HashMap::new();
        for i in 0..MAX_ENTRIES {
            map.insert(
                format!("did:plc:bulk-{i}"),
                CachedSession {
                    pds_endpoint: String::from("https://pds.example"),
                    access_jwt: String::from("jwt"),
                    // Entry 0 expires soonest, so it is the one that must go.
                    good_until: now + Duration::from_secs(3600 + i as u64),
                },
            );
        }

        prune(&mut map);

        assert_eq!(
            map.len(),
            MAX_ENTRIES - 1,
            "room must be left for one insert"
        );
        assert!(!map.contains_key("did:plc:bulk-0"));
        assert!(map.contains_key(&format!("did:plc:bulk-{}", MAX_ENTRIES - 1)));
    }
}
