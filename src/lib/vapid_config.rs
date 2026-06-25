//! VAPID keypair configuration for Web Push.
//!
//! The AppView owns a single VAPID keypair (generated once, e.g. with
//! `npx web-push generate-vapid-keys`). The public key is handed to clients
//! out of band as `PUBLIC_VAPID_KEY`; the private key signs the VAPID JWT on
//! every push. Configuration is optional — when unset, push delivery is
//! disabled and `vapid_config()` returns `None`, mirroring the client, which
//! no-ops without a public key.

use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct VapidConfig {
    /// Base64url (unpadded) VAPID private key, as emitted by
    /// `web-push generate-vapid-keys`.
    pub private_key: String,
    /// Base64url (unpadded) VAPID public key. Must match the client's
    /// `PUBLIC_VAPID_KEY`. The signer derives the `k=` parameter from the
    /// private key, so this isn't read when sending; it's loaded so a
    /// half-configured keypair fails the presence check rather than silently
    /// signing with a key the client never received.
    #[allow(dead_code)]
    pub public_key: String,
    /// VAPID `sub` claim — a `mailto:` or `https:` contact for the push service.
    pub subject: String,
}

static VAPID: OnceLock<Option<VapidConfig>> = OnceLock::new();

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn load_from_env() -> Option<VapidConfig> {
    let private_key = env_non_empty("VAPID_PRIVATE_KEY")?;
    let public_key = env_non_empty("VAPID_PUBLIC_KEY")?;
    let subject =
        env_non_empty("VAPID_SUBJECT").unwrap_or_else(|| String::from("mailto:admin@colibri.social"));
    Some(VapidConfig {
        private_key,
        public_key,
        subject,
    })
}

/// Returns the configured VAPID keypair, or `None` when Web Push is not
/// configured. Memoized on first read.
pub fn vapid_config() -> Option<&'static VapidConfig> {
    VAPID.get_or_init(load_from_env).as_ref()
}

/// True when a VAPID keypair is configured (i.e. Web Push delivery is enabled).
pub fn is_configured() -> bool {
    vapid_config().is_some()
}
