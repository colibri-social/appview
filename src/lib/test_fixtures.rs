//! Shared fixtures for handler / authz tests.
//!
//! Cuts the same `MockDatabase::new(...).into_connection()`,
//! `ActorAuthz { is_owner: true, ... }`, and small role/member builders
//! that were repeated across a dozen test modules into one place.
//!
//! Gated under `cfg(test)` — never compiled into the release binary.

#![allow(dead_code)]

use std::sync::{Mutex, MutexGuard};

use futures::future::BoxFuture;
use sea_orm::{DatabaseBackend, DatabaseConnection, DbErr, MockDatabase};

use crate::lib::colibri::{ColibriMember, ColibriRole, ColibriRoleChannelOverride};
use crate::lib::community_authz::ActorAuthz;
use crate::lib::community_hub::HubRouting;
use crate::lib::handler::LoadAuthzFn;
use crate::lib::peer_client::{PeerError, PeerReply, RelayCall};
use crate::lib::permissions::Permission;
use crate::lib::relay::{DeclaredAppViewFn, RelayContext, RoutingFn, VerifyDelegatedFn, WriteDeps};
use crate::lib::service_auth::{ServiceAuthError, VerifiedCaller};

// ---- Common identifiers --------------------------------------------------

pub const COMMUNITY_OWNER_DID: &str = "did:plc:owner";
pub const COMMUNITY_URI: &str = "at://did:plc:owner/social.colibri.community/c1";

// ---- PDS environment -----------------------------------------------------

/// `PDS_LOC` and `PDS_ADMIN_PASS` are process-global, but cargo runs tests on
/// parallel threads. One lock for every test in every module that touches them —
/// a per-module lock would still let two modules fight over the same variables.
static PDS_ENV_LOCK: Mutex<()> = Mutex::new(());

/// Holds [`PDS_ENV_LOCK`] and sets the PDS environment for the duration of a
/// test, clearing it again on drop — including when the test panics, which a
/// trailing `remove_var` would skip.
pub struct PdsEnvGuard(#[allow(dead_code)] MutexGuard<'static, ()>);

impl PdsEnvGuard {
    /// Points `PDS_LOC` at `pds_loc` and leaves `PDS_ADMIN_PASS` unset, so
    /// credential recovery is ineligible for want of an admin password.
    pub fn without_admin_password(pds_loc: &str) -> Self {
        let guard = PDS_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: `PDS_ENV_LOCK` is held, so no other test reads or writes these
        // variables until this guard is dropped.
        unsafe {
            std::env::set_var("PDS_LOC", pds_loc);
            std::env::remove_var("PDS_ADMIN_PASS");
        }
        Self(guard)
    }

    /// Points `PDS_LOC` at `pds_loc` and supplies an admin password, the
    /// configuration in which credential recovery is possible.
    pub fn with_admin_password(pds_loc: &str, admin_password: &str) -> Self {
        let guard = Self::without_admin_password(pds_loc);
        // SAFETY: as above — the guard returned by `without_admin_password` holds
        // the lock.
        unsafe { std::env::set_var("PDS_ADMIN_PASS", admin_password) };
        guard
    }

    /// Clears both variables, for tests asserting behaviour with no PDS
    /// configured at all.
    pub fn unset() -> Self {
        let guard = PDS_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: the lock is held for the guard's lifetime.
        unsafe {
            std::env::remove_var("PDS_LOC");
            std::env::remove_var("PDS_ADMIN_PASS");
        }
        Self(guard)
    }
}

impl Drop for PdsEnvGuard {
    fn drop(&mut self) {
        // SAFETY: the lock is still held until this guard finishes dropping.
        unsafe {
            std::env::remove_var("PDS_LOC");
            std::env::remove_var("PDS_ADMIN_PASS");
        }
    }
}

// ---- Mock database -------------------------------------------------------

pub fn mock_db() -> DatabaseConnection {
    MockDatabase::new(DatabaseBackend::Postgres).into_connection()
}

// ---- ActorAuthz states ---------------------------------------------------

/// `ActorAuthz` for the community owner (everything implicitly authorized).
pub fn owner_authz() -> ActorAuthz {
    ActorAuthz {
        is_owner: true,
        member: None,
        roles: vec![],
    }
}

/// `ActorAuthz` for a caller who is neither the owner nor a member — i.e.
/// they hold no permissions in the community.
pub fn empty_authz() -> ActorAuthz {
    ActorAuthz {
        is_owner: false,
        member: None,
        roles: vec![],
    }
}

/// `ActorAuthz` for a non-owner member holding the supplied roles.
pub fn member_authz(subject_did: &str, roles: Vec<ColibriRole>) -> ActorAuthz {
    let role_rkeys: Vec<String> = (0..roles.len()).map(|i| format!("role-{i}")).collect();
    ActorAuthz {
        is_owner: false,
        member: Some(member(
            subject_did,
            role_rkeys.iter().map(String::as_str).collect(),
        )),
        roles,
    }
}

// ---- Role / Member builders ---------------------------------------------

/// Convenience constructor for a `ColibriRole`. Permissions are converted to
/// their namespaced string identifiers via [`Permission::as_str`].
pub fn role(name: &str, position: i64, permissions: Vec<Permission>) -> ColibriRole {
    ColibriRole {
        record_type: None,
        name: name.to_string(),
        color: None,
        permissions: permissions
            .into_iter()
            .map(|p| p.as_str().to_string())
            .collect(),
        position,
        hoisted: None,
        mentionable: None,
        protected: None,
        channel_overrides: vec![],
    }
}

/// Same as [`role`] but with one channel-scoped allow/deny override added.
pub fn role_with_override(
    name: &str,
    position: i64,
    permissions: Vec<Permission>,
    channel: &str,
    allow: Vec<Permission>,
    deny: Vec<Permission>,
) -> ColibriRole {
    let mut r = role(name, position, permissions);
    r.channel_overrides.push(ColibriRoleChannelOverride {
        channel: channel.to_string(),
        allow: allow.into_iter().map(|p| p.as_str().to_string()).collect(),
        deny: deny.into_iter().map(|p| p.as_str().to_string()).collect(),
    });
    r
}

/// Convenience constructor for a `ColibriMember`. `roles` is a list of
/// role rkeys (strings); use string literals at the call site.
pub fn member(subject: &str, roles: Vec<&str>) -> ColibriMember {
    ColibriMember {
        record_type: None,
        subject: subject.to_string(),
        roles: roles.into_iter().map(String::from).collect(),
        joined_at: String::from("2026-05-13T00:00:00Z"),
        nickname: None,
        from_membership: None,
    }
}

pub fn relay_ctx() -> RelayContext {
    RelayContext {
        method: String::from("POST"),
        path_and_query: String::from("/xrpc/test"),
    }
}

fn routing_local(
    _: DatabaseConnection,
    _: String,
) -> BoxFuture<'static, Result<HubRouting, DbErr>> {
    Box::pin(async { Ok(HubRouting::Local) })
}

fn no_declaration(
    _: DatabaseConnection,
    _: String,
) -> BoxFuture<'static, Result<Option<String>, DbErr>> {
    Box::pin(async { Ok(None) })
}

fn never_relays(_: RelayCall) -> BoxFuture<'static, Result<PeerReply, PeerError>> {
    Box::pin(async { panic!("a community we administer must not be relayed") })
}

fn unset_authz(
    _: DatabaseConnection,
    _: String,
    _: String,
) -> BoxFuture<'static, Result<ActorAuthz, DbErr>> {
    Box::pin(async { panic!("this test's own authz seam should have replaced this one") })
}

pub fn local_write_deps(caller: &'static str) -> WriteDeps<'static> {
    let verify: &'static VerifyDelegatedFn = Box::leak(Box::new(
        move |_: String,
              _: String|
              -> BoxFuture<'static, Result<VerifiedCaller, ServiceAuthError>> {
            Box::pin(async move {
                Ok(VerifiedCaller {
                    iss: String::from(caller),
                    act: None,
                })
            })
        },
    ));

    WriteDeps {
        verify_delegated_fn: verify,
        load_authz_fn: &unset_authz,
        routing_fn: &routing_local as &'static RoutingFn,
        declared_appview_fn: &no_declaration as &'static DeclaredAppViewFn,
        forward_fn: &never_relays,
    }
}

pub fn write_deps_with_authz<'a>(
    caller: &'static str,
    load_authz_fn: &'a LoadAuthzFn,
) -> WriteDeps<'a> {
    WriteDeps {
        load_authz_fn,
        ..local_write_deps(caller)
    }
}

fn never_authenticates(
    _: String,
    _: String,
) -> BoxFuture<'static, Result<VerifiedCaller, ServiceAuthError>> {
    Box::pin(async { panic!("this request should have been refused before authenticating") })
}

pub fn write_deps_never_authenticating() -> WriteDeps<'static> {
    WriteDeps {
        verify_delegated_fn: &never_authenticates,
        ..local_write_deps("did:plc:unused")
    }
}
