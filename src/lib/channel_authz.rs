//! Whether a given actor may post messages into a channel.
//!
//! This is a different concern from [`crate::lib::permissions`] /
//! [`crate::lib::community_authz`], which gate *admin* actions and are
//! default-deny (a role must explicitly grant a permission). Posting in a
//! channel is default-*allow*: ordinary members with no roles at all can
//! post unless a channel narrows that down via `owner_only` or an explicit
//! roles/members allow-list.

use crate::lib::colibri::ColibriChannel;
use crate::lib::community_authz::ActorAuthz;

/// Whether `actor_did` may post a message into `channel`, given their
/// `authz` state in the channel's community.
///
/// Evaluation order:
///   1. The community owner may always post.
///   2. `owner_only` channels reject everyone else, regardless of allow-lists.
///   3. Empty `allowed_roles`/`allowed_members` means no restriction.
///   4. Otherwise the actor must be in `allowed_members` or hold a role
///      listed in `allowed_roles`.
pub fn can_post(channel: &ColibriChannel, authz: &ActorAuthz, actor_did: &str) -> bool {
    if authz.is_owner {
        return true;
    }
    if channel.owner_only == Some(true) {
        return false;
    }
    if channel.allowed_roles.is_empty() && channel.allowed_members.is_empty() {
        return true;
    }
    if channel.allowed_members.iter().any(|d| d == actor_did) {
        return true;
    }
    authz.member.as_ref().is_some_and(|m| {
        m.roles
            .iter()
            .any(|r| channel.allowed_roles.iter().any(|a| a == r))
    })
}

/// Entries present in exactly one of `old`/`new` (a symmetric difference) —
/// i.e. the entries an editor is effectively adding or removing. Used to
/// scope hierarchy checks to only the entries actually being changed: an
/// editor isn't responsible for entries that were already present and stay
/// untouched.
pub fn touched_entries(old: &[String], new: &[String]) -> Vec<String> {
    old.iter()
        .filter(|e| !new.contains(e))
        .chain(new.iter().filter(|e| !old.contains(e)))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::{empty_authz, member_authz, owner_authz};

    fn channel() -> ColibriChannel {
        ColibriChannel {
            r#type: String::from("social.colibri.channel"),
            name: String::from("general"),
            description: None,
            channel_type: String::from("social.colibri.channel.text"),
            category: String::from("cat1"),
            community: String::from("self"),
            owner_only: None,
            allowed_roles: vec![],
            allowed_members: vec![],
        }
    }

    #[test]
    fn owner_can_always_post() {
        let chan = channel();
        assert!(can_post(&chan, &owner_authz(), "did:plc:owner"));
    }

    #[test]
    fn owner_only_blocks_non_owner() {
        let mut chan = channel();
        chan.owner_only = Some(true);
        assert!(!can_post(&chan, &empty_authz(), "did:plc:alice"));
        assert!(!can_post(
            &chan,
            &member_authz("did:plc:alice", vec![]),
            "did:plc:alice"
        ));
    }

    #[test]
    fn unrestricted_channel_allows_everyone() {
        let chan = channel();
        assert!(can_post(&chan, &empty_authz(), "did:plc:alice"));
    }

    #[test]
    fn allowed_member_can_post_without_a_role() {
        let mut chan = channel();
        chan.allowed_members.push(String::from("did:plc:alice"));
        assert!(can_post(&chan, &empty_authz(), "did:plc:alice"));
        assert!(!can_post(&chan, &empty_authz(), "did:plc:bob"));
    }

    #[test]
    fn allowed_role_grants_access_to_holders_only() {
        let mut chan = channel();
        chan.allowed_roles.push(String::from("role-0"));

        let authz = member_authz(
            "did:plc:alice",
            vec![crate::lib::test_fixtures::role(
                "Trusted",
                10,
                vec![],
            )],
        );
        assert!(can_post(&chan, &authz, "did:plc:alice"));

        assert!(!can_post(&chan, &empty_authz(), "did:plc:bob"));
    }

    #[test]
    fn restricted_channel_denies_non_matching_member() {
        let mut chan = channel();
        chan.allowed_roles.push(String::from("role-x"));
        chan.allowed_members.push(String::from("did:plc:alice"));

        assert!(!can_post(&chan, &empty_authz(), "did:plc:bob"));
    }

    #[test]
    fn touched_entries_finds_additions_and_removals() {
        let old = vec![String::from("a"), String::from("b")];
        let new = vec![String::from("b"), String::from("c")];
        let mut touched = touched_entries(&old, &new);
        touched.sort();
        assert_eq!(touched, vec![String::from("a"), String::from("c")]);
    }

    #[test]
    fn touched_entries_empty_when_unchanged() {
        let old = vec![String::from("a"), String::from("b")];
        let new = old.clone();
        assert!(touched_entries(&old, &new).is_empty());
    }
}
