use sea_orm::{ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter};

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriMember, ColibriRole};
use crate::lib::permissions::Permission;
use crate::models::record_data;

const MEMBER_NSID: &str = "social.colibri.member";
const ROLE_NSID: &str = "social.colibri.role";

/// Aggregated authz state for a single (actor, community) pair.
#[derive(Debug, Clone)]
pub struct ActorAuthz {
    pub is_owner: bool,
    /// Loaded for completeness — endpoints that need nickname / fromMembership
    /// will read it; currently unused outside tests.
    #[allow(dead_code)]
    pub member: Option<ColibriMember>,
    pub roles: Vec<ColibriRole>,
}

impl ActorAuthz {
    /// Highest role position held by the actor, or `None` if the actor has no
    /// roles. The owner is considered to outrank every role and is represented
    /// as `Some(i64::MAX)`.
    pub fn highest_position(&self) -> Option<i64> {
        if self.is_owner {
            return Some(i64::MAX);
        }
        self.roles.iter().map(|r| r.position).max()
    }

    /// Whether the actor holds the given permission, optionally in the context
    /// of a specific channel (so per-channel overrides apply).
    ///
    /// Evaluation order, matching Discord:
    ///   1. Owner short-circuits to allow.
    ///   2. Channel-override `deny` wins over any allow.
    ///   3. Channel-override `allow` grants without needing a base permission.
    ///   4. Otherwise the base `permissions` list of any role is consulted.
    pub fn has(&self, permission: Permission, channel_rkey: Option<&str>) -> bool {
        if self.is_owner {
            return true;
        }

        let needle = permission.as_str();
        let mut has_base = false;
        let mut has_override_allow = false;
        let mut has_override_deny = false;

        for role in &self.roles {
            if role.permissions.iter().any(|p| p == needle) {
                has_base = true;
            }
            if let Some(channel) = channel_rkey {
                for override_entry in role.channel_overrides.iter() {
                    if override_entry.channel != channel {
                        continue;
                    }
                    if override_entry.deny.iter().any(|p| p == needle) {
                        has_override_deny = true;
                    }
                    if override_entry.allow.iter().any(|p| p == needle) {
                        has_override_allow = true;
                    }
                }
            }
        }

        if has_override_deny {
            return false;
        }
        has_override_allow || has_base
    }

    /// Hierarchy guard for actions targeting another user. The acting party
    /// must outrank the target (strictly greater highest-role position). The
    /// owner outranks everyone.
    pub fn outranks(&self, target: &ActorAuthz) -> bool {
        if self.is_owner {
            return true;
        }
        if target.is_owner {
            return false;
        }
        match (self.highest_position(), target.highest_position()) {
            (Some(a), Some(b)) => a > b,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }
}

/// Loads the authz state for `actor_did` in the community identified by
/// `community_uri`. Returns the assembled state, doing per-table queries.
pub async fn load_actor_authz(
    db: &DatabaseConnection,
    community_uri: &str,
    actor_did: &str,
) -> Result<ActorAuthz, DbErr> {
    let community = AtUri::parse(community_uri)
        .ok_or_else(|| DbErr::Custom(format!("invalid community AT-URI: {community_uri}")))?;

    let is_owner = community.authority == actor_did;

    let member_record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(sea_orm::prelude::Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(actor_did.to_string())],
        ))
        .one(db)
        .await?;

    let member = member_record.and_then(|m| serde_json::from_value::<ColibriMember>(m.data).ok());

    let role_rkeys: Vec<String> = member.as_ref().map(|m| m.roles.clone()).unwrap_or_default();

    let roles = if role_rkeys.is_empty() {
        Vec::new()
    } else {
        let role_records = record_data::Entity::find()
            .filter(
                Condition::all()
                    .add(record_data::Column::Did.eq(&community.authority))
                    .add(record_data::Column::Nsid.eq(ROLE_NSID))
                    .add(record_data::Column::Rkey.is_in(role_rkeys.clone())),
            )
            .all(db)
            .await?;
        role_records
            .into_iter()
            .filter_map(|r| serde_json::from_value::<ColibriRole>(r.data).ok())
            .collect()
    };

    Ok(ActorAuthz {
        is_owner,
        member,
        roles,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::colibri::{ColibriMember, ColibriRole, ColibriRoleChannelOverride};

    fn role(name: &str, position: i64, permissions: Vec<Permission>) -> ColibriRole {
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
            channel_overrides: vec![],
        }
    }

    fn role_with_override(
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

    fn member(subject: &str, roles: Vec<&str>) -> ColibriMember {
        ColibriMember {
            record_type: None,
            subject: subject.to_string(),
            roles: roles.into_iter().map(String::from).collect(),
            joined_at: String::from("2026-05-13T00:00:00Z"),
            nickname: None,
            from_membership: None,
        }
    }

    #[test]
    fn owner_has_every_permission() {
        let authz = ActorAuthz {
            is_owner: true,
            member: None,
            roles: vec![],
        };
        assert!(authz.has(Permission::MemberBan, None));
        assert!(authz.has(Permission::MessageDelete, Some("chan-a")));
        assert_eq!(authz.highest_position(), Some(i64::MAX));
    }

    #[test]
    fn non_member_has_no_permissions() {
        let authz = ActorAuthz {
            is_owner: false,
            member: None,
            roles: vec![],
        };
        assert!(!authz.has(Permission::MemberBan, None));
        assert!(authz.highest_position().is_none());
    }

    #[test]
    fn role_permission_grants_action() {
        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:alice", vec!["mod"])),
            roles: vec![role("Moderator", 10, vec![Permission::MemberBan])],
        };
        assert!(authz.has(Permission::MemberBan, None));
        assert!(!authz.has(Permission::CommunityDelete, None));
    }

    #[test]
    fn channel_override_deny_beats_base_permission() {
        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:alice", vec!["mod"])),
            roles: vec![role_with_override(
                "Moderator",
                10,
                vec![Permission::MessageDelete],
                "chan-a",
                vec![],
                vec![Permission::MessageDelete],
            )],
        };
        assert!(authz.has(Permission::MessageDelete, None));
        assert!(authz.has(Permission::MessageDelete, Some("chan-b")));
        assert!(!authz.has(Permission::MessageDelete, Some("chan-a")));
    }

    #[test]
    fn channel_override_allow_grants_without_base() {
        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:alice", vec!["mod"])),
            roles: vec![role_with_override(
                "Helper",
                5,
                vec![],
                "chan-a",
                vec![Permission::MessageDelete],
                vec![],
            )],
        };
        assert!(authz.has(Permission::MessageDelete, Some("chan-a")));
        assert!(!authz.has(Permission::MessageDelete, Some("chan-b")));
        assert!(!authz.has(Permission::MessageDelete, None));
    }

    #[test]
    fn hierarchy_owner_outranks_everyone() {
        let owner = ActorAuthz {
            is_owner: true,
            member: None,
            roles: vec![],
        };
        let mod_user = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:alice", vec!["mod"])),
            roles: vec![role("Moderator", 10, vec![])],
        };
        assert!(owner.outranks(&mod_user));
        assert!(!mod_user.outranks(&owner));
    }

    #[test]
    fn hierarchy_requires_strictly_higher_position() {
        let high = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:a", vec!["r1"])),
            roles: vec![role("High", 20, vec![])],
        };
        let mid = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:b", vec!["r2"])),
            roles: vec![role("Mid", 10, vec![])],
        };
        let other_mid = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:c", vec!["r3"])),
            roles: vec![role("Other Mid", 10, vec![])],
        };
        assert!(high.outranks(&mid));
        assert!(!mid.outranks(&other_mid));
        assert!(!mid.outranks(&high));
    }
}
