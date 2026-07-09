/// Enumerated permissions for the Colibri community role system.
///
/// Permissions are stored as strings on `social.colibri.role` records, so this
/// enum exists primarily to give the Rust code a single source of truth for
/// the namespaced permission identifiers. Variants not yet consumed by an
/// endpoint are intentional placeholders for forthcoming work; the catalog
/// stays exhaustive so the role lexicon and Rust code never drift.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    CommunityManage,
    CommunityDelete,
    CategoryCreate,
    CategoryUpdate,
    CategoryDelete,
    ChannelCreate,
    ChannelUpdate,
    ChannelDelete,
    MessageDelete,
    MemberKick,
    MemberBan,
    MemberUnban,
    RoleManage,
    InvitationCreate,
    InvitationDelete,
    ModerationViewLog,
    ApprovalManage,
    VoiceModerate,
    MentionRoles,
}

impl Permission {
    pub fn as_str(&self) -> &'static str {
        match self {
            Permission::CommunityManage => "community.manage",
            Permission::CommunityDelete => "community.delete",
            Permission::CategoryCreate => "category.create",
            Permission::CategoryUpdate => "category.update",
            Permission::CategoryDelete => "category.delete",
            Permission::ChannelCreate => "channel.create",
            Permission::ChannelUpdate => "channel.update",
            Permission::ChannelDelete => "channel.delete",
            Permission::MessageDelete => "message.hide",
            Permission::MemberKick => "member.kick",
            Permission::MemberBan => "member.ban",
            Permission::MemberUnban => "member.unban",
            Permission::RoleManage => "role.manage",
            Permission::InvitationCreate => "invitation.create",
            Permission::InvitationDelete => "invitation.delete",
            Permission::ModerationViewLog => "moderation.viewLog",
            Permission::ApprovalManage => "approval.manage",
            Permission::VoiceModerate => "voice.moderate",
            Permission::MentionRoles => "mention.roles",
        }
    }

    /// Every permission's namespaced string identifier. Protected ("owner")
    /// roles are granted this full set at read time (see `community_authz` and
    /// the `listRoles` handler), so a newly added permission automatically
    /// applies to every community's owner role without rewriting any records
    pub fn all_strings() -> Vec<String> {
        Self::all().iter().map(|p| p.as_str().to_string()).collect()
    }

    /// Every permission, in declaration order. Useful for bootstrapping the
    /// initial "owner" role on a freshly-created community.
    pub fn all() -> &'static [Permission] {
        &[
            Permission::CommunityManage,
            Permission::CommunityDelete,
            Permission::CategoryCreate,
            Permission::CategoryUpdate,
            Permission::CategoryDelete,
            Permission::ChannelCreate,
            Permission::ChannelUpdate,
            Permission::ChannelDelete,
            Permission::MessageDelete,
            Permission::MemberKick,
            Permission::MemberBan,
            Permission::MemberUnban,
            Permission::RoleManage,
            Permission::InvitationCreate,
            Permission::InvitationDelete,
            Permission::ModerationViewLog,
            Permission::ApprovalManage,
            Permission::VoiceModerate,
            Permission::MentionRoles,
        ]
    }
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_strings_are_namespaced_and_unique() {
        use std::collections::HashSet;

        let all = [
            Permission::CommunityManage,
            Permission::CommunityDelete,
            Permission::CategoryCreate,
            Permission::CategoryUpdate,
            Permission::CategoryDelete,
            Permission::ChannelCreate,
            Permission::ChannelUpdate,
            Permission::ChannelDelete,
            Permission::MessageDelete,
            Permission::MemberKick,
            Permission::MemberBan,
            Permission::MemberUnban,
            Permission::RoleManage,
            Permission::InvitationCreate,
            Permission::InvitationDelete,
            Permission::ModerationViewLog,
            Permission::ApprovalManage,
            Permission::VoiceModerate,
            Permission::MentionRoles,
        ];

        let strings: HashSet<&str> = all.iter().map(|p| p.as_str()).collect();
        assert_eq!(strings.len(), all.len());
        for s in &strings {
            assert!(s.contains('.'), "{s} should be namespaced");
        }
    }
}
