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
    CommunityUpdate,
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
}

impl Permission {
    pub fn as_str(&self) -> &'static str {
        match self {
            Permission::CommunityUpdate => "community.update",
            Permission::CommunityDelete => "community.delete",
            Permission::CategoryCreate => "category.create",
            Permission::CategoryUpdate => "category.update",
            Permission::CategoryDelete => "category.delete",
            Permission::ChannelCreate => "channel.create",
            Permission::ChannelUpdate => "channel.update",
            Permission::ChannelDelete => "channel.delete",
            Permission::MessageDelete => "message.delete",
            Permission::MemberKick => "member.kick",
            Permission::MemberBan => "member.ban",
            Permission::MemberUnban => "member.unban",
            Permission::RoleManage => "role.manage",
            Permission::InvitationCreate => "invitation.create",
            Permission::InvitationDelete => "invitation.delete",
            Permission::ModerationViewLog => "moderation.viewLog",
            Permission::ApprovalManage => "approval.manage",
        }
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
            Permission::CommunityUpdate,
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
        ];

        let strings: HashSet<&str> = all.iter().map(|p| p.as_str()).collect();
        assert_eq!(strings.len(), all.len());
        for s in &strings {
            assert!(s.contains('.'), "{s} should be namespaced");
        }
    }
}
