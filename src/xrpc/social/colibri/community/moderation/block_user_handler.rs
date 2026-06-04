use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::colibri::ColibriModerationSubject;
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation::{
    self, ACTION_BAN, ACTION_KICK, ACTION_UNBAN, RevokeMemberFn, WriteRecordFn,
    revoke_member_boxed, write_moderation_boxed,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::xrpc::com::atproto::identity::resolve_identity;

#[derive(Serialize, Debug)]
pub struct BlockUserResponse {
    pub did: String,
    pub handle: String,
}

/// Helper that resolves an identifier (DID or handle) to a (did, handle)
/// pair using the existing identity-resolution endpoint.
pub async fn resolve_did_and_handle(identifier: &str) -> Result<(String, String), ErrorResponse> {
    let document = resolve_identity(identifier).await?;
    let handle = document
        .also_known_as
        .as_ref()
        .and_then(|aka| aka.first())
        .map(|h| h.strip_prefix("at://").unwrap_or(h).to_string())
        .unwrap_or_else(|| document.id.clone());
    Ok((document.id.clone(), handle))
}

type ResolveFn =
    dyn Fn(String) -> BoxFuture<'static, Result<(String, String), ErrorResponse>> + Send + Sync;

/// Common moderation handler entry point used by both blockUser and
/// unblockUser. `action` toggles between writing a `ban` or `unban` record.
///
/// The shared authz prelude (parse URI → verify auth → load authz → check
/// permission) goes through [`with_community_authz`]. The hierarchy guard
/// for `ban`/`kick` then runs inside the body once the caller's own authz
/// is in hand.
#[allow(clippy::too_many_arguments)]
async fn moderate_user_with(
    action: &'static str,
    community_uri: String,
    identifier: String,
    auth: String,
    db: DatabaseConnection,
    lxm: &'static str,
    permission: Permission,
    verify_auth_fn: &VerifyAuthFn,
    resolve_fn: &ResolveFn,
    load_authz_fn: &LoadAuthzFn,
    write_record_fn: &WriteRecordFn,
    revoke_member_fn: &RevokeMemberFn,
) -> Result<Json<BlockUserResponse>, ErrorResponse> {
    let (target_did, target_handle) = resolve_fn(identifier).await?;
    let target_did_for_body = target_did.clone();

    with_community_authz(
        auth,
        lxm,
        community_uri,
        Some(permission),
        db,
        verify_auth_fn,
        load_authz_fn,
        move |ctx, db| async move {
            // Hierarchy check for any action that targets a present member
            // (ban, kick). Unban is exempt because the target is, by
            // definition, not currently in the community.
            if action == ACTION_BAN || action == ACTION_KICK {
                let target_authz = load_authz_fn(
                    db.clone(),
                    ctx.community_uri.clone(),
                    target_did_for_body.clone(),
                )
                .await?;
                if !ctx.authz.outranks(&target_authz) {
                    return Err(ErrorResponse {
                        body: Json(ErrorBody {
                            error: String::from("Forbidden"),
                            message: String::from(
                                "Cannot act on a member with an equal or higher role position.",
                            ),
                        }),
                    });
                }
            }

            let community_did_for_revoke = ctx.community.authority.clone();
            moderation::issue_action(
                write_record_fn,
                db.clone(),
                ctx.community,
                action,
                ColibriModerationSubject {
                    did: Some(target_did_for_body.clone()),
                    uri: None,
                },
                ctx.caller_did,
                None,
            )
            .await?;

            // Best-effort revoke of the community-side `social.colibri.member`
            // record. The moderation event is the source of truth for ban
            // state; if the PDS write fails the record stays put but the
            // user is already filtered out at read time.
            if (action == ACTION_BAN || action == ACTION_KICK)
                && let Err(e) =
                    revoke_member_fn(db, community_did_for_revoke, target_did_for_body.clone())
                        .await
            {
                log::warn!(
                    "member-record revoke failed for {target_did_for_body} after {action}: {e}"
                );
            }

            Ok(Json(BlockUserResponse {
                did: target_did,
                handle: target_handle,
            }))
        },
    )
    .await
}

fn resolve_boxed(
    identifier: String,
) -> BoxFuture<'static, Result<(String, String), ErrorResponse>> {
    Box::pin(async move { resolve_did_and_handle(&identifier).await })
}

#[post("/xrpc/social.colibri.community.blockUser?<community>&<identifier>&<auth>")]
/// Bans a user from a community by writing a `ban` moderation record.
pub async fn block_user(
    community: &str,
    identifier: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BlockUserResponse>, ErrorResponse> {
    moderate_user_with(
        ACTION_BAN,
        community.to_string(),
        identifier.to_string(),
        auth.to_string(),
        db.inner().clone(),
        "social.colibri.community.blockUser",
        Permission::MemberBan,
        &verify_auth_boxed,
        &resolve_boxed,
        &load_authz_boxed,
        &write_moderation_boxed,
        &revoke_member_boxed,
    )
    .await
}

#[post("/xrpc/social.colibri.community.unblockUser?<community>&<identifier>&<auth>")]
/// Unbans a user from a community by writing an `unban` moderation record.
pub async fn unblock_user(
    community: &str,
    identifier: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BlockUserResponse>, ErrorResponse> {
    moderate_user_with(
        ACTION_UNBAN,
        community.to_string(),
        identifier.to_string(),
        auth.to_string(),
        db.inner().clone(),
        "social.colibri.community.unblockUser",
        Permission::MemberUnban,
        &verify_auth_boxed,
        &resolve_boxed,
        &load_authz_boxed,
        &write_moderation_boxed,
        &revoke_member_boxed,
    )
    .await
}

#[post("/xrpc/social.colibri.community.kickUser?<community>&<identifier>&<auth>")]
/// Kicks a user from a community by writing a `kick` moderation record.
///
/// Unlike `ban`, the kicked user is not prevented from rejoining; the action
/// just revokes their current membership. The same hierarchy guard as `ban`
/// applies — callers can only kick members ranked strictly below them.
pub async fn kick_user(
    community: &str,
    identifier: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BlockUserResponse>, ErrorResponse> {
    moderate_user_with(
        ACTION_KICK,
        community.to_string(),
        identifier.to_string(),
        auth.to_string(),
        db.inner().clone(),
        "social.colibri.community.kickUser",
        Permission::MemberKick,
        &verify_auth_boxed,
        &resolve_boxed,
        &load_authz_boxed,
        &write_moderation_boxed,
        &revoke_member_boxed,
    )
    .await
}

#[post("/xrpc/social.colibri.community.kick?<community>&<member>&<auth>")]
/// Kicks a member by DID directly. Identical semantics to `kickUser` but
/// accepts a DID instead of an identifier, avoiding a handle-resolution
/// round-trip when the caller already holds the member's DID.
pub async fn kick(
    community: &str,
    member: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<BlockUserResponse>, ErrorResponse> {
    moderate_user_with(
        ACTION_KICK,
        community.to_string(),
        member.to_string(),
        auth.to_string(),
        db.inner().clone(),
        "social.colibri.community.kick",
        Permission::MemberKick,
        &verify_auth_boxed,
        &resolve_boxed,
        &load_authz_boxed,
        &write_moderation_boxed,
        &revoke_member_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::at_uri::AtUri;
    use crate::lib::colibri::ColibriModeration;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::{member, mock_db, role};
    use crate::models::record_data;
    use rocket::tokio;
    use sea_orm::DbErr;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn block_user_writes_ban_record_when_owner() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<ColibriModeration>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();
        let revoke_captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let revoke_clone = revoke_captured.clone();

        let write_record = move |_: DatabaseConnection,
                                 _: AtUri,
                                 record: ColibriModeration|
              -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some(record);
                Ok(record_data::Model {
                    id: 1,
                    did: String::from("did:plc:owner"),
                    nsid: String::from("social.colibri.moderation"),
                    rkey: String::from("mod-1"),
                    data: serde_json::json!({}),
                    indexed_at: String::from(""),
                })
            })
        };
        let revoke_fn = move |_: DatabaseConnection,
                              community_did: String,
                              subject_did: String|
              -> BoxFuture<'static, Result<bool, DbErr>> {
            let revoke = revoke_clone.clone();
            Box::pin(async move {
                *revoke.lock().unwrap() = Some((community_did, subject_did));
                Ok(true)
            })
        };
        let result = moderate_user_with(
            ACTION_BAN,
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:target"),
            String::from("token"),
            db,
            "social.colibri.community.blockUser",
            Permission::MemberBan,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_| {
                Box::pin(async {
                    Ok((String::from("did:plc:target"), String::from("target.test")))
                })
            },
            &|_, _, did| {
                Box::pin(async move {
                    Ok(ActorAuthz {
                        is_owner: did == "did:plc:owner",
                        member: None,
                        roles: vec![],
                    })
                })
            },
            &write_record,
            &revoke_fn,
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:target");
        assert_eq!(result.handle, "target.test");
        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(written.action, "ban");
        assert_eq!(written.subject.did.as_deref(), Some("did:plc:target"));
        assert_eq!(written.created_by, "did:plc:owner");

        let (revoke_community, revoke_subject) = revoke_captured.lock().unwrap().take().unwrap();
        assert_eq!(revoke_community, "did:plc:owner");
        assert_eq!(revoke_subject, "did:plc:target");
    }

    #[tokio::test]
    async fn block_user_rejects_when_caller_lacks_permission() {
        let db = mock_db();
        let result = moderate_user_with(
            ACTION_BAN,
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:target"),
            String::from("token"),
            db,
            "social.colibri.community.blockUser",
            Permission::MemberBan,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_| {
                Box::pin(async {
                    Ok((String::from("did:plc:target"), String::from("target.test")))
                })
            },
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not write when permission missing") }),
            &|_, _, _| Box::pin(async { panic!("should not revoke when permission missing") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn block_user_rejects_when_hierarchy_blocks() {
        let db = mock_db();
        let result = moderate_user_with(
            ACTION_BAN,
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:target"),
            String::from("token"),
            db,
            "social.colibri.community.blockUser",
            Permission::MemberBan,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_| {
                Box::pin(async {
                    Ok((String::from("did:plc:target"), String::from("target.test")))
                })
            },
            &|_, _, did| {
                Box::pin(async move {
                    if did == "did:plc:mod" {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:mod", vec!["r1"])),
                            roles: vec![role("Moderator", 10, vec![Permission::MemberBan])],
                        })
                    } else {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:target", vec!["r2"])),
                            roles: vec![role("Target", 20, vec![])],
                        })
                    }
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not write when hierarchy blocks") }),
            &|_, _, _| Box::pin(async { panic!("should not revoke when hierarchy blocks") }),
        )
        .await;

        assert!(result.is_err());
        let err = result.err().unwrap().body.into_inner();
        assert_eq!(err.error, "Forbidden");
        assert!(err.message.contains("role position"));
    }

    #[tokio::test]
    async fn kick_user_writes_kick_record_and_enforces_hierarchy() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<ColibriModeration>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let write_record = move |_: DatabaseConnection,
                                 _: AtUri,
                                 record: ColibriModeration|
              -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some(record);
                Ok(record_data::Model {
                    id: 1,
                    did: String::from("did:plc:owner"),
                    nsid: String::from("social.colibri.moderation"),
                    rkey: String::from("mod-1"),
                    data: serde_json::json!({}),
                    indexed_at: String::from(""),
                })
            })
        };

        let result = moderate_user_with(
            ACTION_KICK,
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:target"),
            String::from("token"),
            db,
            "social.colibri.community.kickUser",
            Permission::MemberKick,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_| {
                Box::pin(async {
                    Ok((String::from("did:plc:target"), String::from("target.test")))
                })
            },
            &|_, _, did| {
                Box::pin(async move {
                    if did == "did:plc:mod" {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:mod", vec!["r1"])),
                            roles: vec![role("Moderator", 50, vec![Permission::MemberKick])],
                        })
                    } else {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:target", vec!["r2"])),
                            roles: vec![role("Member", 10, vec![])],
                        })
                    }
                })
            },
            &write_record,
            &|_, _, _| Box::pin(async { Ok(true) }),
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:target");
        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(written.action, "kick");
        assert_eq!(written.subject.did.as_deref(), Some("did:plc:target"));
        assert_eq!(written.created_by, "did:plc:mod");
    }

    #[tokio::test]
    async fn kick_user_rejects_when_target_outranks_caller() {
        let db = mock_db();
        let result = moderate_user_with(
            ACTION_KICK,
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:target"),
            String::from("token"),
            db,
            "social.colibri.community.kickUser",
            Permission::MemberKick,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_| {
                Box::pin(async {
                    Ok((String::from("did:plc:target"), String::from("target.test")))
                })
            },
            &|_, _, did| {
                Box::pin(async move {
                    if did == "did:plc:mod" {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:mod", vec!["r1"])),
                            roles: vec![role("Moderator", 10, vec![Permission::MemberKick])],
                        })
                    } else {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:target", vec!["r2"])),
                            roles: vec![role("Admin", 50, vec![])],
                        })
                    }
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not write when hierarchy blocks") }),
            &|_, _, _| Box::pin(async { panic!("should not revoke when hierarchy blocks") }),
        )
        .await;

        assert!(result.is_err());
        let err = result.err().unwrap().body.into_inner();
        assert_eq!(err.error, "Forbidden");
        assert!(err.message.contains("role position"));
    }

    #[tokio::test]
    async fn unblock_user_skips_hierarchy_check() {
        let db = mock_db();
        let result = moderate_user_with(
            ACTION_UNBAN,
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:target"),
            String::from("token"),
            db,
            "social.colibri.community.unblockUser",
            Permission::MemberUnban,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_| {
                Box::pin(async {
                    Ok((String::from("did:plc:target"), String::from("target.test")))
                })
            },
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: Some(member("did:plc:mod", vec!["r1"])),
                        roles: vec![role("Moderator", 5, vec![Permission::MemberUnban])],
                    })
                })
            },
            &|_, _, _| {
                Box::pin(async {
                    Ok(record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.moderation"),
                        rkey: String::from("mod-1"),
                        data: serde_json::json!({}),
                        indexed_at: String::from(""),
                    })
                })
            },
            &|_, _, _| Box::pin(async { panic!("unblock should not revoke member record") }),
        )
        .await;

        assert!(result.is_ok());
    }
}
