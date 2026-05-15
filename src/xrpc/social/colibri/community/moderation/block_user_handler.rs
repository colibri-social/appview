use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriModeration, ColibriModerationSubject};
use crate::lib::did_document::DidDocument;
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation::{self, ACTION_BAN, ACTION_KICK, ACTION_UNBAN};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::time::current_iso8601_utc;
use crate::models::record_data;
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
type WriteRecordFn = dyn Fn(
        DatabaseConnection,
        AtUri,
        ColibriModeration,
    ) -> BoxFuture<'static, Result<record_data::Model, DbErr>>
    + Send
    + Sync;

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

            let record = moderation::moderation_record(
                action,
                ColibriModerationSubject {
                    did: Some(target_did_for_body.clone()),
                    uri: None,
                },
                ctx.caller_did,
                current_iso8601_utc(),
                None,
            );
            write_record_fn(db, ctx.community, record).await?;

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

fn write_moderation_boxed(
    db: DatabaseConnection,
    community: AtUri,
    record: ColibriModeration,
) -> BoxFuture<'static, Result<record_data::Model, DbErr>> {
    Box::pin(async move { moderation::write_moderation_record(&db, &community, &record).await })
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
    )
    .await
}

// Silence: import is used inside async closures via traits.
#[allow(dead_code)]
fn _force_did_document_import(d: DidDocument) -> DidDocument {
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::colibri::{ColibriMember, ColibriRole};
    use crate::lib::community_authz::ActorAuthz;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

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

    fn role(position: i64, permissions: Vec<Permission>) -> ColibriRole {
        ColibriRole {
            record_type: None,
            name: String::from("R"),
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

    #[tokio::test]
    async fn block_user_writes_ban_record_when_owner() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
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
                })
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
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:target");
        assert_eq!(result.handle, "target.test");
        let written = captured.lock().unwrap().take().unwrap();
        assert_eq!(written.action, "ban");
        assert_eq!(written.subject.did.as_deref(), Some("did:plc:target"));
        assert_eq!(written.created_by, "did:plc:owner");
    }

    #[tokio::test]
    async fn block_user_rejects_when_caller_lacks_permission() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
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
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn block_user_rejects_when_hierarchy_blocks() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
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
                            roles: vec![role(10, vec![Permission::MemberBan])],
                        })
                    } else {
                        Ok(ActorAuthz {
                            is_owner: false,
                            member: Some(member("did:plc:target", vec!["r2"])),
                            roles: vec![role(20, vec![])],
                        })
                    }
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not write when hierarchy blocks") }),
        )
        .await;

        assert!(result.is_err());
        let err = result.err().unwrap().body.into_inner();
        assert_eq!(err.error, "Forbidden");
        assert!(err.message.contains("role position"));
    }

    #[tokio::test]
    async fn unblock_user_skips_hierarchy_check() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
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
                        roles: vec![role(5, vec![Permission::MemberUnban])],
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
                    })
                })
            },
        )
        .await;

        assert!(result.is_ok());
    }
}
