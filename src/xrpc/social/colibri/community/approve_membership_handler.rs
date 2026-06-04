//! `social.colibri.community.approveMembership` — moderator endpoint that
//! advances a pending join for a closed community by writing the
//! `social.colibri.member` record on the community's PDS.
//!
//! Open communities auto-admit via the firehose path in `lib::tap`
//! (`process_membership_create`); this endpoint is the manual counterpart for
//! `requiresApprovalToJoin: true` communities. It's idempotent — if a member
//! record already exists for the subject, the handler returns success without
//! writing.
//!
//! No `denyMembership` counterpart exists today. Pending applications that a
//! moderator doesn't approve simply sit on the applicant's repo forever — the
//! AppView has no write access to the applicant's PDS, so it can't delete
//! the intent record, and we deliberately don't write an on-protocol
//! rejection record either. If a "reviewed and declined" signal is needed
//! later, the natural shape is an audit-only moderation entry (similar to
//! `ACTION_BLOCKED_JOIN`) that leaves the applicant's membership record
//! untouched.

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriMembership;
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation;
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

#[derive(Serialize, Debug)]
pub struct ApproveMembershipResponse {
    /// DID of the user who was admitted.
    pub did: String,
    /// AT-URI of the community.
    pub community: String,
    /// AT-URI of the freshly-minted `social.colibri.member` record. `None`
    /// when the subject was already a member (idempotent skip).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
}

/// Trait-object seam for fetching a `social.colibri.membership` record from
/// the local cache. Returns the parsed payload plus the user DID (the repo
/// authority of the membership AT-URI).
pub type FetchMembershipFn = dyn Fn(
        DatabaseConnection,
        AtUri,
    ) -> BoxFuture<'static, Result<Option<(String, ColibriMembership)>, DbErr>>
    + Send
    + Sync;

/// Trait-object seam for writing a `social.colibri.member` record on a
/// community's PDS. Mirrors [`moderation::write_member_record`].
pub type WriteMemberFn = dyn Fn(
        DatabaseConnection,
        String,
        String,
        Vec<String>,
        Option<String>,
    ) -> BoxFuture<'static, Result<Option<record_data::Model>, DbErr>>
    + Send
    + Sync;

/// Trait-object seam for the ban check. Wraps [`moderation::is_user_banned`]
/// in production; tests can short-circuit without seeding the mock DB.
pub type IsBannedFn = dyn Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<bool, DbErr>>
    + Send
    + Sync;

pub fn fetch_membership_boxed(
    db: DatabaseConnection,
    membership_uri: AtUri,
) -> BoxFuture<'static, Result<Option<(String, ColibriMembership)>, DbErr>> {
    Box::pin(async move {
        let row = record_data::Entity::find()
            .filter(record_data::Column::Did.eq(&membership_uri.authority))
            .filter(record_data::Column::Nsid.eq(&membership_uri.collection))
            .filter(record_data::Column::Rkey.eq(&membership_uri.rkey))
            .one(&db)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let parsed = serde_json::from_value::<ColibriMembership>(row.data)
            .map_err(|e| DbErr::Custom(e.to_string()))?;
        Ok(Some((membership_uri.authority, parsed)))
    })
}

pub fn write_member_boxed(
    db: DatabaseConnection,
    community_did: String,
    subject_did: String,
    role_rkeys: Vec<String>,
    from_membership: Option<String>,
) -> BoxFuture<'static, Result<Option<record_data::Model>, DbErr>> {
    Box::pin(async move {
        moderation::write_member_record(
            &db,
            &community_did,
            &subject_did,
            role_rkeys,
            from_membership,
        )
        .await
    })
}

pub fn is_banned_boxed(
    db: DatabaseConnection,
    community_did: String,
    subject_did: String,
) -> BoxFuture<'static, Result<bool, DbErr>> {
    Box::pin(async move { moderation::is_user_banned(&db, &community_did, &subject_did).await })
}

fn invalid_request(message: &str) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: message.to_string(),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
async fn approve_membership_with(
    membership_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    fetch_membership_fn: &FetchMembershipFn,
    write_member_fn: &WriteMemberFn,
    is_banned_fn: &IsBannedFn,
) -> Result<Json<ApproveMembershipResponse>, ErrorResponse> {
    let parsed_membership_uri = AtUri::parse(&membership_uri)
        .ok_or_else(|| invalid_request("Invalid membership AT-URI."))?;
    if parsed_membership_uri.collection != "social.colibri.membership" {
        return Err(invalid_request(
            "URI does not point to a social.colibri.membership record.",
        ));
    }

    let (subject_did, membership) =
        fetch_membership_fn(db.clone(), parsed_membership_uri.clone())
            .await?
            .ok_or_else(|| invalid_request("Membership record not found in local index."))?;

    let community_uri_string = membership.community.clone();

    with_community_authz(
        auth,
        "social.colibri.community.approveMembership",
        community_uri_string,
        Some(Permission::ApprovalManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        move |ctx, db| async move {
            if is_banned_fn(db.clone(), ctx.community.authority.clone(), subject_did.clone())
                .await?
            {
                return Err(ErrorResponse {
                    body: Json(ErrorBody {
                        error: String::from("Forbidden"),
                        message: String::from(
                            "Cannot approve membership for a banned user; unban first.",
                        ),
                    }),
                });
            }

            let written = write_member_fn(
                db,
                ctx.community.authority.clone(),
                subject_did.clone(),
                vec![],
                Some(membership_uri),
            )
            .await?;

            let member_uri = written.map(|row| {
                format!(
                    "at://{}/{}/{}",
                    row.did, row.nsid, row.rkey
                )
            });

            Ok(Json(ApproveMembershipResponse {
                did: subject_did,
                community: ctx.community_uri.clone(),
                member: member_uri,
            }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.community.approveMembership?<membership>&<auth>")]
/// Admits a pending applicant to a closed community by writing the
/// community-side `social.colibri.member` record. Requires the caller to
/// hold the `approval.manage` permission. Idempotent on already-admitted
/// members.
pub async fn approve_membership(
    membership: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ApproveMembershipResponse>, ErrorResponse> {
    approve_membership_with(
        membership.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &fetch_membership_boxed,
        &write_member_boxed,
        &is_banned_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::{member, mock_db, role};
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn sample_membership() -> ColibriMembership {
        ColibriMembership {
            r#type: String::from("social.colibri.membership"),
            community: String::from("at://did:plc:community/social.colibri.community/self"),
            created_at: String::from("2026-01-01T00:00:00Z"),
        }
    }

    #[tokio::test]
    async fn approves_membership_when_caller_has_permission() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String, Option<String>)>>> =
            Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let write_member = move |_: DatabaseConnection,
                                 community_did: String,
                                 subject_did: String,
                                 _roles: Vec<String>,
                                 from_membership: Option<String>|
              -> BoxFuture<
            'static,
            Result<Option<record_data::Model>, DbErr>,
        > {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() =
                    Some((community_did.clone(), subject_did.clone(), from_membership));
                Ok(Some(record_data::Model {
                    id: 1,
                    did: community_did,
                    nsid: String::from("social.colibri.member"),
                    rkey: String::from("member-1"),
                    data: serde_json::json!({}),
                    indexed_at: String::from(""),
                }))
            })
        };

        let result = approve_membership_with(
            String::from("at://did:plc:applicant/social.colibri.membership/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: Some(member("did:plc:mod", vec!["r1"])),
                        roles: vec![role(
                            "Moderator",
                            10,
                            vec![Permission::ApprovalManage],
                        )],
                    })
                })
            },
            &|_, uri| {
                Box::pin(async move {
                    Ok(Some((uri.authority, sample_membership())))
                })
            },
            &write_member,
            &|_, _, _| Box::pin(async { Ok(false) }),
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:applicant");
        assert_eq!(
            result.community,
            "at://did:plc:community/social.colibri.community/self"
        );
        assert!(result.member.is_some());

        let (community_did, subject_did, from_membership) =
            captured.lock().unwrap().take().unwrap();
        assert_eq!(community_did, "did:plc:community");
        assert_eq!(subject_did, "did:plc:applicant");
        assert_eq!(
            from_membership.as_deref(),
            Some("at://did:plc:applicant/social.colibri.membership/m1")
        );
    }

    #[tokio::test]
    async fn rejects_when_caller_lacks_approval_permission() {
        let db = mock_db();
        let result = approve_membership_with(
            String::from("at://did:plc:applicant/social.colibri.membership/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            &|_, uri| {
                Box::pin(async move {
                    Ok(Some((uri.authority, sample_membership())))
                })
            },
            &|_, _, _, _, _| Box::pin(async { panic!("should not write member") }),
            &|_, _, _| Box::pin(async { panic!("should not check ban before permission") }),
        )
        .await;
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn rejects_banned_subject() {
        let db = mock_db();
        let result = approve_membership_with(
            String::from("at://did:plc:applicant/social.colibri.membership/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: Some(member("did:plc:mod", vec!["r1"])),
                        roles: vec![role(
                            "Moderator",
                            10,
                            vec![Permission::ApprovalManage],
                        )],
                    })
                })
            },
            &|_, uri| {
                Box::pin(async move {
                    Ok(Some((uri.authority, sample_membership())))
                })
            },
            &|_, _, _, _, _| Box::pin(async { panic!("should not write member when banned") }),
            &|_, _, _| Box::pin(async { Ok(true) }),
        )
        .await;

        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("banned"));
    }

    #[tokio::test]
    async fn rejects_when_membership_record_missing() {
        let db = mock_db();
        let result = approve_membership_with(
            String::from("at://did:plc:applicant/social.colibri.membership/m1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate before validating uri") }),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _| Box::pin(async { Ok(None) }),
            &|_, _, _, _, _| Box::pin(async { panic!("should not write member") }),
            &|_, _, _| Box::pin(async { panic!("should not check ban") }),
        )
        .await;

        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "InvalidRequest");
        assert!(body.message.contains("not found"));
    }

    #[tokio::test]
    async fn rejects_uri_with_wrong_collection() {
        let db = mock_db();
        let result = approve_membership_with(
            String::from("at://did:plc:applicant/app.bsky.feed.post/abc"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate") }),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _| Box::pin(async { panic!("should not fetch") }),
            &|_, _, _, _, _| Box::pin(async { panic!("should not write member") }),
            &|_, _, _| Box::pin(async { panic!("should not check ban") }),
        )
        .await;

        let body = result.err().unwrap().body.into_inner();
        assert_eq!(body.error, "InvalidRequest");
        assert!(body.message.contains("social.colibri.membership"));
    }
}
