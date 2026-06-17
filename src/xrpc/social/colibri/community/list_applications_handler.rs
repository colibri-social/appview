//! `social.colibri.community.listApplications` — moderator endpoint that lists
//! the pending join applications for a community that requires approval.
//!
//! It is the read-side counterpart to `approveMembership`: applicants write a
//! `social.colibri.membership` record to their own repo, the AppView indexes
//! it, and for `requiresApprovalToJoin: true` communities the application then
//! sits unactioned until a moderator approves it. This endpoint surfaces those
//! still-pending applications so a moderator UI can render them and feed each
//! one's membership AT-URI back into `approveMembership`.
//!
//! "Pending" means: an indexed `social.colibri.membership` targeting this
//! community whose author does not yet have a `social.colibri.member` record on
//! the community repo. Open communities auto-admit (see `lib::tap`), so their
//! applications resolve to members almost immediately and won't linger here.
//!
//! Requires the caller to hold the `approval.manage` permission — the same
//! permission `approveMembership` gates on, since the pending-applicant list is
//! moderator-only information.

use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{ColibriActorData, ColibriMembership};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;
use crate::models::{record_data, repos, user_states};
use crate::xrpc::social::colibri::actor::get_data_handler::{ActorData, ActorStatus};

#[derive(Serialize, Deserialize, Debug)]
pub struct Application {
    /// DID of the applicant.
    pub did: String,
    pub handle: String,
    /// AT-URI of the applicant's `social.colibri.membership` record. Feed this
    /// to `approveMembership` to admit them.
    pub membership: String,
    /// When the applicant declared their membership (the membership record's
    /// `createdAt`).
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub data: ActorData,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ApplicationList {
    pub applications: Vec<Application>,
}

/// A pending applicant: the membership record that's awaiting approval, keyed
/// back to its author.
pub struct PendingApplicant {
    pub did: String,
    pub membership_uri: String,
    pub created_at: String,
}

/// Data fetched in a single round-trip per backing table, keyed by applicant
/// DID. Mirrors the member aggregate in `list_members_handler`, but the unit of
/// iteration is a pending membership rather than an admitted member.
pub struct ApplicationAggregate {
    pub applicants: Vec<PendingApplicant>,
    pub profiles: HashMap<String, ActorProfile>,
    pub actor_data: HashMap<String, ColibriActorData>,
    pub states: HashMap<String, String>,
    pub handles: HashMap<String, String>,
}

pub async fn fetch_application_aggregate(
    db: &DatabaseConnection,
    community_authority: &str,
    community_uri: &str,
) -> Result<ApplicationAggregate, DbErr> {
    // Every `social.colibri.membership` targeting this community, regardless of
    // which applicant repo it lives on.
    let memberships = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.membership"))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(community_uri.to_string())],
        ))
        .all(db)
        .await?;

    // Subjects who already hold a `social.colibri.member` record on the
    // community repo — their applications have been actioned (approved or
    // auto-admitted) and are no longer pending.
    let member_rows = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.member"))
        .all(db)
        .await?;
    let admitted: std::collections::HashSet<String> = member_rows
        .into_iter()
        .filter_map(|m| {
            m.data
                .get("subject")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .collect();

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut applicants: Vec<PendingApplicant> = Vec::new();
    for row in memberships {
        if admitted.contains(&row.did) {
            continue;
        }
        // One application per applicant — keep the first indexed membership if a
        // repo somehow carries more than one targeting the same community.
        if !seen.insert(row.did.clone()) {
            continue;
        }
        let created_at = serde_json::from_value::<ColibriMembership>(row.data)
            .map(|m| m.created_at)
            .unwrap_or_default();
        applicants.push(PendingApplicant {
            membership_uri: format!("at://{}/social.colibri.membership/{}", row.did, row.rkey),
            did: row.did,
            created_at,
        });
    }

    if applicants.is_empty() {
        return Ok(ApplicationAggregate {
            applicants,
            profiles: HashMap::new(),
            actor_data: HashMap::new(),
            states: HashMap::new(),
            handles: HashMap::new(),
        });
    }

    let applicant_dids: Vec<String> = applicants.iter().map(|a| a.did.clone()).collect();

    let self_records = record_data::Entity::find()
        .filter(record_data::Column::Did.is_in(applicant_dids.clone()))
        .filter(record_data::Column::Rkey.eq("self"))
        .filter(
            Condition::any()
                .add(record_data::Column::Nsid.eq("app.bsky.actor.profile"))
                .add(record_data::Column::Nsid.eq("social.colibri.actor.data")),
        )
        .all(db)
        .await?;

    let mut profiles: HashMap<String, ActorProfile> = HashMap::new();
    let mut actor_data: HashMap<String, ColibriActorData> = HashMap::new();
    for record in self_records {
        if record.nsid == "app.bsky.actor.profile" {
            if let Ok(profile) = serde_json::from_value::<ActorProfile>(record.data) {
                profiles.insert(record.did, profile);
            }
        } else if record.nsid == "social.colibri.actor.data"
            && let Ok(data) = serde_json::from_value::<ColibriActorData>(record.data)
        {
            actor_data.insert(record.did, data);
        }
    }

    let state_rows = user_states::Entity::find()
        .filter(user_states::Column::Did.is_in(applicant_dids.clone()))
        .all(db)
        .await?;
    let states: HashMap<String, String> = state_rows
        .into_iter()
        .map(|row| (row.did, row.state))
        .collect();

    let repo_rows = repos::Entity::find()
        .filter(repos::Column::Did.is_in(applicant_dids.clone()))
        .all(db)
        .await?;
    let handles: HashMap<String, String> = repo_rows
        .into_iter()
        .filter_map(|row| row.handle.map(|h| (row.did, h)))
        .collect();

    Ok(ApplicationAggregate {
        applicants,
        profiles,
        actor_data,
        states,
        handles,
    })
}

/// Pure assembly from the aggregate to the wire shape, so it can be unit-tested
/// without a database.
pub fn build_applications(aggregate: ApplicationAggregate) -> Vec<Application> {
    let ApplicationAggregate {
        applicants,
        mut profiles,
        mut actor_data,
        mut states,
        mut handles,
    } = aggregate;

    applicants
        .into_iter()
        .map(|applicant| {
            let PendingApplicant {
                did,
                membership_uri,
                created_at,
            } = applicant;

            let handle = handles.remove(&did).unwrap_or_else(|| did.clone());
            let profile = profiles.remove(&did);
            let data = actor_data.remove(&did);
            let state = states.remove(&did);

            let display_name = profile
                .as_ref()
                .and_then(|p| p.display_name.clone())
                .unwrap_or_else(|| handle.clone());
            let (avatar, banner, description): (Option<Value>, Option<Value>, Option<String>) =
                match profile {
                    Some(p) => (p.avatar, p.banner, p.description),
                    None => (None, None, None),
                };
            let status = data
                .map(|d| ActorStatus {
                    text: d.status.unwrap_or(String::from("")),
                    emoji: d.emoji,
                })
                .unwrap_or(ActorStatus {
                    text: String::new(),
                    emoji: None,
                });
            let online_state = state.unwrap_or_else(|| String::from("offline"));

            Application {
                did,
                handle,
                membership: membership_uri,
                created_at,
                data: ActorData {
                    display_name,
                    avatar,
                    banner,
                    description,
                    online_state,
                    status,
                },
            }
        })
        .collect()
}

/// Trait-object seam for fetching the application aggregate. Args are
/// `(db, community_authority, community_uri)`.
pub type FetchAggregateFn = dyn Fn(
        DatabaseConnection,
        String,
        String,
    ) -> BoxFuture<'static, Result<ApplicationAggregate, DbErr>>
    + Send
    + Sync;

pub fn fetch_aggregate_boxed(
    db: DatabaseConnection,
    community_authority: String,
    community_uri: String,
) -> BoxFuture<'static, Result<ApplicationAggregate, DbErr>> {
    Box::pin(
        async move { fetch_application_aggregate(&db, &community_authority, &community_uri).await },
    )
}

async fn list_applications_with(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    fetch_aggregate_fn: &FetchAggregateFn,
) -> Result<Json<ApplicationList>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.listApplications",
        community_uri,
        Some(Permission::ApprovalManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        move |ctx, db| async move {
            let aggregate = fetch_aggregate_fn(
                db,
                ctx.community.authority.clone(),
                ctx.community_uri.clone(),
            )
            .await?;
            Ok(Json(ApplicationList {
                applications: build_applications(aggregate),
            }))
        },
    )
    .await
}

#[get("/xrpc/social.colibri.community.listApplications?<community>&<auth>")]
/// Lists the pending join applications for a community awaiting moderator
/// approval. Requires the caller to hold the `approval.manage` permission.
pub async fn list_applications(
    community: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ApplicationList>, ErrorResponse> {
    list_applications_with(
        community.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &fetch_aggregate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::{member, mock_db, role};
    use rocket::tokio;

    fn empty_aggregate(applicants: Vec<PendingApplicant>) -> ApplicationAggregate {
        ApplicationAggregate {
            applicants,
            profiles: HashMap::new(),
            actor_data: HashMap::new(),
            states: HashMap::new(),
            handles: HashMap::new(),
        }
    }

    #[test]
    fn build_applications_falls_back_to_did_when_handle_and_profile_missing() {
        let apps = build_applications(empty_aggregate(vec![PendingApplicant {
            did: String::from("did:plc:alice"),
            membership_uri: String::from("at://did:plc:alice/social.colibri.membership/m1"),
            created_at: String::from("2026-01-01T00:00:00Z"),
        }]));

        assert_eq!(apps.len(), 1);
        let alice = &apps[0];
        assert_eq!(alice.did, "did:plc:alice");
        assert_eq!(alice.handle, "did:plc:alice");
        assert_eq!(
            alice.membership,
            "at://did:plc:alice/social.colibri.membership/m1"
        );
        assert_eq!(alice.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(alice.data.display_name, "did:plc:alice");
        assert_eq!(alice.data.online_state, "offline");
        assert_eq!(alice.data.status.text, "");
        assert!(alice.data.status.emoji.is_none());
    }

    #[test]
    fn build_applications_uses_profile_handle_and_state() {
        let mut aggregate = empty_aggregate(vec![PendingApplicant {
            did: String::from("did:plc:alice"),
            membership_uri: String::from("at://did:plc:alice/social.colibri.membership/m1"),
            created_at: String::from("2026-02-02T00:00:00Z"),
        }]);
        aggregate
            .handles
            .insert(String::from("did:plc:alice"), String::from("alice.test"));
        aggregate.profiles.insert(
            String::from("did:plc:alice"),
            ActorProfile {
                display_name: Some(String::from("Alice")),
                description: Some(String::from("Alice bio")),
                pronouns: None,
                website: None,
                avatar: Some(serde_json::json!({ "ref": "alice-avatar" })),
                banner: None,
                labels: None,
                joined_via_starter_pack: None,
                pinned_post: None,
                created_at: None,
            },
        );
        aggregate.actor_data.insert(
            String::from("did:plc:alice"),
            ColibriActorData {
                record_type: None,
                emoji: Some(String::from("🦜")),
                status: Some(String::from("Working")),
                communities: vec![],
            },
        );
        aggregate
            .states
            .insert(String::from("did:plc:alice"), String::from("online"));

        let apps = build_applications(aggregate);
        let alice = &apps[0];
        assert_eq!(alice.handle, "alice.test");
        assert_eq!(alice.data.display_name, "Alice");
        assert_eq!(alice.data.description.as_deref(), Some("Alice bio"));
        assert_eq!(alice.data.online_state, "online");
        assert_eq!(alice.data.status.text, "Working");
        assert_eq!(alice.data.status.emoji.as_deref(), Some("🦜"));
    }

    #[tokio::test]
    async fn lists_applications_when_caller_has_permission() {
        let db = mock_db();
        let result = list_applications_with(
            String::from("at://did:plc:community/social.colibri.community/self"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: Some(member("did:plc:mod", vec!["r1"])),
                        roles: vec![role("Moderator", 10, vec![Permission::ApprovalManage])],
                    })
                })
            },
            &|_, authority, uri| {
                Box::pin(async move {
                    assert_eq!(authority, "did:plc:community");
                    assert_eq!(uri, "at://did:plc:community/social.colibri.community/self");
                    Ok(empty_aggregate(vec![PendingApplicant {
                        did: String::from("did:plc:applicant"),
                        membership_uri: String::from(
                            "at://did:plc:applicant/social.colibri.membership/m1",
                        ),
                        created_at: String::from("2026-03-03T00:00:00Z"),
                    }]))
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.applications.len(), 1);
        assert_eq!(result.applications[0].did, "did:plc:applicant");
        assert_eq!(
            result.applications[0].membership,
            "at://did:plc:applicant/social.colibri.membership/m1"
        );
    }

    #[tokio::test]
    async fn rejects_when_caller_lacks_approval_permission() {
        let db = mock_db();
        let result = list_applications_with(
            String::from("at://did:plc:community/social.colibri.community/self"),
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
            &|_, _, _| Box::pin(async { panic!("should not fetch applications") }),
        )
        .await;

        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = list_applications_with(
            String::from("not-a-uri"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { panic!("should not authenticate") }),
            &|_, _, _| Box::pin(async { panic!("should not load authz") }),
            &|_, _, _| Box::pin(async { panic!("should not fetch applications") }),
        )
        .await;

        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
