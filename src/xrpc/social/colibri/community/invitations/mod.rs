use std::collections::HashMap;

use futures::future::BoxFuture;
use rand::Rng;
use rand::distributions::Alphanumeric;
use rocket::serde::json::Json;
use rocket::{State, get, post};
use sea_orm::{
    ActiveValue, ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter,
    sea_query,
};
use serde::Serialize;
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{
    ColibriActorData, ColibriActorProfile, ColibriCommunity, resolve_effective_profile,
};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::time::current_iso8601_utc;
use crate::models::community_invitations::{
    self, ActiveModel as InvitationModel, Entity as Invitations, Model as Invitation,
};
use crate::models::{record_data, repos, user_states};
use crate::xrpc::social::colibri::actor::get_data_handler::{
    Actor, ActorStatus, actor_data_from_effective,
};
use crate::xrpc::social::colibri::community::reads::list_members_handler::fetch_member_aggregate;

/// An invitation hydrated with the invited community's public
/// details (name, picture, member/online counts, join policy) so the invite
/// accept screen can render without a follow-up fetch. Used by `getInvitation`.
#[derive(Serialize, Debug)]
pub struct InvitationResolvedView {
    pub code: String,
    pub community: String,
    #[serde(rename = "createdBy")]
    pub created_by: String,
    pub active: bool,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<Value>,
    #[serde(rename = "memberCount")]
    pub member_count: u64,
    #[serde(rename = "onlineCount")]
    pub online_count: u64,
    #[serde(rename = "requiresApprovalToJoin")]
    pub requires_approval_to_join: bool,
}

/// The invited community's public details, resolved from its record and member
/// aggregate.
pub struct InvitationCommunity {
    pub name: String,
    pub picture: Option<Value>,
    pub requires_approval_to_join: bool,
    pub member_count: u64,
    pub online_count: u64,
}

/// An invitation with `createdBy` hydrated into the full creator profile
/// rather than a bare DID. Used by `listInvitations`.
#[derive(Serialize, Debug)]
pub struct InvitationProfileView {
    pub code: String,
    pub community: String,
    #[serde(rename = "createdBy")]
    pub created_by: Actor,
    pub active: bool,
}

#[derive(Serialize, Debug)]
pub struct InvitationListResponse {
    pub codes: Vec<InvitationProfileView>,
}

#[derive(Serialize, Debug)]
pub struct DeleteInvitationResponse {
    pub code: String,
}

const CODE_LENGTH: usize = 16;

fn generate_invitation_code() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(CODE_LENGTH)
        .map(char::from)
        .collect()
}

// ---- Module-local dependency types --------------------------------------
// (`VerifyAuthFn` and `LoadAuthzFn` come from `crate::lib::handler`.)

type InsertFn = dyn Fn(DatabaseConnection, String, String, String) -> BoxFuture<'static, Result<Invitation, DbErr>>
    + Send
    + Sync;
type FetchOneFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Option<Invitation>, DbErr>>
    + Send
    + Sync;
type FetchManyFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<Invitation>, DbErr>>
    + Send
    + Sync;
type DeactivateFn =
    dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<u64, DbErr>> + Send + Sync;

// ---- DB helpers ----------------------------------------------------------

pub async fn insert_invitation(
    db: &DatabaseConnection,
    code: String,
    community_uri: String,
    created_by: String,
) -> Result<Invitation, DbErr> {
    let row = InvitationModel {
        code: ActiveValue::Set(code.clone()),
        community_uri: ActiveValue::Set(community_uri),
        created_by: ActiveValue::Set(created_by),
        active: ActiveValue::Set(true),
        created_at: ActiveValue::Set(current_iso8601_utc()),
    };
    Invitations::insert(row).exec(db).await?;
    Invitations::find()
        .filter(community_invitations::Column::Code.eq(&code))
        .one(db)
        .await?
        .ok_or_else(|| DbErr::Custom(format!("inserted invitation missing: {code}")))
}

pub async fn fetch_invitation(
    db: &DatabaseConnection,
    code: &str,
) -> Result<Option<Invitation>, DbErr> {
    Invitations::find()
        .filter(community_invitations::Column::Code.eq(code))
        .one(db)
        .await
}

pub async fn fetch_invitations_for_community(
    db: &DatabaseConnection,
    community_uri: &str,
) -> Result<Vec<Invitation>, DbErr> {
    Invitations::find()
        .filter(community_invitations::Column::CommunityUri.eq(community_uri))
        .all(db)
        .await
}

pub async fn deactivate_invitation(db: &DatabaseConnection, code: &str) -> Result<u64, DbErr> {
    let res = Invitations::update_many()
        .col_expr(
            community_invitations::Column::Active,
            sea_query::Expr::value(false),
        )
        .filter(community_invitations::Column::Code.eq(code))
        .exec(db)
        .await?;
    Ok(res.rows_affected)
}

// ---- createInvitation ----------------------------------------------------

#[derive(Serialize, Debug)]
pub struct CreateInvitationResponse {
    pub code: String,
    pub community: String,
    #[serde(rename = "createdBy")]
    pub created_by: String,
    pub active: bool,
}

async fn create_invitation_with(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    insert_fn: &InsertFn,
    code_generator: fn() -> String,
) -> Result<Json<CreateInvitationResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.createInvitation",
        community_uri,
        Some(Permission::InvitationCreate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let code = code_generator();
            let inv = insert_fn(db, code, ctx.community_uri, ctx.caller_did).await?;
            Ok(Json(CreateInvitationResponse {
                code: inv.code,
                community: inv.community_uri,
                created_by: inv.created_by,
                active: inv.active,
            }))
        },
    )
    .await
}

// ---- getInvitation -------------------------------------------------------

/// Standalone fetch function for `getInvitation`. Distinct from `FetchOneFn`
/// in that it takes only the code (no DB-passthrough param) so the live
/// handler can capture its `DatabaseConnection`.
type FetchByCodeFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Option<Invitation>, DbErr>> + Send + Sync;

/// Resolves a community AT-URI to the public details the invite screen needs.
/// Returns `None` when the community record is missing (e.g. deleted after the
/// invitation was minted).
type FetchInvitationCommunityFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Option<InvitationCommunity>, DbErr>> + Send + Sync;

/// Reads the community record for `community_uri` plus its member aggregate and
/// distils the public details the invite accept screen renders.
pub async fn fetch_invitation_community(
    db: &DatabaseConnection,
    community_uri: &str,
) -> Result<Option<InvitationCommunity>, DbErr> {
    let community = AtUri::parse(community_uri)
        .ok_or_else(|| DbErr::Custom(format!("invalid community AT-URI: {community_uri}")))?;

    let record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.community"))
        .filter(record_data::Column::Rkey.eq(&community.rkey))
        .one(db)
        .await?;

    let Some(record) = record else {
        return Ok(None);
    };

    let stored = serde_json::from_value::<ColibriCommunity>(record.data)
        .map_err(|e| DbErr::Custom(format!("failed to parse community record: {e}")))?;

    let aggregate = fetch_member_aggregate(db, community_uri).await?;
    let member_count = aggregate.member_dids.len() as u64;
    let online_count = aggregate
        .states
        .values()
        .filter(|state| state.as_str() != "offline")
        .count() as u64;

    Ok(Some(InvitationCommunity {
        name: stored.name,
        picture: stored.picture,
        requires_approval_to_join: stored.requires_approval_to_join,
        member_count,
        online_count,
    }))
}

async fn get_invitation_with(
    code: String,
    fetch_fn: &FetchByCodeFn,
    fetch_community_fn: &FetchInvitationCommunityFn,
) -> Result<Json<InvitationResolvedView>, ErrorResponse> {
    let row = fetch_fn(code.clone()).await?.ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("NotFound"),
            message: format!("Invitation '{code}' not found."),
        }),
    })?;

    let community = fetch_community_fn(row.community_uri.clone())
        .await?
        .ok_or_else(|| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("NotFound"),
                message: format!("Community for invitation '{code}' not found."),
            }),
        })?;

    Ok(Json(InvitationResolvedView {
        code: row.code,
        community: row.community_uri,
        created_by: row.created_by,
        active: row.active,
        name: community.name,
        picture: community.picture,
        member_count: community.member_count,
        online_count: community.online_count,
        requires_approval_to_join: community.requires_approval_to_join,
    }))
}

// ---- Profile hydration ---------------------------------------------------

/// Hydrates a set of DIDs into their full creator [`Actor`] profiles, keyed by
/// DID. DIDs without a stored profile, handle, or state fall back to sensible
/// defaults (see [`build_actor`]).
type HydrateFn = dyn Fn(DatabaseConnection, Vec<String>) -> BoxFuture<'static, Result<HashMap<String, Actor>, DbErr>>
    + Send
    + Sync;

/// Assembles an [`Actor`] from its component records, mirroring the fallbacks
/// used by `listMembers`: handle and display name default to the DID, online
/// state defaults to `offline`, and an empty status is used when none exists.
fn build_actor(
    did: String,
    handle: Option<String>,
    profile: Option<ActorProfile>,
    colibri_profile: Option<ColibriActorProfile>,
    actor_data: Option<ColibriActorData>,
    state: Option<String>,
) -> Actor {
    let handle = handle.unwrap_or_else(|| did.clone());

    // `is_bot` always comes from Bluesky; the served profile fields come from the
    // effective profile so a non-synced Colibri user shows their Colibri identity
    // here, matching `getData` and the profile popover.
    let is_bot = profile.as_ref().is_some_and(ActorProfile::is_bot);
    let effective = resolve_effective_profile(colibri_profile.as_ref(), profile.as_ref());

    let status = actor_data
        .map(|d| ActorStatus {
            text: d.status.unwrap_or(String::from("")),
            emoji: d.emoji,
        })
        .unwrap_or(ActorStatus {
            text: String::new(),
            emoji: None,
        });

    let online_state = state.unwrap_or_else(|| String::from("offline"));

    Actor {
        did,
        data: actor_data_from_effective(effective, is_bot, &handle, online_state, status),
        handle,
    }
}

/// Fetches profile records, Colibri actor data, online states, and handles for
/// every DID in `dids` in a single round-trip per backing table, then builds an
/// [`Actor`] for each.
pub async fn hydrate_actors(
    db: &DatabaseConnection,
    dids: Vec<String>,
) -> Result<HashMap<String, Actor>, DbErr> {
    if dids.is_empty() {
        return Ok(HashMap::new());
    }

    let self_records = record_data::Entity::find()
        .filter(record_data::Column::Did.is_in(dids.clone()))
        .filter(record_data::Column::Rkey.eq("self"))
        .filter(
            Condition::any()
                .add(record_data::Column::Nsid.eq("app.bsky.actor.profile"))
                .add(record_data::Column::Nsid.eq("social.colibri.actor.profile"))
                .add(record_data::Column::Nsid.eq("social.colibri.actor.data")),
        )
        .all(db)
        .await?;

    let mut profiles: HashMap<String, ActorProfile> = HashMap::new();
    let mut colibri_profiles: HashMap<String, ColibriActorProfile> = HashMap::new();
    let mut actor_data: HashMap<String, ColibriActorData> = HashMap::new();
    for record in self_records {
        if record.nsid == "app.bsky.actor.profile" {
            if let Ok(profile) = serde_json::from_value::<ActorProfile>(record.data) {
                profiles.insert(record.did, profile);
            }
        } else if record.nsid == "social.colibri.actor.profile" {
            if let Ok(profile) = serde_json::from_value::<ColibriActorProfile>(record.data) {
                colibri_profiles.insert(record.did, profile);
            }
        } else if record.nsid == "social.colibri.actor.data"
            && let Ok(data) = serde_json::from_value::<ColibriActorData>(record.data)
        {
            actor_data.insert(record.did, data);
        }
    }

    let state_rows = user_states::Entity::find()
        .filter(user_states::Column::Did.is_in(dids.clone()))
        .all(db)
        .await?;
    let mut states: HashMap<String, String> = state_rows
        .into_iter()
        .map(|row| (row.did, row.state))
        .collect();

    let repo_rows = repos::Entity::find()
        .filter(repos::Column::Did.is_in(dids.clone()))
        .all(db)
        .await?;
    let mut handles: HashMap<String, String> = repo_rows
        .into_iter()
        .filter_map(|row| row.handle.map(|h| (row.did, h)))
        .collect();

    let actors = dids
        .into_iter()
        .map(|did| {
            let actor = build_actor(
                did.clone(),
                handles.remove(&did),
                profiles.remove(&did),
                colibri_profiles.remove(&did),
                actor_data.remove(&did),
                states.remove(&did),
            );
            (did, actor)
        })
        .collect();

    Ok(actors)
}

// ---- listInvitations -----------------------------------------------------

async fn list_invitations_with(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    fetch_fn: &FetchManyFn,
    hydrate_fn: &HydrateFn,
) -> Result<Json<InvitationListResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.listInvitations",
        community_uri,
        Some(Permission::InvitationCreate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let rows = fetch_fn(db.clone(), ctx.community_uri).await?;

            let mut creator_dids: Vec<String> =
                rows.iter().map(|inv| inv.created_by.clone()).collect();
            creator_dids.sort();
            creator_dids.dedup();

            let creators = hydrate_fn(db, creator_dids).await?;

            let codes = rows
                .into_iter()
                .map(|inv| {
                    let created_by = creators.get(&inv.created_by).cloned().unwrap_or_else(|| {
                        build_actor(inv.created_by, None, None, None, None, None)
                    });
                    InvitationProfileView {
                        code: inv.code,
                        community: inv.community_uri,
                        created_by,
                        active: inv.active,
                    }
                })
                .collect();

            Ok(Json(InvitationListResponse { codes }))
        },
    )
    .await
}

// ---- deleteInvitation ----------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn delete_invitation_with(
    community_uri: String,
    code: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    fetch_fn: &FetchOneFn,
    deactivate_fn: &DeactivateFn,
) -> Result<Json<DeleteInvitationResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.deleteInvitation",
        community_uri,
        Some(Permission::InvitationDelete),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let invitation =
                fetch_fn(db.clone(), code.clone())
                    .await?
                    .ok_or_else(|| ErrorResponse {
                        body: Json(ErrorBody {
                            error: String::from("NotFound"),
                            message: format!("Invitation '{code}' not found."),
                        }),
                    })?;

            if invitation.community_uri != ctx.community_uri {
                return Err(ErrorResponse {
                    body: Json(ErrorBody {
                        error: String::from("InvalidRequest"),
                        message: String::from(
                            "Invitation does not belong to the specified community.",
                        ),
                    }),
                });
            }

            deactivate_fn(db, code.clone()).await?;
            Ok(Json(DeleteInvitationResponse { code }))
        },
    )
    .await
}

// ---- Boxed dependencies --------------------------------------------------

fn insert_boxed(
    db: DatabaseConnection,
    code: String,
    community_uri: String,
    created_by: String,
) -> BoxFuture<'static, Result<Invitation, DbErr>> {
    Box::pin(async move { insert_invitation(&db, code, community_uri, created_by).await })
}

fn fetch_boxed(
    db: DatabaseConnection,
    code: String,
) -> BoxFuture<'static, Result<Option<Invitation>, DbErr>> {
    Box::pin(async move { fetch_invitation(&db, &code).await })
}

fn fetch_by_community_boxed(
    db: DatabaseConnection,
    community_uri: String,
) -> BoxFuture<'static, Result<Vec<Invitation>, DbErr>> {
    Box::pin(async move { fetch_invitations_for_community(&db, &community_uri).await })
}

fn deactivate_boxed(
    db: DatabaseConnection,
    code: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move { deactivate_invitation(&db, &code).await })
}

fn hydrate_actors_boxed(
    db: DatabaseConnection,
    dids: Vec<String>,
) -> BoxFuture<'static, Result<HashMap<String, Actor>, DbErr>> {
    Box::pin(async move { hydrate_actors(&db, dids).await })
}

// ---- Public Rocket routes -----------------------------------------------

#[post("/xrpc/social.colibri.community.createInvitation?<community>&<auth>")]
pub async fn create_invitation(
    community: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CreateInvitationResponse>, ErrorResponse> {
    create_invitation_with(
        community.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &insert_boxed,
        generate_invitation_code,
    )
    .await
}

#[get("/xrpc/social.colibri.community.getInvitation?<code>")]
pub async fn get_invitation(
    code: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<InvitationResolvedView>, ErrorResponse> {
    let db = db.inner().clone();
    let fetch = {
        let db = db.clone();
        move |c: String| -> BoxFuture<'static, Result<Option<Invitation>, DbErr>> {
            let db = db.clone();
            Box::pin(async move { fetch_invitation(&db, &c).await })
        }
    };
    let fetch_community =
        move |uri: String| -> BoxFuture<'static, Result<Option<InvitationCommunity>, DbErr>> {
            let db = db.clone();
            Box::pin(async move { fetch_invitation_community(&db, &uri).await })
        };
    get_invitation_with(code.to_string(), &fetch, &fetch_community).await
}

#[post("/xrpc/social.colibri.community.listInvitations?<uri>&<auth>")]
pub async fn list_invitations(
    uri: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<InvitationListResponse>, ErrorResponse> {
    list_invitations_with(
        uri.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &fetch_by_community_boxed,
        &hydrate_actors_boxed,
    )
    .await
}

#[post("/xrpc/social.colibri.community.deleteInvitation?<uri>&<code>&<auth>")]
pub async fn delete_invitation(
    uri: &str,
    code: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<DeleteInvitationResponse>, ErrorResponse> {
    delete_invitation_with(
        uri.to_string(),
        code.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &fetch_boxed,
        &deactivate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::{empty_authz, mock_db, owner_authz};
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn invite(code: &str, community: &str, by: &str, active: bool) -> Invitation {
        Invitation {
            code: code.to_string(),
            community_uri: community.to_string(),
            created_by: by.to_string(),
            active,
            created_at: String::from("2026-05-14T00:00:00Z"),
        }
    }

    #[tokio::test]
    async fn create_invitation_returns_inserted_view() {
        let db = mock_db();
        let captured: Arc<Mutex<Option<(String, String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let insert = move |_: DatabaseConnection,
                           code: String,
                           community: String,
                           created_by: String|
              -> BoxFuture<'static, Result<Invitation, DbErr>> {
            let captured = captured_clone.clone();
            Box::pin(async move {
                *captured.lock().unwrap() =
                    Some((code.clone(), community.clone(), created_by.clone()));
                Ok(invite(&code, &community, &created_by, true))
            })
        };
        let result = create_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &insert,
            || String::from("CODE-FIXED"),
        )
        .await
        .unwrap();

        let inserted = captured.lock().unwrap().take().unwrap();
        assert_eq!(inserted.0, "CODE-FIXED");
        assert_eq!(inserted.1, "at://did:plc:owner/social.colibri.community/c1");
        assert_eq!(inserted.2, "did:plc:owner");

        assert_eq!(result.code, "CODE-FIXED");
        assert!(result.active);
        assert_eq!(result.created_by, "did:plc:owner");
    }

    #[tokio::test]
    async fn create_invitation_rejects_without_permission() {
        let db = mock_db();
        let result = create_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| Box::pin(async { Ok(empty_authz()) }),
            &|_, _, _, _| Box::pin(async { panic!("should not insert") }),
            || String::from("CODE"),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    fn invitation_community() -> InvitationCommunity {
        InvitationCommunity {
            name: String::from("Test Community"),
            picture: Some(serde_json::json!({ "ref": "pic" })),
            requires_approval_to_join: true,
            member_count: 42,
            online_count: 7,
        }
    }

    #[tokio::test]
    async fn get_invitation_returns_hydrated_view() {
        let result = get_invitation_with(
            String::from("CODE"),
            &|c| {
                Box::pin(async move {
                    Ok(Some(invite(
                        &c,
                        "at://did:plc:owner/social.colibri.community/c1",
                        "did:plc:owner",
                        true,
                    )))
                })
            },
            &|uri| {
                Box::pin(async move {
                    assert_eq!(uri, "at://did:plc:owner/social.colibri.community/c1");
                    Ok(Some(invitation_community()))
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.code, "CODE");
        assert_eq!(
            result.community,
            "at://did:plc:owner/social.colibri.community/c1"
        );
        assert!(result.active);
        assert_eq!(result.name, "Test Community");
        assert_eq!(result.member_count, 42);
        assert_eq!(result.online_count, 7);
        assert!(result.requires_approval_to_join);
        assert!(result.picture.is_some());
    }

    #[tokio::test]
    async fn get_invitation_returns_not_found_when_missing() {
        let result = get_invitation_with(
            String::from("NOPE"),
            &|_| Box::pin(async { Ok(None) }),
            &|_| Box::pin(async { panic!("should not resolve community when invitation missing") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }

    #[tokio::test]
    async fn get_invitation_returns_not_found_when_community_missing() {
        let result = get_invitation_with(
            String::from("CODE"),
            &|c| {
                Box::pin(async move {
                    Ok(Some(invite(
                        &c,
                        "at://did:plc:owner/social.colibri.community/c1",
                        "did:plc:owner",
                        true,
                    )))
                })
            },
            &|_| Box::pin(async { Ok(None) }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }

    #[tokio::test]
    async fn list_invitations_returns_all_for_community() {
        let db = mock_db();
        let result = list_invitations_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, _| {
                Box::pin(async {
                    Ok(vec![
                        invite(
                            "A",
                            "at://did:plc:owner/social.colibri.community/c1",
                            "did:plc:owner",
                            true,
                        ),
                        invite(
                            "B",
                            "at://did:plc:owner/social.colibri.community/c1",
                            "did:plc:owner",
                            false,
                        ),
                    ])
                })
            },
            &|_, dids| {
                Box::pin(async move {
                    // Both invitations share one creator, so a single distinct
                    // DID must be passed for hydration.
                    assert_eq!(dids, vec![String::from("did:plc:owner")]);
                    Ok(HashMap::from([(
                        String::from("did:plc:owner"),
                        build_actor(
                            String::from("did:plc:owner"),
                            Some(String::from("owner.test")),
                            Some(ActorProfile {
                                display_name: Some(String::from("Owner")),
                                description: None,
                                pronouns: None,
                                website: None,
                                avatar: None,
                                banner: None,
                                labels: None,
                                joined_via_starter_pack: None,
                                pinned_post: None,
                                created_at: None,
                            }),
                            None,
                            None,
                            Some(String::from("online")),
                        ),
                    )]))
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.codes.len(), 2);
        assert_eq!(result.codes[0].code, "A");
        assert!(result.codes[0].active);
        assert!(!result.codes[1].active);

        // createdBy is hydrated into the full creator profile, and the shared
        // creator is hydrated for every invitation (not just the first).
        assert_eq!(result.codes[0].created_by.did, "did:plc:owner");
        assert_eq!(result.codes[0].created_by.handle, "owner.test");
        assert_eq!(result.codes[0].created_by.data.display_name, "Owner");
        assert_eq!(result.codes[0].created_by.data.online_state, "online");
        assert_eq!(result.codes[1].created_by.data.display_name, "Owner");
    }

    #[tokio::test]
    async fn delete_invitation_deactivates_when_authorized() {
        let db = mock_db();
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
        let calls_clone = calls.clone();

        let deactivate =
            move |_: DatabaseConnection, c: String| -> BoxFuture<'static, Result<u64, DbErr>> {
                let calls = calls_clone.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(c);
                    Ok(1)
                })
            };
        let result = delete_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("CODE"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, c| {
                Box::pin(async move {
                    Ok(Some(invite(
                        &c,
                        "at://did:plc:owner/social.colibri.community/c1",
                        "did:plc:owner",
                        true,
                    )))
                })
            },
            &deactivate,
        )
        .await
        .unwrap();

        assert_eq!(result.code, "CODE");
        assert_eq!(*calls.lock().unwrap(), vec![String::from("CODE")]);
    }

    #[tokio::test]
    async fn delete_invitation_rejects_when_community_mismatches() {
        let db = mock_db();
        let result = delete_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("CODE"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
            &|_, c| {
                Box::pin(async move {
                    Ok(Some(invite(
                        &c,
                        "at://did:plc:other/social.colibri.community/c2",
                        "did:plc:other",
                        true,
                    )))
                })
            },
            &|_, _| Box::pin(async { panic!("should not deactivate when community mismatches") }),
        )
        .await;

        assert!(result.is_err());
        let err = result.err().unwrap().body.into_inner();
        assert_eq!(err.error, "InvalidRequest");
    }

    #[test]
    fn invitation_codes_are_unique_alphanumeric_strings_of_expected_length() {
        let a = generate_invitation_code();
        let b = generate_invitation_code();
        assert_eq!(a.len(), CODE_LENGTH);
        assert_eq!(b.len(), CODE_LENGTH);
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    fn bsky(display_name: &str) -> ActorProfile {
        serde_json::from_value(serde_json::json!({ "displayName": display_name })).unwrap()
    }

    // These three mirror the `get_data_handler` cases so hydrated actors (used
    // by `listInvitations` and `listBannedUsers`) resolve the same effective
    // profile as the profile popover.

    #[test]
    fn build_actor_uses_non_synced_colibri_profile() {
        let colibri = ColibriActorProfile {
            display_name: Some(String::from("Colibri Owner")),
            sync_bluesky: false,
            theme: Some(crate::lib::colibri::ColibriProfileTheme {
                accent_color: Some(String::from("#ff0000")),
                ..Default::default()
            }),
            ..Default::default()
        };
        let actor = build_actor(
            String::from("did:plc:owner"),
            Some(String::from("owner.test")),
            Some(bsky("Bsky Owner")),
            Some(colibri),
            None,
            None,
        );
        assert_eq!(actor.data.display_name, "Colibri Owner");
        assert!(!actor.data.sync_bluesky);
        assert_eq!(
            actor.data.theme.unwrap().accent_color.as_deref(),
            Some("#ff0000")
        );
    }

    #[test]
    fn build_actor_synced_uses_bsky_fields_but_colibri_theme() {
        let colibri = ColibriActorProfile {
            sync_bluesky: true,
            theme: Some(crate::lib::colibri::ColibriProfileTheme {
                banner_color: Some(String::from("#00ff00")),
                ..Default::default()
            }),
            ..Default::default()
        };
        let actor = build_actor(
            String::from("did:plc:owner"),
            Some(String::from("owner.test")),
            Some(bsky("Bsky Owner")),
            Some(colibri),
            None,
            None,
        );
        assert_eq!(actor.data.display_name, "Bsky Owner");
        assert!(actor.data.sync_bluesky);
        assert_eq!(
            actor.data.theme.unwrap().banner_color.as_deref(),
            Some("#00ff00")
        );
    }

    #[test]
    fn build_actor_un_onboarded_falls_back_to_bsky() {
        let actor = build_actor(
            String::from("did:plc:owner"),
            Some(String::from("owner.test")),
            Some(bsky("Bsky Owner")),
            None,
            None,
            None,
        );
        assert_eq!(actor.data.display_name, "Bsky Owner");
        assert!(!actor.data.sync_bluesky);
        assert!(actor.data.theme.is_none());
    }
}
