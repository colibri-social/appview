use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriRole;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation::generate_tid;
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;
use crate::xrpc::social::colibri::community::reads::list_roles_handler::fetch_role_records;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const ROLE_NSID: &str = "social.colibri.role";

#[derive(Serialize, Debug)]
pub struct RoleUriResponse {
    pub uri: String,
}

// ---- role.create -----------------------------------------------------------

/// Makes room for a new role at `target_position` by bumping any existing
/// role that already occupies it (and cascading further up as needed) by
/// the minimum amount required to keep every role's position unique —
/// including the protected Owner role (conventionally `100`), which isn't
/// exempt: if a community ever has 100+ roles, it has to move too.
async fn resolve_position_conflicts(
    db: &DatabaseConnection,
    community_did: &str,
    target_position: i64,
) -> Result<(), sea_orm::DbErr> {
    let records = fetch_role_records(db, community_did).await?;

    let mut roles: Vec<(String, ColibriRole)> = records
        .into_iter()
        .filter_map(|r| {
            let role: ColibriRole = serde_json::from_value(r.data).ok()?;
            Some((r.rkey, role))
        })
        .filter(|(_, role)| role.position >= target_position)
        .collect();
    // Stable order across same-position roles (a pre-existing duplicate-position
    // case this very fix is meant to clean up) so the cascade is deterministic.
    roles.sort_by(|(rkey_a, role_a), (rkey_b, role_b)| {
        role_a
            .position
            .cmp(&role_b.position)
            .then_with(|| rkey_a.cmp(rkey_b))
    });

    let mut next_free = target_position + 1;
    for (rkey, mut role) in roles {
        if role.position < next_free {
            role.position = next_free;
            let data = serde_json::to_value(&role)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(db, community_did, ROLE_NSID, &rkey, data).await?;
            next_free += 1;
        } else {
            next_free = role.position + 1;
        }
    }

    Ok(())
}

async fn create_role_with(
    community_uri: String,
    name: String,
    color: Option<String>,
    permissions: Vec<String>,
    position: i64,
    hoisted: Option<bool>,
    mentionable: Option<bool>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<RoleUriResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.role.create",
        community_uri,
        Some(Permission::RoleManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let role_rkey = generate_tid();

            resolve_position_conflicts(&db, community_did, position).await?;

            let role = ColibriRole {
                record_type: Some(ROLE_NSID.to_string()),
                name,
                color,
                permissions,
                position,
                hoisted,
                mentionable,
                protected: None,
                channel_overrides: vec![],
            };
            let role_data = serde_json::to_value(&role)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::create_record(&db, community_did, ROLE_NSID, Some(&role_rkey), role_data)
                .await?;

            Ok(Json(RoleUriResponse {
                uri: format!("at://{}/{}/{}", community_did, ROLE_NSID, role_rkey),
            }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.role.create?<community>&<name>&<color>&<permissions>&<position>&<hoisted>&<mentionable>&<auth>")]
pub async fn create_role(
    community: &str,
    name: &str,
    color: Option<&str>,
    permissions: Vec<String>,
    position: i64,
    hoisted: Option<bool>,
    mentionable: Option<bool>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<RoleUriResponse>, ErrorResponse> {
    create_role_with(
        community.to_string(),
        name.to_string(),
        color.map(str::to_string),
        permissions,
        position,
        hoisted,
        mentionable,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- role.update -----------------------------------------------------------

async fn update_role_with(
    role_uri: String,
    name: Option<String>,
    color: Option<String>,
    permissions: Vec<String>,
    position: Option<i64>,
    hoisted: Option<bool>,
    mentionable: Option<bool>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<RoleUriResponse>, ErrorResponse> {
    let role = AtUri::parse(&role_uri).ok_or_else(|| invalid_request("Invalid role AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        role.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

    with_community_authz(
        auth,
        "social.colibri.role.update",
        community_uri,
        Some(Permission::RoleManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let role_rkey = &role.rkey;

            let current = community_write::read_cached(&db, community_did, ROLE_NSID, role_rkey)
                .await?
                .ok_or_else(|| not_found_error("Role not found in AppView cache."))?;

            let mut rec: ColibriRole = serde_json::from_value(current).map_err(|e| {
                invalid_request(format!("Cached role record is malformed: {e}"))
            })?;

            if rec.protected == Some(true) {
                return Err(invalid_request("Cannot modify a protected role."));
            }

            if let Some(n) = name {
                rec.name = n;
            }
            if let Some(c) = color {
                rec.color = Some(c);
            }
            // Non-empty permissions list replaces the current set; empty means no change.
            if !permissions.is_empty() {
                rec.permissions = permissions;
            }
            if let Some(pos) = position {
                rec.position = pos;
            }
            if let Some(h) = hoisted {
                rec.hoisted = Some(h);
            }
            if let Some(m) = mentionable {
                rec.mentionable = Some(m);
            }

            let data =
                serde_json::to_value(&rec).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, ROLE_NSID, role_rkey, data).await?;

            Ok(Json(RoleUriResponse { uri: role_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.role.update?<role>&<name>&<color>&<permissions>&<position>&<hoisted>&<mentionable>&<auth>")]
pub async fn update_role(
    role: &str,
    name: Option<&str>,
    color: Option<&str>,
    permissions: Vec<String>,
    position: Option<i64>,
    hoisted: Option<bool>,
    mentionable: Option<bool>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<RoleUriResponse>, ErrorResponse> {
    update_role_with(
        role.to_string(),
        name.map(str::to_string),
        color.map(str::to_string),
        permissions,
        position,
        hoisted,
        mentionable,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- role.delete -----------------------------------------------------------

async fn delete_role_with(
    role_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<RoleUriResponse>, ErrorResponse> {
    let role = AtUri::parse(&role_uri).ok_or_else(|| invalid_request("Invalid role AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        role.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

    with_community_authz(
        auth,
        "social.colibri.role.delete",
        community_uri,
        Some(Permission::RoleManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let role_rkey = &role.rkey;

            let current = community_write::read_cached(&db, community_did, ROLE_NSID, role_rkey)
                .await?
                .ok_or_else(|| not_found_error("Role not found in AppView cache."))?;

            let rec: ColibriRole = serde_json::from_value(current).map_err(|e| {
                invalid_request(format!("Cached role record is malformed: {e}"))
            })?;

            if rec.protected == Some(true) {
                return Err(invalid_request("Cannot delete a protected role."));
            }

            community_write::delete_record(&db, community_did, ROLE_NSID, role_rkey).await?;

            Ok(Json(RoleUriResponse { uri: role_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.role.delete?<role>&<auth>")]
pub async fn delete_role(
    role: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<RoleUriResponse>, ErrorResponse> {
    delete_role_with(
        role.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
