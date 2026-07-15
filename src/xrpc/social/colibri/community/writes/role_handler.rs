use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriRole;
use crate::lib::community_authz::ActorAuthz;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation::generate_tid;
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::xrpc::social::colibri::community::reads::list_roles_handler::fetch_role_records;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const ROLE_NSID: &str = "social.colibri.role";

#[derive(Serialize, Debug)]
pub struct RoleUriResponse {
    pub uri: String,
}

fn forbidden(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("Forbidden"),
            message: message.into(),
        }),
    }
}

fn assert_position_manageable(authz: &ActorAuthz, position: i64) -> Result<(), ErrorResponse> {
    if authz.outranks_position(position) {
        Ok(())
    } else {
        Err(forbidden(
            "Cannot manage a role at or above your own position.",
        ))
    }
}

fn assert_permissions_grantable(
    authz: &ActorAuthz,
    permissions: &[String],
) -> Result<(), ErrorResponse> {
    for requested in permissions {
        let recognized = Permission::all().iter().find(|p| p.as_str() == requested);
        match recognized {
            Some(perm) if authz.has(*perm, None) => {}
            _ => {
                return Err(forbidden(format!(
                    "Cannot grant a permission you do not hold: {requested}"
                )));
            }
        }
    }
    Ok(())
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
            let data =
                serde_json::to_value(&role).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(db, community_did, ROLE_NSID, &rkey, data).await?;
            next_free += 1;
        } else {
            next_free = role.position + 1;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
            assert_position_manageable(&ctx.authz, position)?;
            assert_permissions_grantable(&ctx.authz, &permissions)?;

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
            let role_data =
                serde_json::to_value(&role).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::create_record(
                &db,
                community_did,
                ROLE_NSID,
                Some(&role_rkey),
                role_data,
            )
            .await?;

            Ok(Json(RoleUriResponse {
                uri: format!("at://{}/{}/{}", community_did, ROLE_NSID, role_rkey),
            }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.role.create?<community>&<name>&<color>&<permissions>&<position>&<hoisted>&<mentionable>&<auth>"
)]
#[allow(clippy::too_many_arguments)]
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

#[allow(clippy::too_many_arguments)]
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

            let mut rec: ColibriRole = serde_json::from_value(current)
                .map_err(|e| invalid_request(format!("Cached role record is malformed: {e}")))?;

            if rec.protected == Some(true) {
                return Err(invalid_request("Cannot modify a protected role."));
            }

            assert_position_manageable(&ctx.authz, rec.position)?;

            if let Some(pos) = position {
                assert_position_manageable(&ctx.authz, pos)?;
            }
            if !permissions.is_empty() {
                assert_permissions_grantable(&ctx.authz, &permissions)?;
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

#[post(
    "/xrpc/social.colibri.role.update?<role>&<name>&<color>&<permissions>&<position>&<hoisted>&<mentionable>&<auth>"
)]
#[allow(clippy::too_many_arguments)]
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

            let rec: ColibriRole = serde_json::from_value(current)
                .map_err(|e| invalid_request(format!("Cached role record is malformed: {e}")))?;

            if rec.protected == Some(true) {
                return Err(invalid_request("Cannot delete a protected role."));
            }

            assert_position_manageable(&ctx.authz, rec.position)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::{member, mock_db, owner_authz, role};
    use crate::models::record_data;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn role_record(
        rkey: &str,
        name: &str,
        position: i64,
        permissions: Vec<Permission>,
    ) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from("did:plc:owner"),
            nsid: String::from(ROLE_NSID),
            rkey: String::from(rkey),
            data: serde_json::to_value(role(name, position, permissions)).unwrap(),
            indexed_at: String::new(),
        }
    }

    fn admin_authz() -> ActorAuthz {
        ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:admin", vec!["admin"])),
            roles: vec![role("Admin", 50, vec![Permission::RoleManage])],
        }
    }

    fn mod_authz() -> ActorAuthz {
        ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:mod", vec!["mod"])),
            roles: vec![role("Mod", 10, vec![Permission::RoleManage])],
        }
    }

    fn assert_not_position_forbidden(result: &Result<Json<RoleUriResponse>, ErrorResponse>) {
        if let Err(e) = result {
            assert!(!e.body.message.contains("at or above your own position"));
        }
    }

    #[tokio::test]
    async fn update_role_owner_can_edit_any_role() {
        let _ = crate::lib::crypto::install_master_key(vec![0u8; 32]);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![role_record("admin", "Admin", 50, vec![])]])
            .into_connection();

        let result = update_role_with(
            String::from("at://did:plc:owner/social.colibri.role/admin"),
            Some(String::from("Renamed")),
            None,
            vec![],
            None,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            &|_, _, _| Box::pin(async { Ok(owner_authz()) }),
        )
        .await;

        assert_not_position_forbidden(&result);
    }

    #[tokio::test]
    async fn update_role_admin_can_edit_role_below_them() {
        let _ = crate::lib::crypto::install_master_key(vec![0u8; 32]);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![role_record("mod", "Mod", 10, vec![])]])
            .into_connection();

        let result = update_role_with(
            String::from("at://did:plc:owner/social.colibri.role/mod"),
            Some(String::from("Renamed")),
            None,
            vec![],
            None,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:admin")) }),
            &|_, _, _| Box::pin(async { Ok(admin_authz()) }),
        )
        .await;

        assert_not_position_forbidden(&result);
    }

    #[tokio::test]
    async fn update_role_admin_rejects_editing_own_role() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![role_record("admin", "Admin", 50, vec![])]])
            .into_connection();

        let result = update_role_with(
            String::from("at://did:plc:owner/social.colibri.role/admin"),
            Some(String::from("Renamed")),
            None,
            vec![],
            None,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:admin")) }),
            &|_, _, _| Box::pin(async { Ok(admin_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }

    #[tokio::test]
    async fn update_role_mod_rejects_editing_own_role() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![role_record("mod", "Mod", 10, vec![])]])
            .into_connection();

        let result = update_role_with(
            String::from("at://did:plc:owner/social.colibri.role/mod"),
            Some(String::from("Renamed")),
            None,
            vec![],
            None,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| Box::pin(async { Ok(mod_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }

    #[tokio::test]
    async fn create_role_mod_can_create_role_below_own_position() {
        let db = mock_db();

        let result = create_role_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("Helper"),
            None,
            vec![],
            5,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| Box::pin(async { Ok(mod_authz()) }),
        )
        .await;

        assert_not_position_forbidden(&result);
    }

    #[tokio::test]
    async fn create_role_rejects_position_at_or_above_caller() {
        let db = mock_db();

        let result = create_role_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("Sneaky"),
            None,
            vec![],
            10,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| Box::pin(async { Ok(mod_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }

    #[tokio::test]
    async fn create_role_rejects_ungranted_permission() {
        let db = mock_db();

        let result = create_role_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("Helper"),
            None,
            vec![String::from("community.delete")],
            5,
            None,
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| Box::pin(async { Ok(mod_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("do not hold"));
    }

    #[tokio::test]
    async fn delete_role_rejects_deleting_own_role() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![role_record("mod", "Mod", 10, vec![])]])
            .into_connection();

        let result = delete_role_with(
            String::from("at://did:plc:owner/social.colibri.role/mod"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| Box::pin(async { Ok(mod_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }
}
