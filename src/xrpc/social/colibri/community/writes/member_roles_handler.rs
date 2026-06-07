use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriMember;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;
use crate::models::record_data;

const MEMBER_NSID: &str = "social.colibri.member";

#[derive(Serialize, Debug)]
pub struct SetMemberRolesResponse {
    pub did: String,
    pub roles: Vec<String>,
}

async fn set_member_roles_with(
    community_uri: String,
    member_did: String,
    roles: Vec<String>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<SetMemberRolesResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.setMemberRoles",
        community_uri,
        Some(Permission::RoleManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;

            // Find the member record for `member_did`.
            let member_row = record_data::Entity::find()
                .filter(record_data::Column::Did.eq(community_did))
                .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
                .filter(Expr::cust_with_values(
                    r#""record_data"."data"->>'subject' = $1"#,
                    vec![sea_orm::Value::from(member_did.clone())],
                ))
                .one(&db)
                .await?
                .ok_or_else(|| {
                    not_found_error(format!("{member_did} is not a member of this community."))
                })?;

            let mut member: ColibriMember = serde_json::from_value(member_row.data)
                .map_err(|e| invalid_request(format!("Cached member record is malformed: {e}")))?;

            // Accept AT-URIs or bare rkeys for roles.
            member.roles = roles
                .iter()
                .map(|uri| {
                    AtUri::parse(uri)
                        .map(|u| u.rkey)
                        .unwrap_or_else(|| uri.clone())
                })
                .collect();

            let new_roles = member.roles.clone();
            let data =
                serde_json::to_value(&member).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;

            community_write::put_record(&db, community_did, MEMBER_NSID, &member_row.rkey, data)
                .await?;

            Ok(Json(SetMemberRolesResponse {
                did: member_did,
                roles: new_roles,
            }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.community.setMemberRoles?<community>&<member>&<roles>&<auth>")]
/// Replaces the role set for a community member. `roles` is provided as
/// repeated query-string values (AT-URIs or bare rkeys).
pub async fn set_member_roles(
    community: &str,
    member: &str,
    roles: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<SetMemberRolesResponse>, ErrorResponse> {
    set_member_roles_with(
        community.to_string(),
        member.to_string(),
        roles,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
