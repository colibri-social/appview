use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriMember, ColibriRole};
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;
use crate::xrpc::social::colibri::community::reads::list_roles_handler::fetch_role_records;

const MEMBER_NSID: &str = "social.colibri.member";

async fn highest_position_among(
    db: &DatabaseConnection,
    community_did: &str,
    rkeys: &[String],
) -> Result<Option<i64>, DbErr> {
    if rkeys.is_empty() {
        return Ok(None);
    }
    let records = fetch_role_records(db, community_did).await?;
    Ok(records
        .into_iter()
        .filter(|r| rkeys.contains(&r.rkey))
        .filter_map(|r| serde_json::from_value::<ColibriRole>(r.data).ok())
        .map(|role| role.position)
        .max())
}

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
            let previously_held_rkeys = member.roles.clone();

            member.roles = roles
                .iter()
                .map(|uri| {
                    AtUri::parse(uri)
                        .map(|u| u.rkey)
                        .unwrap_or_else(|| uri.clone())
                })
                .collect();

            let newly_granted_rkeys: Vec<String> = member
                .roles
                .iter()
                .filter(|rkey| !previously_held_rkeys.contains(rkey))
                .cloned()
                .collect();
            if let Some(max_position) =
                highest_position_among(&db, community_did, &newly_granted_rkeys).await?
                && !ctx.authz.outranks_position(max_position)
            {
                return Err(ErrorResponse {
                    body: Json(ErrorBody {
                        error: String::from("Forbidden"),
                        message: String::from(
                            "Cannot assign a role at or above your own position.",
                        ),
                    }),
                });
            }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::{member, role};
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn member_record(subject: &str, roles: Vec<&str>) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from("did:plc:owner"),
            nsid: String::from(MEMBER_NSID),
            rkey: String::from("member-1"),
            data: serde_json::to_value(member(subject, roles)).unwrap(),
            indexed_at: String::new(),
        }
    }

    fn role_record(rkey: &str, position: i64) -> record_data::Model {
        record_data::Model {
            id: 2,
            did: String::from("did:plc:owner"),
            nsid: String::from("social.colibri.role"),
            rkey: String::from(rkey),
            data: serde_json::to_value(role("Admin", position, vec![])).unwrap(),
            indexed_at: String::new(),
        }
    }

    fn caller_authz() -> ActorAuthz {
        ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:caller", vec!["mod"])),
            roles: vec![role("Moderator", 10, vec![Permission::RoleManage])],
        }
    }

    #[tokio::test]
    async fn set_member_roles_rejects_granting_own_position_role_to_other_member() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![member_record("did:plc:other", vec![])]])
            .append_query_results([vec![role_record("mod", 10)]])
            .into_connection();

        let result = set_member_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("did:plc:other"),
            vec![String::from("mod")],
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| Box::pin(async { Ok(caller_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }

    #[tokio::test]
    async fn set_member_roles_rejects_self_escalation() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![member_record("did:plc:caller", vec!["mod"])]])
            .append_query_results([vec![role_record("admin", 50)]])
            .into_connection();

        let result = set_member_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("did:plc:caller"),
            vec![String::from("admin")],
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| Box::pin(async { Ok(caller_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }

    #[tokio::test]
    async fn set_member_roles_allows_self_edit_within_ceiling() {
        let _ = crate::lib::crypto::install_master_key(vec![0u8; 32]);

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![member_record("did:plc:caller", vec!["mod"])]])
            .append_query_results([vec![role_record("mod", 10), role_record("helper", 5)]])
            .into_connection();

        let result = set_member_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("did:plc:caller"),
            vec![String::from("mod"), String::from("helper")],
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| Box::pin(async { Ok(caller_authz()) }),
        )
        .await;

        if let Err(e) = result {
            let body = e.body.into_inner();
            assert!(
                !body.message.contains("at or above your own position"),
                "self-edit within ceiling was wrongly rejected: {}",
                body.message
            );
        }
    }

    #[tokio::test]
    async fn set_member_roles_rejects_when_new_role_outranks_caller() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![member_record("did:plc:target", vec![])]])
            .append_query_results([vec![role_record("admin", 50)]])
            .into_connection();

        let result = set_member_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            String::from("did:plc:target"),
            vec![String::from("admin")],
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:caller")) }),
            &|_, _, _| Box::pin(async { Ok(caller_authz()) }),
        )
        .await;

        let body = result.expect_err("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("at or above your own position"));
    }
}
