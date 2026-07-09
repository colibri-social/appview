use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriRole;
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

#[derive(Serialize, Deserialize, Debug)]
pub struct RoleChannelOverride {
    pub channel: String,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Role {
    pub uri: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    pub permissions: Vec<String>,
    pub position: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hoisted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mentionable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protected: Option<bool>,
    #[serde(rename = "channelOverrides")]
    pub channel_overrides: Vec<RoleChannelOverride>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RoleList {
    pub roles: Vec<Role>,
}

pub async fn fetch_role_records(
    db: &DatabaseConnection,
    authority: &str,
) -> Result<Vec<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.role"))
        .all(db)
        .await
}

pub fn build_roles(records: Vec<record_data::Model>, authority: &str) -> Vec<Role> {
    records
        .into_iter()
        .filter_map(|r| {
            let rkey = r.rkey.clone();
            let did = r.did.clone();
            let stored = serde_json::from_value::<ColibriRole>(r.data).ok()?;
            let channel_overrides = stored
                .channel_overrides
                .into_iter()
                .map(|o| RoleChannelOverride {
                    channel: format!("at://{}/social.colibri.channel/{}", authority, o.channel),
                    allow: o.allow,
                    deny: o.deny,
                })
                .collect();
            let permissions = if stored.protected == Some(true) {
                Permission::all_strings()
            } else {
                stored.permissions
            };
            Some(Role {
                uri: format!("at://{}/social.colibri.role/{}", did, rkey),
                name: stored.name,
                color: stored.color,
                permissions,
                position: stored.position,
                hoisted: stored.hoisted,
                mentionable: stored.mentionable,
                protected: stored.protected,
                channel_overrides,
            })
        })
        .collect()
}

type FetchRolesFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>>
    + Send
    + Sync;

async fn list_roles_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_roles_fn: &FetchRolesFn,
) -> Result<Json<RoleList>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    let records = fetch_roles_fn(db.clone(), community.authority.clone()).await?;
    crate::lib::owner_role_heal::spawn_heal(&db, &community.authority, &records);
    let roles = build_roles(records, &community.authority);

    Ok(Json(RoleList { roles }))
}

fn fetch_roles_boxed(
    db: DatabaseConnection,
    authority: String,
) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>> {
    Box::pin(async move { fetch_role_records(&db, &authority).await })
}

#[get("/xrpc/social.colibri.community.listRoles?<community>")]
/// Lists all roles cached for a Colibri community.
pub async fn list_roles(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<RoleList>, ErrorResponse> {
    list_roles_with(
        community.to_string(),
        db.inner().clone(),
        &fetch_roles_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn maps_records_to_role_response() {
        let db = mock_db();
        let result = list_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            &|_, _| {
                Box::pin(async {
                    Ok(vec![record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.role"),
                        rkey: String::from("owner"),
                        data: serde_json::json!({
                            "name": "Owner",
                            "permissions": ["member.ban", "message.hide"],
                            "position": 100,
                            "protected": true,
                            "channelOverrides": []
                        }),
                        indexed_at: String::from(""),
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.roles.len(), 1);
        assert_eq!(
            result.roles[0].uri,
            "at://did:plc:owner/social.colibri.role/owner"
        );
        assert_eq!(result.roles[0].name, "Owner");
        assert_eq!(result.roles[0].position, 100);
        assert_eq!(result.roles[0].protected, Some(true));
        assert_eq!(result.roles[0].permissions, Permission::all_strings());
    }

    #[tokio::test]
    async fn non_protected_role_keeps_its_stored_permissions() {
        let db = mock_db();
        let result = list_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            &|_, _| {
                Box::pin(async {
                    Ok(vec![record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.role"),
                        rkey: String::from("mod"),
                        data: serde_json::json!({
                            "name": "Moderator",
                            "permissions": ["member.ban", "message.hide"],
                            "position": 50,
                            "channelOverrides": []
                        }),
                        indexed_at: String::from(""),
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.roles[0].protected, None);
        assert_eq!(
            result.roles[0].permissions,
            vec!["member.ban", "message.hide"]
        );
    }

    #[tokio::test]
    async fn expands_channel_override_rkeys_to_uris() {
        let db = mock_db();
        let result = list_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            &|_, _| {
                Box::pin(async {
                    Ok(vec![record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.role"),
                        rkey: String::from("mod"),
                        data: serde_json::json!({
                            "name": "Moderator",
                            "permissions": [],
                            "position": 50,
                            "channelOverrides": [
                                { "channel": "chan-a", "allow": ["message.hide"], "deny": [] }
                            ]
                        }),
                        indexed_at: String::from(""),
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.roles[0].channel_overrides.len(), 1);
        assert_eq!(
            result.roles[0].channel_overrides[0].channel,
            "at://did:plc:owner/social.colibri.channel/chan-a"
        );
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = list_roles_with(String::from("not-a-uri"), db, &|_, _| {
            Box::pin(async { panic!("should not fetch when uri is invalid") })
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn skips_records_with_invalid_payload() {
        let db = mock_db();
        let result = list_roles_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            &|_, _| {
                Box::pin(async {
                    Ok(vec![
                        record_data::Model {
                            id: 1,
                            did: String::from("did:plc:owner"),
                            nsid: String::from("social.colibri.role"),
                            rkey: String::from("bad"),
                            data: serde_json::json!({ "irrelevant": true }),
                            indexed_at: String::from(""),
                        },
                        record_data::Model {
                            id: 2,
                            did: String::from("did:plc:owner"),
                            nsid: String::from("social.colibri.role"),
                            rkey: String::from("mod"),
                            data: serde_json::json!({
                                "name": "Moderator",
                                "permissions": [],
                                "position": 50
                            }),
                            indexed_at: String::from(""),
                        },
                    ])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.roles.len(), 1);
        assert_eq!(result.roles[0].name, "Moderator");
    }
}
