use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriCommunity;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

use super::list_categories_handler::{Category, fetch_category_records};
use super::list_channels_handler::{Channel, fetch_channel_records};
use super::list_members_handler::{Member, MemberAggregate, build_member, fetch_member_aggregate};
use super::list_roles_handler::{Role, build_roles, fetch_role_records};

#[derive(Serialize, Debug)]
pub struct CommunityInfo {
    pub uri: String,
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<Value>,
    #[serde(rename = "categoryOrder")]
    pub category_order: Vec<String>,
    #[serde(rename = "requiresApprovalToJoin")]
    pub requires_approval_to_join: bool,
}

#[derive(Serialize, Debug)]
pub struct CommunityDataResponse {
    pub community: CommunityInfo,
    pub categories: Vec<Category>,
    pub channels: Vec<Channel>,
    pub roles: Vec<Role>,
    pub members: Vec<Member>,
    pub did: String,
}

pub struct RawCommunityData {
    pub community_record: Option<record_data::Model>,
    pub category_records: Vec<record_data::Model>,
    pub channel_records: Vec<record_data::Model>,
    pub role_records: Vec<record_data::Model>,
    pub member_aggregate: MemberAggregate,
}

pub async fn fetch_raw_community_data(
    db: &DatabaseConnection,
    community_uri: &str,
) -> Result<RawCommunityData, DbErr> {
    let community = AtUri::parse(community_uri)
        .ok_or_else(|| DbErr::Custom(format!("invalid community AT-URI: {community_uri}")))?;

    let community_record = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.community"))
        .filter(record_data::Column::Rkey.eq(&community.rkey))
        .one(db)
        .await?;

    let category_records =
        fetch_category_records(db, &community.authority, &community.rkey).await?;
    let channel_records = fetch_channel_records(db, &community.authority, &community.rkey).await?;
    let role_records = fetch_role_records(db, &community.authority).await?;
    let member_aggregate = fetch_member_aggregate(db, community_uri).await?;

    Ok(RawCommunityData {
        community_record,
        category_records,
        channel_records,
        role_records,
        member_aggregate,
    })
}

#[derive(Deserialize)]
struct StoredCategory {
    name: String,
    #[serde(rename = "channelOrder", default)]
    channel_order: Vec<String>,
}

#[derive(Deserialize)]
struct StoredChannel {
    name: String,
    #[serde(rename = "type")]
    channel_type: String,
    category: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(rename = "ownerOnly", default)]
    owner_only: Option<bool>,
    #[serde(rename = "allowedRoles", default)]
    allowed_roles: Vec<String>,
    #[serde(rename = "allowedMembers", default)]
    allowed_members: Vec<String>,
}

type FetchDataFn =
    fn(DatabaseConnection, String) -> BoxFuture<'static, Result<RawCommunityData, DbErr>>;

async fn get_data_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_data_fn: FetchDataFn,
) -> Result<Json<CommunityDataResponse>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    let raw = fetch_data_fn(db, community_uri).await?;

    let community_record = raw.community_record.ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("NotFound"),
            message: String::from("Community not found."),
        }),
    })?;

    let stored_community = serde_json::from_value::<ColibriCommunity>(community_record.data)
        .map_err(|_| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InternalError"),
                message: String::from("Failed to parse community record."),
            }),
        })?;

    let category_order = stored_community
        .category_order
        .into_iter()
        .map(|rkey| {
            format!(
                "at://{}/social.colibri.category/{}",
                community.authority, rkey
            )
        })
        .collect();

    let community_info = CommunityInfo {
        uri: format!(
            "at://{}/social.colibri.community/{}",
            community_record.did, community_record.rkey
        ),
        name: stored_community.name,
        description: stored_community.description,
        picture: stored_community.picture,
        category_order,
        requires_approval_to_join: stored_community.requires_approval_to_join,
    };

    let categories = raw
        .category_records
        .into_iter()
        .filter_map(|r| {
            let stored = serde_json::from_value::<StoredCategory>(r.data).ok()?;
            let channel_order = stored
                .channel_order
                .into_iter()
                .map(|rkey| {
                    format!(
                        "at://{}/social.colibri.channel/{}",
                        community.authority, rkey
                    )
                })
                .collect();
            Some(Category {
                uri: format!("at://{}/{}/{}", r.did, r.nsid, r.rkey),
                name: stored.name,
                channel_order,
            })
        })
        .collect();

    let channels = raw
        .channel_records
        .into_iter()
        .filter_map(|r| {
            let stored = serde_json::from_value::<StoredChannel>(r.data).ok()?;
            Some(Channel {
                uri: format!("at://{}/{}/{}", r.did, r.nsid, r.rkey),
                name: stored.name,
                channel_type: stored.channel_type,
                category: format!(
                    "at://{}/social.colibri.category/{}",
                    community.authority, stored.category
                ),
                description: stored.description,
                owner_only: stored.owner_only,
                // Stored as bare rkeys; the client addresses roles by AT-URI.
                allowed_roles: stored
                    .allowed_roles
                    .into_iter()
                    .map(|rkey| {
                        format!("at://{}/social.colibri.role/{}", community.authority, rkey)
                    })
                    .collect(),
                allowed_members: stored.allowed_members,
            })
        })
        .collect();

    let roles = build_roles(raw.role_records, &community.authority);

    let MemberAggregate {
        member_dids,
        mut profiles,
        mut colibri_profiles,
        mut actor_data,
        mut states,
        mut handles,
        mut member_roles,
    } = raw.member_aggregate;

    let members = member_dids
        .into_iter()
        .map(|did| {
            let handle = handles.remove(&did);
            let profile = profiles.remove(&did);
            let colibri_profile = colibri_profiles.remove(&did);
            let data = actor_data.remove(&did);
            let state = states.remove(&did);
            let role_rkeys = member_roles.remove(&did).unwrap_or_default();
            build_member(
                did,
                handle,
                profile,
                colibri_profile,
                data,
                state,
                &community.authority,
                role_rkeys,
            )
        })
        .collect();

    Ok(Json(CommunityDataResponse {
        community: community_info,
        categories,
        channels,
        roles,
        members,
        did: community.authority,
    }))
}

fn fetch_data_boxed(
    db: DatabaseConnection,
    community_uri: String,
) -> BoxFuture<'static, Result<RawCommunityData, DbErr>> {
    Box::pin(async move { fetch_raw_community_data(&db, &community_uri).await })
}

#[get("/xrpc/social.colibri.community.getData?<community>")]
/// Returns the community record, categories, channels, roles, and members in a
/// single response.
pub async fn get_data(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CommunityDataResponse>, ErrorResponse> {
    get_data_with(community.to_string(), db.inner().clone(), fetch_data_boxed).await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    fn community_model() -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from("did:plc:owner"),
            nsid: String::from("social.colibri.community"),
            rkey: String::from("self"),
            data: serde_json::json!({
                "$type": "social.colibri.community",
                "name": "Test Community",
                "description": "A test community",
                "categoryOrder": ["cat1"],
                "requiresApprovalToJoin": false
            }),
            indexed_at: String::from(""),
        }
    }

    fn raw_data_with_community(community: Option<record_data::Model>) -> RawCommunityData {
        RawCommunityData {
            community_record: community,
            category_records: vec![record_data::Model {
                id: 2,
                did: String::from("did:plc:owner"),
                nsid: String::from("social.colibri.category"),
                rkey: String::from("cat1"),
                data: serde_json::json!({
                    "name": "General",
                    "channelOrder": ["chan-a"],
                    "community": "self"
                }),
                indexed_at: String::from(""),
            }],
            channel_records: vec![record_data::Model {
                id: 3,
                did: String::from("did:plc:owner"),
                nsid: String::from("social.colibri.channel"),
                rkey: String::from("chan-a"),
                data: serde_json::json!({
                    "name": "general",
                    "type": "social.colibri.channel.text",
                    "category": "cat1",
                    "community": "self"
                }),
                indexed_at: String::from(""),
            }],
            role_records: vec![record_data::Model {
                id: 4,
                did: String::from("did:plc:owner"),
                nsid: String::from("social.colibri.role"),
                rkey: String::from("owner"),
                data: serde_json::json!({
                    "name": "Owner",
                    "permissions": [],
                    "position": 100,
                    "protected": true
                }),
                indexed_at: String::from(""),
            }],
            member_aggregate: MemberAggregate {
                member_dids: vec![String::from("did:plc:alice")],
                profiles: HashMap::new(),
                colibri_profiles: HashMap::new(),
                actor_data: HashMap::new(),
                states: HashMap::new(),
                handles: HashMap::from([(
                    String::from("did:plc:alice"),
                    String::from("alice.test"),
                )]),
                member_roles: HashMap::new(),
            },
        }
    }

    #[tokio::test]
    async fn assembles_full_community_data() {
        let db = mock_db();
        let result = get_data_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            db,
            |_, _| Box::pin(async { Ok(raw_data_with_community(Some(community_model()))) }),
        )
        .await
        .unwrap();

        assert_eq!(result.community.name, "Test Community");
        assert_eq!(
            result.community.uri,
            "at://did:plc:owner/social.colibri.community/self"
        );
        assert_eq!(
            result.community.category_order,
            vec!["at://did:plc:owner/social.colibri.category/cat1"]
        );
        assert_eq!(result.categories.len(), 1);
        assert_eq!(result.categories[0].name, "General");
        assert_eq!(result.channels.len(), 1);
        assert_eq!(result.channels[0].name, "general");
        assert_eq!(result.roles.len(), 1);
        assert_eq!(result.roles[0].name, "Owner");
        assert_eq!(result.members.len(), 1);
        assert_eq!(result.members[0].handle, "alice.test");
    }

    #[tokio::test]
    async fn returns_not_found_when_community_record_missing() {
        let db = mock_db();
        let result = get_data_with(
            String::from("at://did:plc:owner/social.colibri.community/self"),
            db,
            |_, _| Box::pin(async { Ok(raw_data_with_community(None)) }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = get_data_with(String::from("not-a-uri"), db, |_, _| {
            Box::pin(async { panic!("should not fetch when uri is invalid") })
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
