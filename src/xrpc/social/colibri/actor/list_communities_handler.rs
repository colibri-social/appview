use std::collections::{HashMap, HashSet};

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{get, State};
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QueryFilter,
    QuerySelect,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::colibri::{ColibriActorData, ColibriMember};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use crate::models::record_data;

const ACTOR_DATA_NSID: &str = "social.colibri.actor.data";

const MEMBER_NSID: &str = "social.colibri.member";
const ROLE_NSID: &str = "social.colibri.role";

#[derive(Serialize, Deserialize, Debug)]
pub struct Community {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "categoryOrder", skip_serializing_if = "Option::is_none")]
    pub category_order: Option<Vec<String>>,
    #[serde(rename = "requiresApprovalToJoin")]
    pub requires_approval_to_join: bool,
    pub uri: Option<String>,
    #[serde(rename = "isLegacy")]
    pub is_legacy: Option<bool>,
    /// Whether the authenticated caller owns this community (holds a protected
    /// role, or hosts the community on their own repo). Lets clients gate
    /// owner-only affordances (e.g. hiding "Leave") without a follow-up fetch.
    #[serde(rename = "isOwner")]
    pub is_owner: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct CommunityList {
    pub communities: Vec<Community>,
}

/// One authorized community plus the per-caller flags the handler derives.
pub struct CommunityExtended {
    pub community: record_data::Model,
    pub is_legacy: bool,
    pub is_owner: bool,
}

/// Raw query row: a community record + whether it's legacy (derived from rkey).
#[derive(FromQueryResult)]
struct CommunityRow {
    #[sea_orm(nested)]
    community: record_data::Model,
    is_legacy: bool,
}

pub async fn get_authorized_communities(
    db: &DatabaseConnection,
    user_did: &str,
) -> Result<Vec<CommunityExtended>, DbErr> {
    // Three OR branches describe "the caller is in this community":
    //
    // 1. `record_data.did = caller` — the community lives on the caller's
    //    own repo. Covers legacy communities they host and any self-hosted
    //    edge cases.
    //
    // 2. There's a `social.colibri.member` record on the community's repo
    //    with `subject = caller`. The Variant A native check — owners get
    //    this from the bootstrap flow, regular members from auto-admit on
    //    open communities or `approveMembership` on closed ones.
    //
    // 3. Legacy-only fallback: for non-`self` community rkeys (i.e. legacy
    //    communities that live as one of many records on a user repo), fall
    //    back to checking for a `social.colibri.membership` record on the
    //    caller's repo pointing at this community. Necessary because the
    //    AppView never writes `member` records to legacy communities (no
    //    PDS credentials for them), so pre-existing memberships would
    //    otherwise be invisible.
    //
    //    Gating the membership-only path behind `rkey <> 'self'` is
    //    deliberate: for Variant A communities, we want closed-community
    //    pending applications (membership present, member absent) to stay
    //    hidden, which only branch 2 enforces.
    //
    // `is_legacy` is derived purely from the rkey.
    let rows = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.community"))
        .filter(
            Condition::any()
                .add(record_data::Column::Did.eq(user_did))
                .add(Expr::cust_with_values(
                    r#"
                    EXISTS (
                        SELECT 1 FROM record_data m
                        WHERE "m"."nsid" = 'social.colibri.member'
                          AND "m"."did" = "record_data"."did"
                          AND "m"."data"->>'subject' = $1
                    )
                    "#,
                    vec![sea_orm::Value::from(user_did)]
                ))
                .add(Expr::cust_with_values(
                    r#"
                    "record_data"."rkey" <> 'self'
                    AND EXISTS (
                        SELECT 1 FROM record_data m
                        WHERE "m"."nsid" = 'social.colibri.membership'
                          AND "m"."did" = $1
                          AND "m"."data"->>'community' =
                              'at://' || "record_data"."did" || '/social.colibri.community/' || "record_data"."rkey"
                    )
                    "#,
                    vec![sea_orm::Value::from(user_did)]
                ))
        )
        .column_as(
            Expr::cust(r#""record_data"."rkey" <> 'self'"#),
            "is_legacy",
        )
        .into_model::<CommunityRow>()
        .all(db)
        .await?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    Ok(attach_ownership(db, user_did, rows).await?)
}

/// Resolves, for each authorized community, whether the caller is an owner —
/// i.e. they host the community on their own repo, or they hold a protected
/// ("Owner") role in it. Member role assignments are stored as role rkeys, and
/// the protected flag lives on the role record, so we intersect the two per
/// community DID. Two batched queries cover every community at once.
async fn attach_ownership(
    db: &DatabaseConnection,
    user_did: &str,
    rows: Vec<CommunityRow>,
) -> Result<Vec<CommunityExtended>, DbErr> {
    let community_dids: Vec<String> = rows
        .iter()
        .map(|r| r.community.did.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // The caller's role rkeys per community DID, from their member record.
    let member_records = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(record_data::Column::Did.is_in(community_dids.clone()))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(user_did)],
        ))
        .all(db)
        .await?;

    let mut roles_by_did: HashMap<String, HashSet<String>> = HashMap::new();
    for record in member_records {
        let did = record.did.clone();
        if let Ok(member) = serde_json::from_value::<ColibriMember>(record.data) {
            roles_by_did.entry(did).or_default().extend(member.roles);
        }
    }

    // The protected (owner) role rkeys per community DID.
    let protected_records = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(ROLE_NSID))
        .filter(record_data::Column::Did.is_in(community_dids))
        .filter(Expr::cust(r#""record_data"."data"->>'protected' = 'true'"#))
        .all(db)
        .await?;

    let mut protected_by_did: HashMap<String, HashSet<String>> = HashMap::new();
    for record in protected_records {
        protected_by_did
            .entry(record.did.clone())
            .or_default()
            .insert(record.rkey);
    }

    Ok(rows
        .into_iter()
        .map(|row| {
            let did = &row.community.did;
            let holds_protected = match (roles_by_did.get(did), protected_by_did.get(did)) {
                (Some(held), Some(protected)) => held.intersection(protected).next().is_some(),
                _ => false,
            };
            let is_owner = row.community.did == user_did || holds_protected;
            CommunityExtended {
                community: row.community,
                is_legacy: row.is_legacy,
                is_owner,
            }
        })
        .collect())
}

/// The community DID embedded in an `at://{did}/{nsid}/{rkey}` URI.
fn community_did(uri: &Option<String>) -> Option<&str> {
    uri.as_deref()?.split('/').nth(2)
}

/// Reorders `communities` to match the caller's saved sidebar order. `order`
/// is the `communities` array from the caller's `social.colibri.actor.data`
/// record — a list of community DIDs. Communities not present in `order` (e.g.
/// just joined, before the client has persisted a new order) sort to the end,
/// keeping their original relative order. Ordering is a best-effort client
/// preference, so an empty or partial `order` is fine.
fn order_communities(communities: Vec<Community>, order: &[String]) -> Vec<Community> {
    if order.is_empty() {
        return communities;
    }

    let rank: HashMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(i, did)| (did.as_str(), i))
        .collect();

    let mut indexed: Vec<(usize, usize, Community)> = communities
        .into_iter()
        .enumerate()
        .map(|(original, community)| {
            let position = community_did(&community.uri)
                .and_then(|did| rank.get(did).copied())
                .unwrap_or(usize::MAX);
            (position, original, community)
        })
        .collect();

    // Sort by saved position, breaking ties by original index so the result is
    // stable for communities sharing a rank (unknown ones, or legacy ones that
    // share a DID).
    indexed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    indexed
        .into_iter()
        .map(|(_, _, community)| community)
        .collect()
}

type VerifyAuthFn =
    fn(
        String,
        String,
    ) -> BoxFuture<'static, Result<String, crate::lib::service_auth::ServiceAuthError>>;
type GetCommunitiesFn =
    fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<CommunityExtended>, DbErr>>;
type GetOrderFn = fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<String>, DbErr>>;

async fn list_communities_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: VerifyAuthFn,
    get_communities_fn: GetCommunitiesFn,
    get_order_fn: GetOrderFn,
) -> Result<Json<CommunityList>, ErrorResponse> {
    let did = verify_auth_fn(auth, String::from("social.colibri.actor.listCommunities"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let order = get_order_fn(db.clone(), did.clone()).await?;
    let community_models = get_communities_fn(db, did).await?;

    let communities: Vec<Community> = community_models
        .iter()
        .filter_map(|c| {
            let mut c_data = serde_json::from_value::<Community>(c.community.data.clone())
                .map_err(|e| {
                    log::warn!(
                        "skipping malformed community record {}/{}: {e}",
                        c.community.did,
                        c.community.rkey
                    )
                })
                .ok()?;
            let uri = format!(
                "at://{}/{}/{}",
                c.community.did, c.community.nsid, c.community.rkey
            );

            c_data.uri = Some(uri);
            c_data.is_legacy = Some(c.is_legacy);
            c_data.is_owner = Some(c.is_owner);

            Some(c_data)
        })
        .collect();

    let communities = order_communities(communities, &order);

    Ok(Json(CommunityList { communities }))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, crate::lib::service_auth::ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn get_authorized_communities_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<Vec<CommunityExtended>, DbErr>> {
    Box::pin(async move { get_authorized_communities(&db, &did).await })
}

/// Fetches the caller's saved community order from their `actor.data` record.
/// A missing record (user never persisted an order) is not an error — it just
/// means "no preference", so the communities keep their default order.
fn get_community_order_boxed(
    db: DatabaseConnection,
    did: String,
) -> BoxFuture<'static, Result<Vec<String>, DbErr>> {
    Box::pin(async move {
        match get_atproto_record::<ColibriActorData>(
            did,
            String::from(ACTOR_DATA_NSID),
            String::from("self"),
            &db,
        )
        .await
        {
            Ok(data) => Ok(data.communities),
            Err(DbErr::RecordNotFound(_)) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    })
}

#[get("/xrpc/social.colibri.actor.listCommunities?<auth>")]
/// Returns the actor data for a specified identity.
pub async fn list_communities(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CommunityList>, ErrorResponse> {
    list_communities_with(
        auth.to_string(),
        db.inner().clone(),
        verify_auth_boxed,
        get_authorized_communities_boxed,
        get_community_order_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn maps_community_models_to_response() {
        let db = mock_db();

        let res = list_communities_with(
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:me")) }),
            |_, _| {
                Box::pin(async {
                    Ok(vec![CommunityExtended {
                        community: record_data::Model {
                            id: 1,
                            did: String::from("did:plc:abc"),
                            nsid: String::from("social.colibri.community"),
                            rkey: String::from("community-1"),
                            data: serde_json::json!({
                                "name": "General",
                                "requiresApprovalToJoin": false,
                                "description": "desc",
                                "categoryOrder": ["cat1"]
                            }),
                            indexed_at: String::from(""),
                        },
                        is_legacy: true,
                        is_owner: true,
                    }])
                })
            },
            |_, _| Box::pin(async { Ok(vec![]) }),
        )
        .await
        .unwrap();

        assert_eq!(res.communities.len(), 1);
        assert_eq!(
            res.communities[0].uri.as_deref(),
            Some("at://did:plc:abc/social.colibri.community/community-1")
        );
        assert_eq!(res.communities[0].is_legacy, Some(true));
        assert_eq!(res.communities[0].is_owner, Some(true));
    }

    #[tokio::test]
    async fn returns_auth_error_when_token_is_invalid() {
        let db = mock_db();
        let result = list_communities_with(
            String::from("token"),
            db,
            |_, _| {
                Box::pin(async {
                    Err(crate::lib::service_auth::ServiceAuthError::InvalidSignature)
                })
            },
            |_, _| Box::pin(async { panic!("should not fetch communities") }),
            |_, _| Box::pin(async { panic!("should not fetch order") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }

    fn community(did: &str, name: &str) -> Community {
        Community {
            name: String::from(name),
            picture: None,
            description: None,
            category_order: None,
            requires_approval_to_join: false,
            uri: Some(format!("at://{did}/social.colibri.community/self")),
            is_legacy: Some(false),
            is_owner: Some(false),
        }
    }

    #[test]
    fn order_communities_follows_saved_order() {
        let communities = vec![
            community("did:plc:a", "A"),
            community("did:plc:b", "B"),
            community("did:plc:c", "C"),
        ];
        // Saved order pulls C to the front, then B, then A.
        let order = vec![
            String::from("did:plc:c"),
            String::from("did:plc:b"),
            String::from("did:plc:a"),
        ];

        let ordered = order_communities(communities, &order);
        let dids: Vec<&str> = ordered
            .iter()
            .filter_map(|c| community_did(&c.uri))
            .collect();
        assert_eq!(dids, vec!["did:plc:c", "did:plc:b", "did:plc:a"]);
    }

    #[test]
    fn order_communities_appends_unsaved_to_end_in_original_order() {
        let communities = vec![
            community("did:plc:a", "A"),
            community("did:plc:b", "B"),
            community("did:plc:c", "C"),
        ];
        // Only B is in the saved order; A and C keep their relative order after it.
        let order = vec![String::from("did:plc:b")];

        let ordered = order_communities(communities, &order);
        let dids: Vec<&str> = ordered
            .iter()
            .filter_map(|c| community_did(&c.uri))
            .collect();
        assert_eq!(dids, vec!["did:plc:b", "did:plc:a", "did:plc:c"]);
    }

    #[test]
    fn order_communities_empty_order_is_identity() {
        let communities = vec![community("did:plc:a", "A"), community("did:plc:b", "B")];
        let ordered = order_communities(communities, &[]);
        let dids: Vec<&str> = ordered
            .iter()
            .filter_map(|c| community_did(&c.uri))
            .collect();
        assert_eq!(dids, vec!["did:plc:a", "did:plc:b"]);
    }
}
