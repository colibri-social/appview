use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QueryFilter,
    QuerySelect,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use crate::models::record_data;

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
}

#[derive(Serialize, Deserialize)]
pub struct CommunityList {
    pub communities: Vec<Community>,
}

#[derive(FromQueryResult)]
pub struct CommunityExtended {
    #[sea_orm(nested)]
    pub community: record_data::Model,
    pub is_legacy: bool,
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
    record_data::Entity::find()
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
        .into_model::<CommunityExtended>()
        .all(db)
        .await
}

type VerifyAuthFn =
    fn(
        String,
        String,
    ) -> BoxFuture<'static, Result<String, crate::lib::service_auth::ServiceAuthError>>;
type GetCommunitiesFn =
    fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<CommunityExtended>, DbErr>>;

async fn list_communities_with(
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: VerifyAuthFn,
    get_communities_fn: GetCommunitiesFn,
) -> Result<Json<CommunityList>, ErrorResponse> {
    let did = verify_auth_fn(auth, String::from("social.colibri.actor.listCommunities"))
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

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

            Some(c_data)
        })
        .collect();

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
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(res.communities.len(), 1);
        assert_eq!(
            res.communities[0].uri.as_deref(),
            Some("at://did:plc:abc/social.colibri.community/community-1")
        );
        assert_eq!(res.communities[0].is_legacy, Some(true));
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
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
