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
    record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.community"))
        .filter(
            Condition::any()
                .add(record_data::Column::Did.eq(user_did))
                .add(Expr::cust_with_values(
                    r#"
                    EXISTS (
                        SELECT 1 FROM record_data m
                        WHERE "m"."nsid" = 'social.colibri.membership'
                          AND "m"."did" = $1
                          AND "m"."data"->>'community' = 'at://' || "record_data"."did" || '/social.colibri.community/' || "record_data"."rkey"
                          AND (
                              ("record_data"."data"->>'requiresApprovalToJoin')::boolean IS NOT TRUE
                              OR EXISTS (
                                  SELECT 1 FROM record_data a
                                  WHERE "a"."nsid" = 'social.colibri.approval'
                                    AND "a"."data" @> jsonb_build_object(
                                        'membership', 'at://' || "m"."did" || '/social.colibri.membership/' || "m"."rkey"
                                    )
                              )
                          )
                    )
                    "#,
                    vec![sea_orm::Value::from(user_did)]
                ))
        )
        // Add the subquery for the legacy check
        .column_as(
            Expr::cust(
                r#"
                NOT EXISTS (
                    SELECT 1 FROM record_data p
                    WHERE "p"."nsid" = 'app.bsky.actor.profile'
                      AND "p"."did" = "record_data"."did"
                      AND "p"."data" @> '{"labels": {"values": [{"val": "bot"}]}}'::jsonb
                )
                "#
            ),
            "is_legacy",
        )
        // Map the result to the custom struct
        .into_model::<CommunityExtended>()
        .all(db)
        .await
}

#[get("/xrpc/social.colibri.actor.listCommunities?<auth>")]
/// Returns the actor data for a specified identity.
pub async fn list_communities(
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CommunityList>, ErrorResponse> {
    let did = service_auth::verify_service_auth(auth, "social.colibri.actor.listCommunities")
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let community_models = get_authorized_communities(db, did.as_str()).await?;

    let communities: Vec<Community> = community_models
        .iter()
        .map(|c| {
            let mut c_data = serde_json::from_value::<Community>(c.community.data.clone()).unwrap();
            let uri = format!(
                "at://{}/{}/{}",
                c.community.did, c.community.nsid, c.community.rkey
            );

            c_data.uri = Some(uri);
            c_data.is_legacy = Some(c.is_legacy);

            c_data
        })
        .collect();

    Ok(Json(CommunityList {
        communities: communities,
    }))
}
