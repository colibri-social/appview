use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriCategory, ColibriCommunity};
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const CATEGORY_NSID: &str = "social.colibri.category";

#[derive(Serialize, Debug)]
pub struct ReorderResponse {
    pub uri: String,
}

// ---- community.reorderChannels ---------------------------------------------

async fn reorder_channels_with(
    category_uri: String,
    channel_order: Vec<String>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<ReorderResponse>, ErrorResponse> {
    let category = AtUri::parse(&category_uri).ok_or_else(|| {
        invalid_request("Invalid category AT-URI.")
    })?;
    let community_uri = format!("at://{}/{}/{}", category.authority, COMMUNITY_NSID, COMMUNITY_RKEY);

    with_community_authz(
        auth,
        "social.colibri.community.reorderChannels",
        community_uri,
        Some(Permission::ChannelUpdate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let category_rkey = &category.rkey;

            let current = community_write::read_cached(&db, community_did, CATEGORY_NSID, category_rkey)
                .await?
                .ok_or_else(|| not_found_error("Category not found in AppView cache."))?;

            let mut rec: ColibriCategory = serde_json::from_value(current)
                .map_err(|e| invalid_request(format!("Cached category record is malformed: {e}")))?;

            // The frontend sends AT-URIs; extract just the rkey from each.
            rec.channel_order = channel_order
                .iter()
                .map(|uri| {
                    AtUri::parse(uri)
                        .map(|u| u.rkey)
                        .unwrap_or_else(|| uri.clone())
                })
                .collect();

            let data = serde_json::to_value(&rec)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, CATEGORY_NSID, category_rkey, data).await?;

            Ok(Json(ReorderResponse { uri: category_uri }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.community.reorderChannels?<category>&<channel_order>&<auth>"
)]
/// Persists a new channel order within a category. `channel_order` is
/// provided as repeated query-string values.
pub async fn reorder_channels(
    category: &str,
    channel_order: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReorderResponse>, ErrorResponse> {
    reorder_channels_with(
        category.to_string(),
        channel_order,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- community.reorderCategories -------------------------------------------

async fn reorder_categories_with(
    community_uri: String,
    category_order: Vec<String>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<ReorderResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.reorderCategories",
        community_uri.clone(),
        Some(Permission::CategoryUpdate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;

            let current = community_write::read_cached(
                &db,
                community_did,
                COMMUNITY_NSID,
                COMMUNITY_RKEY,
            )
            .await?
            .ok_or_else(|| not_found_error("Community record not found in AppView cache."))?;

            let mut community: ColibriCommunity = serde_json::from_value(current)
                .map_err(|e| invalid_request(format!("Cached community record is malformed: {e}")))?;

            // Accept either AT-URIs or bare rkeys.
            community.category_order = category_order
                .iter()
                .map(|uri| {
                    AtUri::parse(uri)
                        .map(|u| u.rkey)
                        .unwrap_or_else(|| uri.clone())
                })
                .collect();

            let data = serde_json::to_value(&community)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, COMMUNITY_NSID, COMMUNITY_RKEY, data)
                .await?;

            Ok(Json(ReorderResponse {
                uri: format!(
                    "at://{}/{}/{}",
                    community_did, COMMUNITY_NSID, COMMUNITY_RKEY
                ),
            }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.community.reorderCategories?<community>&<category_order>&<auth>"
)]
/// Persists a new category order for the community sidebar. `category_order`
/// is provided as repeated query-string values.
pub async fn reorder_categories(
    community: &str,
    category_order: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReorderResponse>, ErrorResponse> {
    reorder_categories_with(
        community.to_string(),
        category_order,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
