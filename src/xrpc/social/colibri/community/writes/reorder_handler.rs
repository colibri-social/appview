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

/// Clients address records by AT-URI, but order arrays store bare rkeys. A value
/// that is neither is rejected rather than stored verbatim — writing it through
/// would corrupt the order with something no read path can resolve.
fn normalize_order(values: &[String], label: &str) -> Result<Vec<String>, ErrorResponse> {
    values
        .iter()
        .map(|value| match AtUri::parse(value) {
            Some(uri) => Ok(uri.rkey),
            None if !value.is_empty() && !value.contains('/') => Ok(value.clone()),
            None => Err(invalid_request(format!(
                "{label} contains an entry that is neither an AT-URI nor a record key: {value}"
            ))),
        })
        .collect()
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
    let category =
        AtUri::parse(&category_uri).ok_or_else(|| invalid_request("Invalid category AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        category.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

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

            let current =
                community_write::read_cached(&db, community_did, CATEGORY_NSID, category_rkey)
                    .await?
                    .ok_or_else(|| not_found_error("Category not found in AppView cache."))?;

            let mut rec: ColibriCategory = serde_json::from_value(current).map_err(|e| {
                invalid_request(format!("Cached category record is malformed: {e}"))
            })?;

            let next_order = normalize_order(&channel_order, "channelOrder")?;
            // Rocket's lenient `Vec<String>` query guard yields an empty vec when
            // the params are missing entirely, which would silently wipe a real
            // order and report success.
            if next_order.is_empty() && !rec.channel_order.is_empty() {
                return Err(invalid_request(
                    "channelOrder must not be empty for a category that has channels.",
                ));
            }
            rec.channel_order = next_order;

            let data =
                serde_json::to_value(&rec).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, CATEGORY_NSID, category_rkey, data)
                .await?;

            Ok(Json(ReorderResponse { uri: category_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.community.reorderChannels?<category>&<channelOrder>&<auth>")]
/// Persists a new channel order within a category. `channelOrder` is
/// provided as repeated query-string values.
#[allow(non_snake_case)]
pub async fn reorder_channels(
    category: &str,
    channelOrder: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReorderResponse>, ErrorResponse> {
    reorder_channels_with(
        category.to_string(),
        channelOrder,
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

            let current =
                community_write::read_cached(&db, community_did, COMMUNITY_NSID, COMMUNITY_RKEY)
                    .await?
                    .ok_or_else(|| {
                        not_found_error("Community record not found in AppView cache.")
                    })?;

            let mut community: ColibriCommunity = serde_json::from_value(current).map_err(|e| {
                invalid_request(format!("Cached community record is malformed: {e}"))
            })?;

            let next_order = normalize_order(&category_order, "categoryOrder")?;
            if next_order.is_empty() && !community.category_order.is_empty() {
                return Err(invalid_request(
                    "categoryOrder must not be empty for a community that has categories.",
                ));
            }
            community.category_order = next_order;

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

#[post("/xrpc/social.colibri.community.reorderCategories?<community>&<categoryOrder>&<auth>")]
/// Persists a new category order for the community sidebar. `categoryOrder`
/// is provided as repeated query-string values.
#[allow(non_snake_case)]
pub async fn reorder_categories(
    community: &str,
    categoryOrder: Vec<String>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReorderResponse>, ErrorResponse> {
    reorder_categories_with(
        community.to_string(),
        categoryOrder,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
