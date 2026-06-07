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
use crate::lib::moderation::generate_tid;
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const CATEGORY_NSID: &str = "social.colibri.category";

#[derive(Serialize, Debug)]
pub struct CategoryUriResponse {
    pub uri: String,
}

// ---- category.create -------------------------------------------------------

async fn create_category_with(
    community_uri: String,
    name: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<CategoryUriResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.category.create",
        community_uri,
        Some(Permission::CategoryCreate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let category_rkey = generate_tid();

            // Build and write the new category record.
            let category = ColibriCategory {
                r#type: CATEGORY_NSID.to_string(),
                name,
                channel_order: vec![],
                community: COMMUNITY_RKEY.to_string(),
            };
            let cat_data = serde_json::to_value(&category)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::create_record(
                &db,
                community_did,
                CATEGORY_NSID,
                Some(&category_rkey),
                cat_data,
            )
            .await?;

            // Append the new rkey to the community's categoryOrder.
            let community_data =
                community_write::read_cached(&db, community_did, COMMUNITY_NSID, COMMUNITY_RKEY)
                    .await?
                    .ok_or_else(|| {
                        not_found_error("Community record not found in AppView cache.")
                    })?;

            let mut community: ColibriCommunity =
                serde_json::from_value(community_data).map_err(|e| {
                    invalid_request(format!("Cached community record is malformed: {e}"))
                })?;
            community.category_order.push(category_rkey.clone());
            let comm_data = serde_json::to_value(&community)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(
                &db,
                community_did,
                COMMUNITY_NSID,
                COMMUNITY_RKEY,
                comm_data,
            )
            .await?;

            Ok(Json(CategoryUriResponse {
                uri: format!("at://{}/{}/{}", community_did, CATEGORY_NSID, category_rkey),
            }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.category.create?<community>&<name>&<auth>")]
pub async fn create_category(
    community: &str,
    name: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CategoryUriResponse>, ErrorResponse> {
    create_category_with(
        community.to_string(),
        name.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- category.update -------------------------------------------------------

async fn update_category_with(
    category_uri: String,
    name: Option<String>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<CategoryUriResponse>, ErrorResponse> {
    let category =
        AtUri::parse(&category_uri).ok_or_else(|| invalid_request("Invalid category AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        category.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

    with_community_authz(
        auth,
        "social.colibri.category.update",
        community_uri,
        Some(Permission::CategoryUpdate),
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

            if let Some(n) = name {
                rec.name = n;
            }

            let data =
                serde_json::to_value(&rec).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, CATEGORY_NSID, category_rkey, data)
                .await?;

            Ok(Json(CategoryUriResponse { uri: category_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.category.update?<category>&<name>&<auth>")]
pub async fn update_category(
    category: &str,
    name: Option<&str>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CategoryUriResponse>, ErrorResponse> {
    update_category_with(
        category.to_string(),
        name.map(str::to_string),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- category.delete -------------------------------------------------------

async fn delete_category_with(
    category_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<CategoryUriResponse>, ErrorResponse> {
    let category =
        AtUri::parse(&category_uri).ok_or_else(|| invalid_request("Invalid category AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        category.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

    with_community_authz(
        auth,
        "social.colibri.category.delete",
        community_uri,
        Some(Permission::CategoryDelete),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let category_rkey = &category.rkey;

            community_write::delete_record(&db, community_did, CATEGORY_NSID, category_rkey)
                .await?;

            // Remove the rkey from the community's categoryOrder.
            if let Ok(Some(community_data)) =
                community_write::read_cached(&db, community_did, COMMUNITY_NSID, COMMUNITY_RKEY)
                    .await
                && let Ok(mut community) =
                    serde_json::from_value::<ColibriCommunity>(community_data)
            {
                community.category_order.retain(|r| r != category_rkey);
                if let Ok(data) = serde_json::to_value(&community)
                    && let Err(e) = community_write::put_record(
                        &db,
                        community_did,
                        COMMUNITY_NSID,
                        COMMUNITY_RKEY,
                        data,
                    )
                    .await
                {
                    log::warn!(
                        "failed to remove deleted category {category_rkey} from \
                                 community {community_did} categoryOrder: {e}"
                    );
                }
            }

            Ok(Json(CategoryUriResponse { uri: category_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.category.delete?<category>&<auth>")]
pub async fn delete_category(
    category: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CategoryUriResponse>, ErrorResponse> {
    delete_category_with(
        category.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
