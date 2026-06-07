use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriCategory, ColibriChannel};
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
const CHANNEL_NSID: &str = "social.colibri.channel";

#[derive(Serialize, Debug)]
pub struct ChannelUriResponse {
    pub uri: String,
}

// ---- channel.create --------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn create_channel_with(
    community_uri: String,
    category_uri: String,
    name: String,
    channel_type: String,
    description: Option<String>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    let category =
        AtUri::parse(&category_uri).ok_or_else(|| invalid_request("Invalid category AT-URI."))?;

    with_community_authz(
        auth,
        "social.colibri.channel.create",
        community_uri,
        Some(Permission::ChannelCreate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let channel_rkey = generate_tid();
            let category_rkey = &category.rkey;

            // Build and write the new channel record.
            let channel = ColibriChannel {
                r#type: CHANNEL_NSID.to_string(),
                name,
                description,
                channel_type,
                category: category_rkey.clone(),
                community: COMMUNITY_RKEY.to_string(),
                owner_only: None,
            };
            let chan_data = serde_json::to_value(&channel)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::create_record(
                &db,
                community_did,
                CHANNEL_NSID,
                Some(&channel_rkey),
                chan_data,
            )
            .await?;

            // Append the new channel rkey to the category's channelOrder.
            let cat_current =
                community_write::read_cached(&db, community_did, CATEGORY_NSID, category_rkey)
                    .await?
                    .ok_or_else(|| not_found_error("Category not found in AppView cache."))?;

            let mut cat: ColibriCategory = serde_json::from_value(cat_current).map_err(|e| {
                invalid_request(format!("Cached category record is malformed: {e}"))
            })?;
            cat.channel_order.push(channel_rkey.clone());
            let cat_data =
                serde_json::to_value(&cat).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, CATEGORY_NSID, category_rkey, cat_data)
                .await?;

            Ok(Json(ChannelUriResponse {
                uri: format!("at://{}/{}/{}", community_did, CHANNEL_NSID, channel_rkey),
            }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.channel.create?<community>&<category>&<name>&<type>&<description>&<auth>"
)]
pub async fn create_channel(
    community: &str,
    category: &str,
    name: &str,
    r#type: &str,
    description: Option<&str>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    create_channel_with(
        community.to_string(),
        category.to_string(),
        name.to_string(),
        r#type.to_string(),
        description.map(str::to_string),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- channel.update --------------------------------------------------------

async fn update_channel_with(
    channel_uri: String,
    name: Option<String>,
    description: Option<String>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    let channel =
        AtUri::parse(&channel_uri).ok_or_else(|| invalid_request("Invalid channel AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        channel.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

    with_community_authz(
        auth,
        "social.colibri.channel.update",
        community_uri,
        Some(Permission::ChannelUpdate),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let channel_rkey = &channel.rkey;

            let current =
                community_write::read_cached(&db, community_did, CHANNEL_NSID, channel_rkey)
                    .await?
                    .ok_or_else(|| not_found_error("Channel not found in AppView cache."))?;

            let mut rec: ColibriChannel = serde_json::from_value(current)
                .map_err(|e| invalid_request(format!("Cached channel record is malformed: {e}")))?;

            if let Some(n) = name {
                rec.name = n;
            }
            if let Some(d) = description {
                rec.description = Some(d);
            }

            let data =
                serde_json::to_value(&rec).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, CHANNEL_NSID, channel_rkey, data)
                .await?;

            Ok(Json(ChannelUriResponse { uri: channel_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.channel.update?<channel>&<name>&<description>&<auth>")]
pub async fn update_channel(
    channel: &str,
    name: Option<&str>,
    description: Option<&str>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    update_channel_with(
        channel.to_string(),
        name.map(str::to_string),
        description.map(str::to_string),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}

// ---- channel.delete --------------------------------------------------------

async fn delete_channel_with(
    channel_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    let channel =
        AtUri::parse(&channel_uri).ok_or_else(|| invalid_request("Invalid channel AT-URI."))?;
    let community_uri = format!(
        "at://{}/{}/{}",
        channel.authority, COMMUNITY_NSID, COMMUNITY_RKEY
    );

    with_community_authz(
        auth,
        "social.colibri.channel.delete",
        community_uri,
        Some(Permission::ChannelDelete),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            let community_did = &ctx.community.authority;
            let channel_rkey = &channel.rkey;

            // Read the channel first so we know which category to update.
            let chan_data =
                community_write::read_cached(&db, community_did, CHANNEL_NSID, channel_rkey)
                    .await?;
            let category_rkey = chan_data
                .and_then(|d| serde_json::from_value::<ColibriChannel>(d).ok())
                .map(|c| c.category);

            community_write::delete_record(&db, community_did, CHANNEL_NSID, channel_rkey).await?;

            // Remove from the parent category's channelOrder.
            if let Some(cat_rkey) = category_rkey
                && let Ok(Some(cat_current)) =
                    community_write::read_cached(&db, community_did, CATEGORY_NSID, &cat_rkey).await
                && let Ok(mut cat) = serde_json::from_value::<ColibriCategory>(cat_current)
            {
                cat.channel_order.retain(|r| r != channel_rkey);
                if let Ok(data) = serde_json::to_value(&cat)
                    && let Err(e) = community_write::put_record(
                        &db,
                        community_did,
                        CATEGORY_NSID,
                        &cat_rkey,
                        data,
                    )
                    .await
                {
                    log::warn!(
                        "failed to remove deleted channel {channel_rkey} from \
                                     category {cat_rkey}: {e}"
                    );
                }
            }

            Ok(Json(ChannelUriResponse { uri: channel_uri }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.channel.delete?<channel>&<auth>")]
pub async fn delete_channel(
    channel: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    delete_channel_with(
        channel.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
