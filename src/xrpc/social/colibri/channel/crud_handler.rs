use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::channel_authz;
use crate::lib::colibri::{ColibriCategory, ColibriChannel, ColibriRole};
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::moderation::generate_tid;
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const CATEGORY_NSID: &str = "social.colibri.category";
const CHANNEL_NSID: &str = "social.colibri.channel";
const ROLE_NSID: &str = "social.colibri.role";

fn forbidden_message(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("Forbidden"),
            message: message.into(),
        }),
    }
}

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
                allowed_roles: vec![],
                allowed_members: vec![],
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

#[allow(clippy::too_many_arguments)]
async fn update_channel_with(
    channel_uri: String,
    name: Option<String>,
    description: Option<String>,
    owner_only: Option<bool>,
    allowed_roles: Vec<String>,
    clear_allowed_roles: Option<bool>,
    allowed_members: Vec<String>,
    clear_allowed_members: Option<bool>,
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

            // Only the owner may toggle ownerOnly: it's a channel-wide lock
            // that overrides every other restriction, so a non-owner editor
            // (even one with channel.update) shouldn't be able to flip it.
            if owner_only.is_some() && !ctx.authz.is_owner {
                return Err(forbidden_message(
                    "Only the community owner may change ownerOnly.",
                ));
            }
            if let Some(o) = owner_only {
                rec.owner_only = Some(o);
            }

            // Hierarchy guard, same idea as Discord: an editor may only
            // add/remove roles strictly below their own highest role, and
            // only add/remove members who don't outrank them. Entries that
            // are present in both the old and new list are left alone even
            // if the editor couldn't have added them themselves.
            let old_allowed_roles = rec.allowed_roles.clone();
            let final_allowed_roles = if clear_allowed_roles == Some(true) {
                vec![]
            } else if !allowed_roles.is_empty() {
                allowed_roles
            } else {
                old_allowed_roles.clone()
            };
            for role_rkey in channel_authz::touched_entries(&old_allowed_roles, &final_allowed_roles)
            {
                let role_data =
                    community_write::read_cached(&db, community_did, ROLE_NSID, &role_rkey)
                        .await?
                        .ok_or_else(|| invalid_request(format!("Role {role_rkey} not found.")))?;
                let role: ColibriRole = serde_json::from_value(role_data).map_err(|e| {
                    invalid_request(format!("Cached role record is malformed: {e}"))
                })?;
                if !ctx.authz.outranks_position(role.position) {
                    return Err(forbidden_message(format!(
                        "Cannot add or remove role {role_rkey}: it is not below your highest role."
                    )));
                }
            }
            rec.allowed_roles = final_allowed_roles;

            let old_allowed_members = rec.allowed_members.clone();
            let final_allowed_members = if clear_allowed_members == Some(true) {
                vec![]
            } else if !allowed_members.is_empty() {
                allowed_members
            } else {
                old_allowed_members.clone()
            };
            for member_did in
                channel_authz::touched_entries(&old_allowed_members, &final_allowed_members)
            {
                let target_authz =
                    load_authz_fn(db.clone(), ctx.community_uri.clone(), member_did.clone())
                        .await?;
                if !ctx.authz.outranks(&target_authz) {
                    return Err(forbidden_message(format!(
                        "Cannot add or remove member {member_did}: they hold an equal or higher role."
                    )));
                }
            }
            rec.allowed_members = final_allowed_members;

            let data =
                serde_json::to_value(&rec).map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;
            community_write::put_record(&db, community_did, CHANNEL_NSID, channel_rkey, data)
                .await?;

            Ok(Json(ChannelUriResponse { uri: channel_uri }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.channel.update?<channel>&<name>&<description>&<ownerOnly>&<allowedRoles>&<clearAllowedRoles>&<allowedMembers>&<clearAllowedMembers>&<auth>"
)]
#[allow(non_snake_case, clippy::too_many_arguments)]
pub async fn update_channel(
    channel: &str,
    name: Option<&str>,
    description: Option<&str>,
    ownerOnly: Option<bool>,
    allowedRoles: Vec<String>,
    clearAllowedRoles: Option<bool>,
    allowedMembers: Vec<String>,
    clearAllowedMembers: Option<bool>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ChannelUriResponse>, ErrorResponse> {
    update_channel_with(
        channel.to_string(),
        name.map(str::to_string),
        description.map(str::to_string),
        ownerOnly,
        allowedRoles,
        clearAllowedRoles,
        allowedMembers,
        clearAllowedMembers,
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
