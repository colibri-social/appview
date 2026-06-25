use rocket::data::{Data, ToByteUnit};
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::colibri::ColibriCommunity;
use crate::lib::community_credentials;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::crypto;
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::pds_client;
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const ALLOWED_PICTURE_MIME_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif"];
/// Upper bound (in mebibytes) on the picture bytes accepted in the request
/// body. Generous enough for community avatars while still capping abusive
/// uploads.
const MAX_PICTURE_MEBIBYTES: i64 = 10;

#[derive(Serialize, Debug)]
pub struct UpdateCommunityResponse {
    pub uri: String,
}

#[allow(clippy::too_many_arguments)]
async fn update_community_with(
    community_uri: String,
    name: Option<String>,
    description: Option<String>,
    picture: Option<Vec<u8>>,
    mime_type: Option<String>,
    requires_approval_to_join: Option<bool>,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<UpdateCommunityResponse>, ErrorResponse> {
    with_community_authz(
        auth,
        "social.colibri.community.update",
        community_uri.clone(),
        Some(Permission::CommunityManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        |ctx, db| async move {
            // Read the current community record from cache.
            let current_data = community_write::read_cached(
                &db,
                &ctx.community.authority,
                COMMUNITY_NSID,
                COMMUNITY_RKEY,
            )
            .await?
            .ok_or_else(|| not_found_error("Community record not found in AppView cache."))?;

            let mut community: ColibriCommunity =
                serde_json::from_value(current_data).map_err(|e| {
                    invalid_request(format!("Cached community record is malformed: {e}"))
                })?;

            // Patch the mutable fields.
            if let Some(n) = name {
                community.name = n;
            }
            if let Some(d) = description {
                community.description = d;
            }
            if let Some(r) = requires_approval_to_join {
                community.requires_approval_to_join = r;
            }

            // Handle picture upload if new bytes were supplied in the request
            // body.
            if let Some(bytes) = picture {
                let mt = mime_type.as_deref().unwrap_or("");
                if !ALLOWED_PICTURE_MIME_TYPES.contains(&mt) {
                    return Err(invalid_request(format!(
                        "Unsupported mimeType `{mt}`. Accepted: {}.",
                        ALLOWED_PICTURE_MIME_TYPES.join(", ")
                    )));
                }

                let creds = community_credentials::load_credentials(
                    &db,
                    crypto::master_key(),
                    &ctx.community.authority,
                )
                .await
                .map_err(community_write::creds_err_to_db)?
                .ok_or_else(|| {
                    sea_orm::DbErr::Custom(format!(
                        "no credentials for {}",
                        ctx.community.authority
                    ))
                })?;

                let session = pds_client::create_session(
                    &creds.pds_endpoint,
                    &creds.identifier,
                    &creds.password,
                )
                .await
                .map_err(community_write::pds_err_to_db)?;

                let blob =
                    pds_client::upload_blob(&creds.pds_endpoint, &session.access_jwt, bytes, mt)
                        .await
                        .map_err(community_write::pds_err_to_db)?;

                community.picture = Some(blob);
            }

            let data = serde_json::to_value(&community)
                .map_err(|e| sea_orm::DbErr::Custom(e.to_string()))?;

            community_write::put_record(
                &db,
                &ctx.community.authority,
                COMMUNITY_NSID,
                COMMUNITY_RKEY,
                data,
            )
            .await?;

            Ok(Json(UpdateCommunityResponse {
                uri: format!(
                    "at://{}/{}/{}",
                    ctx.community.authority, COMMUNITY_NSID, COMMUNITY_RKEY
                ),
            }))
        },
    )
    .await
}

#[post(
    "/xrpc/social.colibri.community.update?<community>&<name>&<description>&<mimeType>&<requiresApprovalToJoin>&<auth>",
    data = "<picture>"
)]
/// Updates the community's metadata. Only the fields supplied are changed;
/// omitted fields keep their current values. The picture, if any, is sent as
/// the raw request body — large images can't fit in a query string — with its
/// MIME type declared via the `mimeType` query parameter. An empty body means
/// "no picture change".
#[allow(non_snake_case, clippy::too_many_arguments)]
pub async fn update_community(
    community: &str,
    name: Option<&str>,
    description: Option<&str>,
    mimeType: Option<&str>,
    requiresApprovalToJoin: Option<bool>,
    auth: &str,
    picture: Data<'_>,
    db: &State<DatabaseConnection>,
) -> Result<Json<UpdateCommunityResponse>, ErrorResponse> {
    // Read the (optional) picture bytes from the request body. An empty body
    // means "no picture change"; anything else is treated as raw image bytes.
    let capped = picture
        .open(MAX_PICTURE_MEBIBYTES.mebibytes())
        .into_bytes()
        .await
        .map_err(|e| invalid_request(format!("Failed to read picture body: {e}")))?;
    if !capped.is_complete() {
        return Err(invalid_request(format!(
            "picture exceeds the maximum allowed size of {MAX_PICTURE_MEBIBYTES} MiB."
        )));
    }
    let bytes = capped.into_inner();
    let picture = if bytes.is_empty() { None } else { Some(bytes) };

    update_community_with(
        community.to_string(),
        name.map(str::to_string),
        description.map(str::to_string),
        picture,
        mimeType.map(str::to_string),
        requiresApprovalToJoin,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
