use rocket::data::ToByteUnit;
use rocket::form::Form;
use rocket::fs::TempFile;
use rocket::serde::json::Json;
use rocket::{FromForm, State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;
use serde_json::Value;

use crate::lib::colibri::ColibriCommunity;
use crate::lib::community_credentials;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::crypto;
use crate::lib::handler::{
    CallerContext, LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed,
    with_community_authz,
};
use crate::lib::pds_client;
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;
use crate::xrpc::util::unpack_image_file;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const ALLOWED_PICTURE_MIME_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif"];

/// Upper bound (in mebibytes) on the image bytes accepted in the request
/// body. Generous enough for community avatars while still capping abusive
/// uploads.
const MAX_PICTURE_MEBIBYTES: u64 = 10;
const MAX_BANNER_MEBIBYTES: u64 = 10;

#[derive(Serialize, Debug)]
pub struct UpdateCommunityResponse {
    pub uri: String,
}

#[allow(clippy::too_many_arguments)]
async fn update_community_with(
    community_uri: String,
    name: Option<String>,
    description: Option<String>,
    picture_blob: Option<Vec<u8>>,
    picture_mime: Option<String>,
    banner_blob: Option<Vec<u8>>,
    banner_mime: Option<String>,
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
            if let Some(bytes) = picture_blob {
                let mime = picture_mime.as_deref().unwrap_or_default();
                let blob = upload_image_to_pds(&ctx, &db, mime, bytes).await?;
                community.picture = Some(blob);
            }

            // Handle banner upload if new bytes were supplied in the request
            // body.
            if let Some(bytes) = banner_blob {
                let mime = banner_mime.as_deref().unwrap_or_default();
                let blob = upload_image_to_pds(&ctx, &db, mime, bytes).await?;
                community.banner = Some(blob);
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

#[derive(FromForm, Debug)]
pub struct CommunityXrpcBody<'r> {
    picture: Option<TempFile<'r>>,
    banner: Option<TempFile<'r>>,
}

async fn upload_image_to_pds(
    ctx: &CallerContext,
    db: &DatabaseConnection,
    mime_type: &str,
    bytes: Vec<u8>,
) -> Result<Value, ErrorResponse> {
    if !ALLOWED_PICTURE_MIME_TYPES.contains(&mime_type) {
        return Err(invalid_request(format!(
            "Unsupported image MIME type `{mime_type}`. Accepted: {}.",
            ALLOWED_PICTURE_MIME_TYPES.join(", ")
        )));
    }

    let creds =
        community_credentials::load_credentials(db, crypto::master_key(), &ctx.community.authority)
            .await
            .map_err(community_write::creds_err_to_db)?
            .ok_or_else(|| {
                community_credentials::missing_credentials_err(&ctx.community.authority)
            })?;

    let session =
        pds_client::create_session(&creds.pds_endpoint, &creds.identifier, &creds.password)
            .await
            .map_err(community_write::pds_err_to_db)?;

    let blob = pds_client::upload_blob(&creds.pds_endpoint, &session.access_jwt, bytes, mime_type)
        .await
        .map_err(community_write::pds_err_to_db)?;

    Ok(blob)
}

#[post(
    "/xrpc/social.colibri.community.update?<community>&<name>&<description>&<requiresApprovalToJoin>&<auth>",
    data = "<body>"
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
    requiresApprovalToJoin: Option<bool>,
    auth: &str,
    body: Form<CommunityXrpcBody<'_>>,
    db: &State<DatabaseConnection>,
) -> Result<Json<UpdateCommunityResponse>, ErrorResponse> {
    let (picture_blob, picture_mime) =
        match unpack_image_file(&body.picture, MAX_PICTURE_MEBIBYTES.mebibytes()).await? {
            None => (None, None),
            Some((blob, mime)) => (Some(blob), Some(mime)),
        };

    let (banner_blob, banner_mime) =
        match unpack_image_file(&body.banner, MAX_BANNER_MEBIBYTES.mebibytes()).await? {
            None => (None, None),
            Some((blob, mime)) => (Some(blob), Some(mime)),
        };

    update_community_with(
        community.to_string(),
        name.map(str::to_string),
        description.map(str::to_string),
        picture_blob,
        picture_mime,
        banner_blob,
        banner_mime,
        requiresApprovalToJoin,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
