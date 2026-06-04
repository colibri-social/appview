use base64::Engine as _;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::colibri::ColibriCommunity;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::pds_client;
use crate::lib::responses::ErrorResponse;
use crate::lib::crypto;
use crate::lib::community_credentials;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";
const ALLOWED_PICTURE_MIME_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif"];

#[derive(Serialize, Debug)]
pub struct UpdateCommunityResponse {
    pub uri: String,
}

async fn update_community_with(
    community_uri: String,
    name: Option<String>,
    description: Option<String>,
    picture_b64: Option<String>,
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
        Some(Permission::CommunityUpdate),
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

            // Handle picture upload if a new one was supplied.
            if let Some(b64) = picture_b64 {
                let mt = mime_type.as_deref().unwrap_or("");
                if !ALLOWED_PICTURE_MIME_TYPES.contains(&mt) {
                    return Err(invalid_request(format!(
                        "Unsupported mime_type `{mt}`. Accepted: {}.",
                        ALLOWED_PICTURE_MIME_TYPES.join(", ")
                    )));
                }
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(&b64)
                    .or_else(|_| {
                        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&b64)
                    })
                    .map_err(|_| invalid_request("picture is not valid base64."))?;

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

                let blob = pds_client::upload_blob(
                    &creds.pds_endpoint,
                    &session.access_jwt,
                    bytes,
                    mt,
                )
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
    "/xrpc/social.colibri.community.update?<community>&<name>&<description>&<picture>&<mime_type>&<requires_approval_to_join>&<auth>"
)]
/// Updates the community's metadata. Only the fields supplied are changed;
/// omitted fields keep their current values.
pub async fn update_community(
    community: &str,
    name: Option<&str>,
    description: Option<&str>,
    picture: Option<&str>,
    mime_type: Option<&str>,
    requires_approval_to_join: Option<bool>,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<UpdateCommunityResponse>, ErrorResponse> {
    update_community_with(
        community.to_string(),
        name.map(str::to_string),
        description.map(str::to_string),
        picture.map(str::to_string),
        mime_type.map(str::to_string),
        requires_approval_to_join,
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
    )
    .await
}
