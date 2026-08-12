use rocket::data::ToByteUnit;
use rocket::form::Form;
use rocket::fs::TempFile;
use rocket::serde::json::Json;
use rocket::{FromForm, State, post};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::colibri::ColibriCommunity;
use crate::lib::community_write::{self, invalid_request, not_found_error};
use crate::lib::handler::{CallerContext, LoadAuthzFn, load_authz_boxed};
use crate::lib::peer_client::PeerBody;
use crate::lib::permissions::Permission;
use crate::lib::relay::{RelayContext, RelayRequest, WriteDeps, with_community_write};
use crate::lib::responses::ErrorResponse;
use crate::xrpc::util::unpack_image_file;

const COMMUNITY_NSID: &str = "social.colibri.community";
const COMMUNITY_RKEY: &str = "self";

/// Upper bound (in mebibytes) on the image bytes accepted in the request
/// body. Generous enough for community avatars while still capping abusive
/// uploads.
const MAX_PICTURE_MEBIBYTES: u64 = 10;
const MAX_BANNER_MEBIBYTES: u64 = 10;

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateCommunityResponse {
    pub uri: String,
}

/// The fields a single `community.update` call may change. `picture`/`banner`
/// hold already-uploaded blob refs, the matching `remove_*` flag drops the
/// current one instead. Anything left `None` keeps its current value.
struct CommunityPatch {
    name: Option<String>,
    description: Option<String>,
    requires_approval_to_join: Option<bool>,
    picture: Option<Value>,
    remove_picture: bool,
    banner: Option<Value>,
    remove_banner: bool,
}

fn apply_patch(community: &mut ColibriCommunity, patch: CommunityPatch) {
    if let Some(n) = patch.name {
        community.name = n;
    }
    if let Some(d) = patch.description {
        community.description = d;
    }
    if let Some(r) = patch.requires_approval_to_join {
        community.requires_approval_to_join = r;
    }

    if patch.picture.is_some() {
        community.picture = patch.picture;
    } else if patch.remove_picture {
        community.picture = None;
    }

    if patch.banner.is_some() {
        community.banner = patch.banner;
    } else if patch.remove_banner {
        community.banner = None;
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_community_with(
    relay: RelayRequest,
    community_uri: String,
    name: Option<String>,
    description: Option<String>,
    picture_blob: Option<Vec<u8>>,
    picture_mime: Option<String>,
    banner_blob: Option<Vec<u8>>,
    banner_mime: Option<String>,
    requires_approval_to_join: Option<bool>,
    remove_picture: bool,
    remove_banner: bool,
    auth: String,
    db: DatabaseConnection,
    deps: WriteDeps<'_>,
    load_authz_fn: &LoadAuthzFn,
) -> Result<Json<UpdateCommunityResponse>, ErrorResponse> {
    if remove_picture && picture_blob.is_some() {
        return Err(invalid_request(
            "`removePicture` cannot be combined with a new picture in the request body.",
        ));
    }
    if remove_banner && banner_blob.is_some() {
        return Err(invalid_request(
            "`removeBanner` cannot be combined with a new banner in the request body.",
        ));
    }

    let deps = WriteDeps {
        load_authz_fn,
        ..deps
    };

    with_community_write(
        relay,
        auth,
        "social.colibri.community.update",
        community_uri.clone(),
        Some(Permission::CommunityManage),
        None,
        db,
        &deps,
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

            // Upload any new images before patching so the record can embed the
            // resulting blob refs.
            let new_picture = match picture_blob {
                Some(bytes) => {
                    let mime = picture_mime.as_deref().unwrap_or_default();
                    Some(upload_image_to_pds(&ctx, &db, mime, bytes).await?)
                }
                None => None,
            };
            let new_banner = match banner_blob {
                Some(bytes) => {
                    let mime = banner_mime.as_deref().unwrap_or_default();
                    Some(upload_image_to_pds(&ctx, &db, mime, bytes).await?)
                }
                None => None,
            };

            apply_patch(
                &mut community,
                CommunityPatch {
                    name,
                    description,
                    requires_approval_to_join,
                    picture: new_picture,
                    remove_picture,
                    banner: new_banner,
                    remove_banner,
                },
            );

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
    if !community_write::is_allowed_picture_mime(mime_type) {
        return Err(invalid_request(format!(
            "Unsupported image MIME type `{mime_type}`. Accepted: {}.",
            community_write::ALLOWED_PICTURE_MIME_TYPES.join(", ")
        )));
    }

    let blob = community_write::upload_blob(db, &ctx.community.authority, bytes, mime_type).await?;

    Ok(blob)
}

#[post(
    "/xrpc/social.colibri.community.update?<community>&<name>&<description>&<requiresApprovalToJoin>&<removePicture>&<removeBanner>&<auth>",
    data = "<body>"
)]
/// Updates the community's metadata. Only the fields supplied are changed;
/// omitted fields keep their current values. The picture and banner, if any,
/// are sent as multipart parts named after them, each carrying its own MIME
/// type. A part left out means "no change" for that image, `removePicture` and
/// `removeBanner` drop the current one instead.
#[allow(non_snake_case, clippy::too_many_arguments)]
pub async fn update_community(
    relay: RelayContext,
    community: &str,
    name: Option<&str>,
    description: Option<&str>,
    requiresApprovalToJoin: Option<bool>,
    removePicture: Option<bool>,
    removeBanner: Option<bool>,
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
        relay_request(
            relay,
            picture_blob.as_deref(),
            picture_mime.as_deref(),
            banner_blob.as_deref(),
            banner_mime.as_deref(),
        ),
        community.to_string(),
        name.map(str::to_string),
        description.map(str::to_string),
        picture_blob,
        picture_mime,
        banner_blob,
        banner_mime,
        requiresApprovalToJoin,
        removePicture.unwrap_or(false),
        removeBanner.unwrap_or(false),
        auth.to_string(),
        db.inner().clone(),
        WriteDeps::production(),
        &load_authz_boxed,
    )
    .await
}

const RELAY_BOUNDARY: &str = "colibri-relay-boundary-8f2c1a";

fn relay_request(
    relay: RelayContext,
    picture: Option<&[u8]>,
    picture_mime: Option<&str>,
    banner: Option<&[u8]>,
    banner_mime: Option<&str>,
) -> RelayRequest {
    let mut parts: Vec<(&str, &[u8], &str)> = Vec::new();
    if let (Some(bytes), Some(mime)) = (picture, picture_mime) {
        parts.push(("picture", bytes, mime));
    }
    if let (Some(bytes), Some(mime)) = (banner, banner_mime) {
        parts.push(("banner", bytes, mime));
    }
    if parts.is_empty() {
        return relay.into();
    }

    let mut bytes: Vec<u8> = Vec::new();
    for (name, data, mime) in parts {
        bytes.extend_from_slice(
            format!(
                "--{RELAY_BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"; \
                 filename=\"{name}\"\r\nContent-Type: {mime}\r\n\r\n"
            )
            .as_bytes(),
        );
        bytes.extend_from_slice(data);
        bytes.extend_from_slice(b"\r\n");
    }
    bytes.extend_from_slice(format!("--{RELAY_BOUNDARY}--\r\n").as_bytes());

    relay.with_body(PeerBody {
        content_type: format!("multipart/form-data; boundary={RELAY_BOUNDARY}"),
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::{mock_db, relay_ctx, write_deps_never_authenticating};
    use rocket::tokio;
    use serde_json::json;

    fn blob(cid: &str) -> Value {
        json!({
            "$type": "blob",
            "ref": { "$link": cid },
            "mimeType": "image/png",
            "size": 1234,
        })
    }

    fn community() -> ColibriCommunity {
        ColibriCommunity {
            r#type: String::from(COMMUNITY_NSID),
            name: String::from("Test"),
            description: String::from("desc"),
            category_order: vec![String::from("cat1")],
            requires_approval_to_join: false,
            picture: Some(blob("bafpicture")),
            banner: Some(blob("bafbanner")),
            migrated_to: None,
            migrated_from: None,
            appview: Some(String::from("did:web:appview.test")),
        }
    }

    fn patch() -> CommunityPatch {
        CommunityPatch {
            name: None,
            description: None,
            requires_approval_to_join: None,
            picture: None,
            remove_picture: false,
            banner: None,
            remove_banner: false,
        }
    }

    #[test]
    fn removing_the_banner_drops_it_from_the_record() {
        let mut c = community();

        apply_patch(
            &mut c,
            CommunityPatch {
                remove_banner: true,
                ..patch()
            },
        );

        assert!(c.banner.is_none());
        assert_eq!(c.picture, Some(blob("bafpicture")));

        let serialized = serde_json::to_value(&c).unwrap();
        assert!(serialized.get("banner").is_none());
        assert!(serialized.get("picture").is_some());
    }

    #[test]
    fn omitting_the_banner_keeps_the_existing_one() {
        let mut c = community();

        apply_patch(
            &mut c,
            CommunityPatch {
                name: Some(String::from("Renamed")),
                ..patch()
            },
        );

        assert_eq!(c.name, "Renamed");
        assert_eq!(c.banner, Some(blob("bafbanner")));
        assert_eq!(c.picture, Some(blob("bafpicture")));
    }

    #[test]
    fn a_new_banner_replaces_the_existing_one() {
        let mut c = community();

        apply_patch(
            &mut c,
            CommunityPatch {
                banner: Some(blob("bafnewbanner")),
                ..patch()
            },
        );

        assert_eq!(c.banner, Some(blob("bafnewbanner")));
    }

    #[test]
    fn removing_the_picture_leaves_the_banner_alone() {
        let mut c = community();

        apply_patch(
            &mut c,
            CommunityPatch {
                remove_picture: true,
                ..patch()
            },
        );

        assert!(c.picture.is_none());
        assert_eq!(c.banner, Some(blob("bafbanner")));
    }

    #[tokio::test]
    async fn rejects_a_new_banner_combined_with_removal() {
        let err = update_community_with(
            relay_ctx().into(),
            String::from("at://did:plc:c/social.colibri.community/self"),
            None,
            None,
            None,
            None,
            Some(vec![1, 2, 3]),
            Some(String::from("image/png")),
            None,
            false,
            true,
            String::from("token"),
            mock_db(),
            write_deps_never_authenticating(),
            &|_, _, _| Box::pin(async { panic!("must not load authz") }),
        )
        .await
        .expect_err("bytes plus removeBanner must be rejected");

        assert_eq!(err.body.error, "InvalidRequest");
    }
}
