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

            // Only an admin may toggle ownerOnly: it's a channel-wide lock
            // that overrides every other restriction, so an ordinary editor
            // (even one with channel.update) shouldn't be able to flip it.
            if owner_only.is_some() && !ctx.authz.is_admin() {
                return Err(forbidden_message(
                    "Only a community admin may change ownerOnly.",
                ));
            }
            if let Some(o) = owner_only {
                rec.owner_only = Some(o);
            }

            // The client addresses roles by full AT-URI, but channel records
            // store bare rkeys (so `can_post` can compare them against a
            // member's stored role rkeys). Normalize incoming URIs to rkeys;
            // anything that isn't a parseable AT-URI is passed through as-is.
            let allowed_roles: Vec<String> = allowed_roles
                .iter()
                .map(|r| AtUri::parse(r).map(|u| u.rkey).unwrap_or_else(|| r.clone()))
                .collect();

            // Hierarchy guard, same idea as Discord: an editor may only
            // add/remove roles strictly below their own highest role, and
            // only add/remove members who don't outrank them. Entries that
            // are present in both the old and new list are left alone even
            // if the editor couldn't have added them themselves.
            //
            // Admins (the owner, or a protected-role holder) bypass these
            // guards entirely, mirroring `can_post` and the ownerOnly gate.
            // The human owner acts under their own DID — `is_owner` never
            // matches them — so without this they'd fail their own equal
            // position check (e.g. adding themselves to allowedMembers, or
            // adding the Owner role itself).
            let is_admin = ctx.authz.is_admin();
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
                if !is_admin && !ctx.authz.outranks_position(role.position) {
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
            if !is_admin {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::{member, role};
    use crate::models::record_data;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn channel_record() -> serde_json::Value {
        serde_json::json!({
            "$type": "social.colibri.channel",
            "name": "General",
            "type": "social.colibri.channel.text",
            "category": "cat1",
            "community": "self",
        })
    }

    // A non-admin who holds `channel.update` (via an unprotected role) clears the
    // permission gate but must still be refused when they try to flip ownerOnly —
    // the gate that the always-send-ownerOnly client bug used to trip silently.
    #[tokio::test]
    async fn update_channel_rejects_owner_only_change_from_non_admin() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![record_data::Model {
                id: 1,
                did: String::from("did:plc:owner"),
                nsid: String::from(CHANNEL_NSID),
                rkey: String::from("chan1"),
                data: channel_record(),
                indexed_at: String::new(),
            }]])
            .into_connection();

        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:rando", vec!["mod"])),
            roles: vec![role("Moderator", 10, vec![Permission::ChannelUpdate])],
        };

        let result = update_channel_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan1"),
            None,
            None,
            Some(true),
            vec![],
            None,
            vec![],
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &move |_, _, _| {
                let authz = authz.clone();
                Box::pin(async move { Ok(authz) })
            },
        )
        .await;

        let body = result.err().expect("expected Forbidden").body.into_inner();
        assert_eq!(body.error, "Forbidden");
        assert!(body.message.contains("admin"));
    }

    // The community owner acts under their own DID (so `is_owner` is false; they
    // hold a protected "Owner" role instead). Adding themselves to allowedMembers
    // used to trip the member hierarchy guard — they compare equal against
    // themselves, and `outranks` requires a strictly higher position. Admins now
    // bypass the guard. The write itself needs un-mockable PDS credentials, so we
    // only assert that the request gets *past* the hierarchy guard rather than
    // being rejected with the Forbidden "equal or higher role" error.
    #[tokio::test]
    async fn update_channel_lets_owner_add_themselves_to_allowed_members() {
        // The write path past the guard needs the at-rest key installed (a
        // process global). Ignore the error if another test already set it.
        let _ = crate::lib::crypto::install_master_key(vec![0u8; 32]);

        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![record_data::Model {
                id: 1,
                did: String::from("did:plc:owner"),
                nsid: String::from(CHANNEL_NSID),
                rkey: String::from("chan1"),
                data: channel_record(),
                indexed_at: String::new(),
            }]])
            .into_connection();

        let mut owner_role = role("Owner", 100, vec![Permission::ChannelUpdate]);
        owner_role.protected = Some(true);
        let authz = ActorAuthz {
            is_owner: false,
            member: Some(member("did:plc:owner-human", vec!["owner"])),
            roles: vec![owner_role],
        };

        let result = update_channel_with(
            String::from("at://did:plc:owner/social.colibri.channel/chan1"),
            None,
            None,
            None,
            vec![],
            None,
            vec![String::from("did:plc:owner-human")],
            None,
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:owner-human")) }),
            &move |_, _, _| {
                let authz = authz.clone();
                Box::pin(async move { Ok(authz) })
            },
        )
        .await;

        // Either it succeeds, or it fails later (credentials/PDS) — but it must
        // never be the hierarchy rejection.
        if let Err(e) = result {
            let body = e.body.into_inner();
            assert!(
                !body.message.contains("equal or higher role"),
                "owner was wrongly rejected by the hierarchy guard: {}",
                body.message
            );
        }
    }
}
