use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{ColibriActorData, ColibriActorProfile, resolve_effective_profile};
use crate::lib::member_records;
use crate::lib::responses::{ErrorCode, ErrorResponse};
use crate::lib::voice_moderation::ModerationLookup;
use crate::models::{record_data, repos, user_states};
use crate::xrpc::social::colibri::actor::get_data_handler::{
    ActorData, ActorStatus, actor_data_from_effective,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Member {
    pub did: String,
    pub handle: String,
    pub roles: Vec<String>,
    pub data: ActorData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vc: Option<String>,
    #[serde(rename = "vcMuted", skip_serializing_if = "Option::is_none")]
    pub vc_muted: Option<bool>,
    #[serde(rename = "vcDeafened", skip_serializing_if = "Option::is_none")]
    pub vc_deafened: Option<bool>,
    #[serde(rename = "vcServerMuted", skip_serializing_if = "Option::is_none")]
    pub vc_server_muted: Option<bool>,
    #[serde(rename = "vcServerDeafened", skip_serializing_if = "Option::is_none")]
    pub vc_server_deafened: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MemberList {
    pub members: Vec<Member>,
}

/// Data fetched in a single round-trip per backing table, keyed by member DID.
pub struct MemberAggregate {
    pub member_dids: Vec<String>,
    pub profiles: HashMap<String, ActorProfile>,
    /// Raw `social.colibri.actor.profile` records, keyed by member DID. Resolved
    /// against `profiles` per the `syncBluesky` rule when building each member.
    pub colibri_profiles: HashMap<String, ColibriActorProfile>,
    pub actor_data: HashMap<String, ColibriActorData>,
    pub states: HashMap<String, String>,
    pub vc: HashMap<String, String>,
    pub vc_muted: HashMap<String, bool>,
    pub vc_deafened: HashMap<String, bool>,
    pub vc_server_muted: HashMap<String, bool>,
    pub vc_server_deafened: HashMap<String, bool>,
    pub handles: HashMap<String, String>,
    /// Role rkeys assigned to each member, keyed by member DID. These are
    /// raw record keys — callers must expand them to full AT-URIs.
    pub member_roles: HashMap<String, Vec<String>>,
}

pub async fn fetch_member_aggregate(
    db: &DatabaseConnection,
    community_uri: &str,
    moderation: &ModerationLookup,
) -> Result<MemberAggregate, DbErr> {
    // Variant A: members are `social.colibri.member` records sitting on the
    // community's own repo, with `data->>'subject'` carrying the user DID.
    let community = AtUri::parse(community_uri)
        .ok_or_else(|| DbErr::Custom(format!("invalid community AT-URI: {community_uri}")))?;

    let members = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&community.authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.member"))
        .all(db)
        .await?;

    member_records::spawn_dedupe(db, &community.authority, &members);
    let members = member_records::one_per_subject(members);

    let mut member_roles: HashMap<String, Vec<String>> = HashMap::new();
    let mut member_dids: Vec<String> = members
        .into_iter()
        .filter_map(|m| {
            let did = m
                .data
                .get("subject")
                .and_then(|v| v.as_str())
                .map(String::from)?;
            let roles = m
                .data
                .get("roles")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            member_roles.insert(did.clone(), roles);
            Some(did)
        })
        .collect();
    member_dids.sort();
    member_dids.dedup();

    if member_dids.is_empty() {
        return Ok(MemberAggregate {
            member_dids,
            profiles: HashMap::new(),
            colibri_profiles: HashMap::new(),
            actor_data: HashMap::new(),
            states: HashMap::new(),
            vc: HashMap::new(),
            vc_muted: HashMap::new(),
            vc_deafened: HashMap::new(),
            vc_server_muted: HashMap::new(),
            vc_server_deafened: HashMap::new(),
            handles: HashMap::new(),
            member_roles: HashMap::new(),
        });
    }

    let self_records = record_data::Entity::find()
        .filter(record_data::Column::Did.is_in(member_dids.clone()))
        .filter(record_data::Column::Rkey.eq("self"))
        .filter(
            Condition::any()
                .add(record_data::Column::Nsid.eq("app.bsky.actor.profile"))
                .add(record_data::Column::Nsid.eq("social.colibri.actor.profile"))
                .add(record_data::Column::Nsid.eq("social.colibri.actor.data")),
        )
        .all(db)
        .await?;

    let mut profiles: HashMap<String, ActorProfile> = HashMap::new();
    let mut colibri_profiles: HashMap<String, ColibriActorProfile> = HashMap::new();
    let mut actor_data: HashMap<String, ColibriActorData> = HashMap::new();
    for record in self_records {
        if record.nsid == "app.bsky.actor.profile" {
            if let Ok(profile) = serde_json::from_value::<ActorProfile>(record.data) {
                profiles.insert(record.did, profile);
            }
        } else if record.nsid == "social.colibri.actor.profile" {
            if let Ok(profile) = serde_json::from_value::<ColibriActorProfile>(record.data) {
                colibri_profiles.insert(record.did, profile);
            }
        } else if record.nsid == "social.colibri.actor.data"
            && let Ok(data) = serde_json::from_value::<ColibriActorData>(record.data)
        {
            actor_data.insert(record.did, data);
        }
    }

    let state_rows = user_states::Entity::find()
        .filter(user_states::Column::Did.is_in(member_dids.clone()))
        .all(db)
        .await?;
    let mut states: HashMap<String, String> = HashMap::new();
    let mut vc: HashMap<String, String> = HashMap::new();
    let mut vc_muted: HashMap<String, bool> = HashMap::new();
    let mut vc_deafened: HashMap<String, bool> = HashMap::new();
    let mut vc_server_muted: HashMap<String, bool> = HashMap::new();
    let mut vc_server_deafened: HashMap<String, bool> = HashMap::new();
    for row in state_rows {
        if let Some(channel) = row.vc
            && row.vc_community.as_deref() == Some(community_uri)
        {
            let (server_muted, server_deafened) = moderation.get(&channel, &row.did).await;
            vc.insert(row.did.clone(), channel);
            vc_muted.insert(row.did.clone(), row.vc_muted.unwrap_or(false));
            vc_deafened.insert(row.did.clone(), row.vc_deafened.unwrap_or(false));
            vc_server_muted.insert(row.did.clone(), server_muted);
            vc_server_deafened.insert(row.did.clone(), server_deafened);
        }
        states.insert(row.did, row.state);
    }

    let repo_rows = repos::Entity::find()
        .filter(repos::Column::Did.is_in(member_dids.clone()))
        .all(db)
        .await?;
    let handles: HashMap<String, String> = repo_rows
        .into_iter()
        .filter_map(|row| row.handle.map(|h| (row.did, h)))
        .collect();

    Ok(MemberAggregate {
        member_dids,
        profiles,
        colibri_profiles,
        actor_data,
        states,
        vc,
        vc_muted,
        vc_deafened,
        vc_server_muted,
        vc_server_deafened,
        handles,
        member_roles,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn build_member(
    did: String,
    handle: Option<String>,
    profile: Option<ActorProfile>,
    colibri_profile: Option<ColibriActorProfile>,
    actor_data: Option<ColibriActorData>,
    state: Option<String>,
    community_authority: &str,
    role_rkeys: Vec<String>,
) -> Member {
    let handle = handle.unwrap_or_else(|| did.clone());

    // `is_bot` always comes from the Bluesky profile; the served profile fields
    // (name/avatar/banner/description/theme/syncBluesky) come from the effective
    // profile so a non-synced Colibri user shows their Colibri identity, matching
    // `getData` and the profile popover.
    let is_bot = profile.as_ref().is_some_and(ActorProfile::is_bot);
    let effective = resolve_effective_profile(colibri_profile.as_ref(), profile.as_ref());

    let status = actor_data
        .map(|d| ActorStatus {
            text: d.status.unwrap_or(String::from("")),
            emoji: d.emoji,
        })
        .unwrap_or(ActorStatus {
            text: String::new(),
            emoji: None,
        });

    let online_state = state.unwrap_or_else(|| String::from("offline"));

    let roles = role_rkeys
        .into_iter()
        .map(|rkey| format!("at://{}/social.colibri.role/{}", community_authority, rkey))
        .collect();

    Member {
        did,
        roles,
        data: actor_data_from_effective(effective, is_bot, &handle, online_state, status),
        handle,
        vc: None,
        vc_muted: None,
        vc_deafened: None,
        vc_server_muted: None,
        vc_server_deafened: None,
    }
}

type FetchAggregateFn = fn(
    DatabaseConnection,
    String,
    ModerationLookup,
) -> BoxFuture<'static, Result<MemberAggregate, DbErr>>;

pub fn take_voice_state(aggregate: &mut MemberAggregate, member: &mut Member) {
    let did = &member.did;
    member.vc = aggregate.vc.remove(did);
    member.vc_muted = aggregate.vc_muted.remove(did);
    member.vc_deafened = aggregate.vc_deafened.remove(did);
    member.vc_server_muted = aggregate.vc_server_muted.remove(did);
    member.vc_server_deafened = aggregate.vc_server_deafened.remove(did);
}

pub fn build_members(aggregate: &mut MemberAggregate, community_authority: &str) -> Vec<Member> {
    std::mem::take(&mut aggregate.member_dids)
        .into_iter()
        .map(|did| {
            let handle = aggregate.handles.remove(&did);
            let profile = aggregate.profiles.remove(&did);
            let colibri_profile = aggregate.colibri_profiles.remove(&did);
            let data = aggregate.actor_data.remove(&did);
            let state = aggregate.states.remove(&did);
            let role_rkeys = aggregate.member_roles.remove(&did).unwrap_or_default();
            let mut member = build_member(
                did,
                handle,
                profile,
                colibri_profile,
                data,
                state,
                community_authority,
                role_rkeys,
            );
            take_voice_state(aggregate, &mut member);
            member
        })
        .collect()
}

async fn list_members_with(
    community_uri: String,
    db: DatabaseConnection,
    moderation: ModerationLookup,
    fetch_aggregate_fn: FetchAggregateFn,
) -> Result<Json<MemberList>, ErrorResponse> {
    let community = AtUri::parse(&community_uri)
        .ok_or_else(|| ErrorCode::InvalidRequest.with("Invalid community AT-URI."))?;

    let mut aggregate = fetch_aggregate_fn(db, community_uri, moderation).await?;
    let members = build_members(&mut aggregate, &community.authority);

    Ok(Json(MemberList { members }))
}

fn fetch_aggregate_boxed(
    db: DatabaseConnection,
    community_uri: String,
    moderation: ModerationLookup,
) -> BoxFuture<'static, Result<MemberAggregate, DbErr>> {
    Box::pin(async move { fetch_member_aggregate(&db, &community_uri, &moderation).await })
}

#[get("/xrpc/social.colibri.community.listMembers?<community>")]
/// Lists every Colibri member of a community along with their profile, status,
/// and online state.
pub async fn list_members(
    community: &str,
    db: &State<DatabaseConnection>,
    moderation: &State<ModerationLookup>,
) -> Result<Json<MemberList>, ErrorResponse> {
    list_members_with(
        community.to_string(),
        db.inner().clone(),
        moderation.inner().clone(),
        fetch_aggregate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn profile_for(name: &str) -> ActorProfile {
        ActorProfile {
            display_name: Some(name.to_string()),
            description: Some(format!("{name} bio")),
            pronouns: None,
            website: None,
            avatar: Some(serde_json::json!({ "ref": format!("{name}-avatar") })),
            banner: None,
            labels: None,
            joined_via_starter_pack: None,
            pinned_post: None,
            created_at: None,
        }
    }

    fn actor_data_for(emoji: Option<&str>, status: &str) -> ColibriActorData {
        ColibriActorData {
            record_type: None,
            emoji: emoji.map(|e| e.to_string()),
            status: Some(status.to_string()),
            communities: vec![],
        }
    }

    const HOME: &str = "at://did:plc:owner/social.colibri.community/c1";
    const HOME_VC: &str = "at://did:plc:owner/social.colibri.channel/vc";
    const ELSEWHERE: &str = "at://did:plc:other/social.colibri.community/c1";
    const ELSEWHERE_VC: &str = "at://did:plc:other/social.colibri.channel/vc";

    fn member_record(id: i64, subject: &str) -> record_data::Model {
        record_data::Model {
            id,
            did: String::from("did:plc:owner"),
            nsid: String::from("social.colibri.member"),
            rkey: format!("m{id}"),
            data: serde_json::json!({ "subject": subject, "roles": [] }),
            indexed_at: String::from(""),
        }
    }

    fn vc_state(did: &str, vc: &str, community: &str) -> user_states::Model {
        user_states::Model {
            did: String::from(did),
            state: String::from("online"),
            manual_state: None,
            channel: None,
            vc: Some(String::from(vc)),
            vc_community: Some(String::from(community)),
            vc_muted: Some(true),
            vc_deafened: None,
        }
    }

    #[tokio::test]
    async fn omits_voice_state_from_another_community() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                member_record(1, "did:plc:alice"),
                member_record(2, "did:plc:bob"),
            ]])
            .append_query_results([Vec::<record_data::Model>::new()])
            .append_query_results([vec![
                vc_state("did:plc:alice", HOME_VC, HOME),
                vc_state("did:plc:bob", ELSEWHERE_VC, ELSEWHERE),
            ]])
            .append_query_results([Vec::<repos::Model>::new()])
            .into_connection();

        let aggregate = fetch_member_aggregate(&db, HOME, &ModerationLookup::default())
            .await
            .unwrap();

        assert_eq!(
            aggregate.vc.get("did:plc:alice").map(String::as_str),
            Some(HOME_VC)
        );
        assert_eq!(aggregate.vc.get("did:plc:bob"), None);
        assert_eq!(aggregate.vc_muted.get("did:plc:alice"), Some(&true));
        assert_eq!(aggregate.vc_muted.get("did:plc:bob"), None);
        assert_eq!(aggregate.member_dids.len(), 2);
        assert_eq!(aggregate.states.len(), 2);
    }

    #[tokio::test]
    async fn surfaces_server_mute_from_the_moderation_lookup() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![member_record(1, "did:plc:alice")]])
            .append_query_results([Vec::<record_data::Model>::new()])
            .append_query_results([vec![vc_state("did:plc:alice", HOME_VC, HOME)]])
            .append_query_results([Vec::<repos::Model>::new()])
            .into_connection();

        let moderation =
            ModerationLookup::new(std::sync::Arc::new(|channel: String, did: String| {
                Box::pin(async move { (channel == HOME_VC && did == "did:plc:alice", true) })
            }));

        let aggregate = fetch_member_aggregate(&db, HOME, &moderation)
            .await
            .unwrap();

        assert_eq!(aggregate.vc_server_muted.get("did:plc:alice"), Some(&true));
        assert_eq!(
            aggregate.vc_server_deafened.get("did:plc:alice"),
            Some(&true)
        );
    }

    #[tokio::test]
    async fn does_not_report_server_mute_for_members_outside_voice() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![member_record(1, "did:plc:alice")]])
            .append_query_results([Vec::<record_data::Model>::new()])
            .append_query_results([vec![user_states::Model {
                did: String::from("did:plc:alice"),
                state: String::from("online"),
                manual_state: None,
                channel: None,
                vc: None,
                vc_community: None,
                vc_muted: None,
                vc_deafened: None,
            }]])
            .append_query_results([Vec::<repos::Model>::new()])
            .into_connection();

        let moderation = ModerationLookup::new(std::sync::Arc::new(|_, _| {
            Box::pin(async { panic!("should not be consulted for a member outside voice") })
        }));

        let aggregate = fetch_member_aggregate(&db, HOME, &moderation)
            .await
            .unwrap();

        assert!(aggregate.vc.is_empty());
        assert!(aggregate.vc_server_muted.is_empty());
    }

    /// A non-synced Colibri profile: owns its mirrored fields and theme.
    fn unsynced_colibri_profile(display_name: &str) -> ColibriActorProfile {
        ColibriActorProfile {
            display_name: Some(display_name.to_string()),
            description: Some(format!("{display_name} colibri bio")),
            avatar: Some(serde_json::json!({ "ref": "colibri-avatar" })),
            sync_bluesky: false,
            theme: Some(crate::lib::colibri::ColibriProfileTheme {
                accent_color: Some(String::from("#ff0000")),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn assembles_members_from_aggregate() {
        let db = mock_db();
        let result = list_members_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            ModerationLookup::default(),
            |_, _, _| {
                Box::pin(async {
                    Ok(MemberAggregate {
                        member_dids: vec![
                            String::from("did:plc:alice"),
                            String::from("did:plc:bob"),
                        ],
                        profiles: HashMap::from([
                            (String::from("did:plc:alice"), profile_for("Alice")),
                            (String::from("did:plc:bob"), profile_for("Bob")),
                        ]),
                        colibri_profiles: HashMap::new(),
                        actor_data: HashMap::from([(
                            String::from("did:plc:alice"),
                            actor_data_for(Some("🦜"), "Working"),
                        )]),
                        states: HashMap::from([(
                            String::from("did:plc:alice"),
                            String::from("online"),
                        )]),
                        vc: HashMap::new(),
                        vc_muted: HashMap::new(),
                        vc_deafened: HashMap::new(),
                        vc_server_muted: HashMap::new(),
                        vc_server_deafened: HashMap::new(),
                        handles: HashMap::from([
                            (String::from("did:plc:alice"), String::from("alice.test")),
                            (String::from("did:plc:bob"), String::from("bob.test")),
                        ]),
                        member_roles: HashMap::new(),
                    })
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.members.len(), 2);
        let alice = result
            .members
            .iter()
            .find(|m| m.did == "did:plc:alice")
            .unwrap();
        assert_eq!(alice.handle, "alice.test");
        assert_eq!(alice.data.display_name, "Alice");
        assert_eq!(alice.data.online_state, "online");
        assert_eq!(alice.data.status.text, "Working");
        assert_eq!(alice.data.status.emoji.as_deref(), Some("🦜"));
        assert!(alice.roles.is_empty());

        let bob = result
            .members
            .iter()
            .find(|m| m.did == "did:plc:bob")
            .unwrap();
        assert_eq!(bob.data.online_state, "offline");
        assert_eq!(bob.data.status.text, "");
    }

    #[tokio::test]
    async fn expands_role_rkeys_to_uris_per_member() {
        let db = mock_db();
        let result = list_members_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            ModerationLookup::default(),
            |_, _, _| {
                Box::pin(async {
                    Ok(MemberAggregate {
                        member_dids: vec![String::from("did:plc:alice")],
                        profiles: HashMap::new(),
                        colibri_profiles: HashMap::new(),
                        actor_data: HashMap::new(),
                        states: HashMap::new(),
                        vc: HashMap::new(),
                        vc_muted: HashMap::new(),
                        vc_deafened: HashMap::new(),
                        vc_server_muted: HashMap::new(),
                        vc_server_deafened: HashMap::new(),
                        handles: HashMap::new(),
                        member_roles: HashMap::from([(
                            String::from("did:plc:alice"),
                            vec![String::from("owner"), String::from("mod")],
                        )]),
                    })
                })
            },
        )
        .await
        .unwrap();

        let alice = &result.members[0];
        assert_eq!(
            alice.roles,
            vec![
                "at://did:plc:owner/social.colibri.role/owner",
                "at://did:plc:owner/social.colibri.role/mod",
            ]
        );
    }

    #[tokio::test]
    async fn falls_back_to_did_when_handle_and_profile_missing() {
        let member = build_member(
            String::from("did:plc:alice"),
            None,
            None,
            None,
            None,
            None,
            "did:plc:owner",
            vec![],
        );
        assert_eq!(member.handle, "did:plc:alice");
        assert_eq!(member.data.display_name, "did:plc:alice");
        assert_eq!(member.data.online_state, "offline");
        assert_eq!(member.data.status.text, "");
        assert!(member.data.status.emoji.is_none());
        assert!(member.roles.is_empty());
        assert!(!member.data.sync_bluesky);
        assert!(member.data.theme.is_none());
    }

    // The three effective-profile cases below mirror the `get_data_handler`
    // tests so member-list enrichment stays consistent with the profile popover.

    #[tokio::test]
    async fn non_synced_member_uses_colibri_profile_and_theme() {
        let member = build_member(
            String::from("did:plc:alice"),
            Some(String::from("alice.test")),
            Some(profile_for("Bsky Alice")),
            Some(unsynced_colibri_profile("Colibri Alice")),
            None,
            None,
            "did:plc:owner",
            vec![],
        );
        assert_eq!(member.data.display_name, "Colibri Alice");
        assert_eq!(
            member.data.description.as_deref(),
            Some("Colibri Alice colibri bio")
        );
        assert!(!member.data.sync_bluesky);
        assert_eq!(
            member.data.theme.unwrap().accent_color.as_deref(),
            Some("#ff0000")
        );
    }

    #[tokio::test]
    async fn synced_member_uses_bsky_fields_but_colibri_theme() {
        let colibri = ColibriActorProfile {
            sync_bluesky: true,
            theme: Some(crate::lib::colibri::ColibriProfileTheme {
                banner_color: Some(String::from("#00ff00")),
                ..Default::default()
            }),
            ..Default::default()
        };
        let member = build_member(
            String::from("did:plc:alice"),
            Some(String::from("alice.test")),
            Some(profile_for("Bsky Alice")),
            Some(colibri),
            None,
            None,
            "did:plc:owner",
            vec![],
        );
        assert_eq!(member.data.display_name, "Bsky Alice");
        assert!(member.data.sync_bluesky);
        assert_eq!(
            member.data.theme.unwrap().banner_color.as_deref(),
            Some("#00ff00")
        );
    }

    #[tokio::test]
    async fn un_onboarded_member_falls_back_to_bsky() {
        let member = build_member(
            String::from("did:plc:alice"),
            Some(String::from("alice.test")),
            Some(profile_for("Bsky Alice")),
            None,
            None,
            None,
            "did:plc:owner",
            vec![],
        );
        assert_eq!(member.data.display_name, "Bsky Alice");
        assert!(!member.data.sync_bluesky);
        assert!(member.data.theme.is_none());
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = mock_db();
        let result = list_members_with(
            String::from("not-a-uri"),
            db,
            ModerationLookup::default(),
            |_, _, _| Box::pin(async { panic!("should not fetch when uri is invalid") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
