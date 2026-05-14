use std::collections::HashMap;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::lib::at_uri::AtUri;
use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::ColibriActorData;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::{record_data, repos, user_states};
use crate::xrpc::social::colibri::actor::get_data_handler::{ActorData, ActorStatus};

#[derive(Serialize, Deserialize, Debug)]
pub struct Member {
    pub did: String,
    pub handle: String,
    pub data: ActorData,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MemberList {
    pub members: Vec<Member>,
}

/// Data fetched in a single round-trip per backing table, keyed by member DID.
pub struct MemberAggregate {
    pub member_dids: Vec<String>,
    pub profiles: HashMap<String, ActorProfile>,
    pub actor_data: HashMap<String, ColibriActorData>,
    pub states: HashMap<String, String>,
    pub handles: HashMap<String, String>,
}

pub async fn fetch_member_aggregate(
    db: &DatabaseConnection,
    community_uri: &str,
) -> Result<MemberAggregate, DbErr> {
    let memberships = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq("social.colibri.membership"))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(community_uri.to_string())],
        ))
        .all(db)
        .await?;

    let mut member_dids: Vec<String> = memberships.into_iter().map(|m| m.did).collect();
    member_dids.sort();
    member_dids.dedup();

    if member_dids.is_empty() {
        return Ok(MemberAggregate {
            member_dids,
            profiles: HashMap::new(),
            actor_data: HashMap::new(),
            states: HashMap::new(),
            handles: HashMap::new(),
        });
    }

    let self_records = record_data::Entity::find()
        .filter(record_data::Column::Did.is_in(member_dids.clone()))
        .filter(record_data::Column::Rkey.eq("self"))
        .filter(
            Condition::any()
                .add(record_data::Column::Nsid.eq("app.bsky.actor.profile"))
                .add(record_data::Column::Nsid.eq("social.colibri.actor.data")),
        )
        .all(db)
        .await?;

    let mut profiles: HashMap<String, ActorProfile> = HashMap::new();
    let mut actor_data: HashMap<String, ColibriActorData> = HashMap::new();
    for record in self_records {
        if record.nsid == "app.bsky.actor.profile" {
            if let Ok(profile) = serde_json::from_value::<ActorProfile>(record.data) {
                profiles.insert(record.did, profile);
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
    let states: HashMap<String, String> = state_rows
        .into_iter()
        .map(|row| (row.did, row.state))
        .collect();

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
        actor_data,
        states,
        handles,
    })
}

pub fn build_member(
    did: String,
    handle: Option<String>,
    profile: Option<ActorProfile>,
    actor_data: Option<ColibriActorData>,
    state: Option<String>,
) -> Member {
    let handle = handle.unwrap_or_else(|| did.clone());
    let display_name = profile
        .as_ref()
        .and_then(|p| p.display_name.clone())
        .unwrap_or_else(|| handle.clone());

    let (avatar, banner, description): (Option<Value>, Option<Value>, Option<String>) =
        match profile {
            Some(p) => (p.avatar, p.banner, p.description),
            None => (None, None, None),
        };

    let status = actor_data
        .map(|d| ActorStatus {
            text: d.status,
            emoji: d.emoji,
        })
        .unwrap_or(ActorStatus {
            text: String::new(),
            emoji: None,
        });

    let online_state = state.unwrap_or_else(|| String::from("offline"));

    Member {
        did,
        handle,
        data: ActorData {
            display_name,
            avatar,
            banner,
            description,
            online_state,
            status,
        },
    }
}

type FetchAggregateFn =
    fn(DatabaseConnection, String) -> BoxFuture<'static, Result<MemberAggregate, DbErr>>;

async fn list_members_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_aggregate_fn: FetchAggregateFn,
) -> Result<Json<MemberList>, ErrorResponse> {
    if AtUri::parse(&community_uri).is_none() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invalid community AT-URI."),
            }),
        });
    }

    let aggregate = fetch_aggregate_fn(db, community_uri).await?;

    let MemberAggregate {
        member_dids,
        mut profiles,
        mut actor_data,
        mut states,
        mut handles,
    } = aggregate;

    let members = member_dids
        .into_iter()
        .map(|did| {
            let handle = handles.remove(&did);
            let profile = profiles.remove(&did);
            let data = actor_data.remove(&did);
            let state = states.remove(&did);
            build_member(did, handle, profile, data, state)
        })
        .collect();

    Ok(Json(MemberList { members }))
}

fn fetch_aggregate_boxed(
    db: DatabaseConnection,
    community_uri: String,
) -> BoxFuture<'static, Result<MemberAggregate, DbErr>> {
    Box::pin(async move { fetch_member_aggregate(&db, &community_uri).await })
}

#[get("/xrpc/social.colibri.community.listMembers?<community>")]
/// Lists every Colibri member of a community along with their profile, status,
/// and online state.
pub async fn list_members(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<MemberList>, ErrorResponse> {
    list_members_with(
        community.to_string(),
        db.inner().clone(),
        fetch_aggregate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
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
            status: status.to_string(),
            communities: vec![],
        }
    }

    #[tokio::test]
    async fn assembles_members_from_aggregate() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_members_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            |_, _| {
                Box::pin(async {
                    Ok(MemberAggregate {
                        member_dids: vec![String::from("did:plc:alice"), String::from("did:plc:bob")],
                        profiles: HashMap::from([
                            (String::from("did:plc:alice"), profile_for("Alice")),
                            (String::from("did:plc:bob"), profile_for("Bob")),
                        ]),
                        actor_data: HashMap::from([(
                            String::from("did:plc:alice"),
                            actor_data_for(Some("🦜"), "Working"),
                        )]),
                        states: HashMap::from([(String::from("did:plc:alice"), String::from("online"))]),
                        handles: HashMap::from([
                            (String::from("did:plc:alice"), String::from("alice.test")),
                            (String::from("did:plc:bob"), String::from("bob.test")),
                        ]),
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

        let bob = result
            .members
            .iter()
            .find(|m| m.did == "did:plc:bob")
            .unwrap();
        assert_eq!(bob.data.online_state, "offline");
        assert_eq!(bob.data.status.text, "");
    }

    #[tokio::test]
    async fn falls_back_to_did_when_handle_and_profile_missing() {
        let member = build_member(String::from("did:plc:alice"), None, None, None, None);
        assert_eq!(member.handle, "did:plc:alice");
        assert_eq!(member.data.display_name, "did:plc:alice");
        assert_eq!(member.data.online_state, "offline");
        assert_eq!(member.data.status.text, "");
        assert!(member.data.status.emoji.is_none());
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_members_with(
            String::from("not-a-uri"),
            db,
            |_, _| Box::pin(async { panic!("should not fetch when uri is invalid") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
