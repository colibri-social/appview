use futures::future::BoxFuture;
use sea_orm::DatabaseConnection;
use serde::de::{DeserializeOwned, Error};

use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{
    ColibriActorData, ColibriCategory, ColibriChannel, ColibriCommunity, ColibriMember,
    ColibriMembership, ColibriMessage, ColibriReaction,
};
use crate::lib::events::{
    CategoryEventData, ChannelEventData, ColibriServerEvent, ColibriServerEventData,
    CommunityEventData, MemberEventData, MemberEventMember, MemberEventMemberData,
    MemberEventMemberStatus, MessageEventAuthor, MessageEventAuthorData, MessageEventAuthorStatus,
    MessageEventData, ReactionEventData, UserEventData, UserEventProfile, UserEventStatus,
};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::get_state::get_state;
use crate::lib::tap::TapMessageRecord;
use crate::xrpc::com::atproto::identity::resolve_did;

type FetchRecordFn = dyn Fn(
        String,
        String,
        String,
        DatabaseConnection,
    ) -> BoxFuture<'static, Result<serde_json::Value, serde_json::Error>>
    + Send
    + Sync;
type ResolveHandleFn =
    dyn Fn(String) -> BoxFuture<'static, Result<String, serde_json::Error>> + Send + Sync;
type GetStateFn = dyn Fn(String, DatabaseConnection) -> BoxFuture<'static, Result<String, serde_json::Error>>
    + Send
    + Sync;

fn make_event_uri(event_record: &TapMessageRecord) -> String {
    format!(
        "at://{}/{}/{}",
        event_record.did, event_record.collection, event_record.rkey
    )
}

fn parse_payload<T: DeserializeOwned>(
    event_record: &TapMessageRecord,
) -> Result<T, serde_json::Error> {
    let payload = event_record
        .record
        .clone()
        .ok_or_else(|| serde_json::Error::custom("Missing record payload"))?;
    serde_json::from_value(payload)
}

fn irrelevant_event() -> ColibriServerEvent {
    ColibriServerEvent {
        event_type: String::from("empty"),
        data: None,
        is_relevant: false,
    }
}

async fn fetch_record_value(
    did: String,
    nsid: String,
    rkey: String,
    db: DatabaseConnection,
) -> Result<serde_json::Value, serde_json::Error> {
    get_atproto_record::<serde_json::Value>(did, nsid, rkey, &db)
        .await
        .map_err(|e| serde_json::Error::custom(e.to_string()))
}

async fn resolve_handle_for_did(did: String) -> Result<String, serde_json::Error> {
    let doc = resolve_did(&did)
        .await
        .map_err(|e| serde_json::Error::custom(e.body.into_inner().message))?
        .0;

    let aka = doc
        .also_known_as
        .ok_or_else(|| serde_json::Error::custom("Unable to get handle"))?;

    aka.first()
        .cloned()
        .ok_or_else(|| serde_json::Error::custom("Unable to get handle"))
}

async fn get_user_state(did: String, db: DatabaseConnection) -> Result<String, serde_json::Error> {
    get_state(did, &db)
        .await
        .map(|s| s.to_string())
        .map_err(|e| serde_json::Error::custom(e.to_string()))
}

/// Builds a fully-enriched [`MemberEventMember`] for the given subject, using
/// best-effort profile/state lookups. Individual fetch failures fall back to
/// safe defaults so a missing profile never drops the event.
async fn build_member_event_member(
    subject_did: &str,
    member: &ColibriMember,
    db: DatabaseConnection,
    fetch_record_fn: &FetchRecordFn,
    resolve_handle_fn: &ResolveHandleFn,
    get_state_fn: &GetStateFn,
) -> MemberEventMember {
    let handle = resolve_handle_fn(subject_did.to_string())
        .await
        .unwrap_or_else(|_| subject_did.to_string());

    let state = get_state_fn(subject_did.to_string(), db.clone())
        .await
        .unwrap_or_else(|_| String::from("offline"));

    let profile = fetch_record_fn(
        subject_did.to_string(),
        String::from("app.bsky.actor.profile"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ActorProfile>(v).ok());

    let actor_data = fetch_record_fn(
        subject_did.to_string(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok());

    MemberEventMember {
        did: subject_did.to_string(),
        handle,
        roles: member.roles.clone(),
        joined_at: member.joined_at.clone(),
        nickname: member.nickname.clone(),
        data: MemberEventMemberData {
            display_name: profile
                .as_ref()
                .and_then(|p| p.display_name.clone())
                .unwrap_or_default(),
            avatar: profile.as_ref().and_then(|p| p.avatar.clone()),
            banner: profile.as_ref().and_then(|p| p.banner.clone()),
            description: profile.as_ref().and_then(|p| p.description.clone()),
            online_state: state,
            status: MemberEventMemberStatus {
                text: actor_data
                    .as_ref()
                    .map(|d| d.status.clone())
                    .unwrap_or_default(),
                emoji: actor_data.as_ref().and_then(|d| d.emoji.clone()),
            },
        },
    }
}

async fn map_tap_event_with(
    event_record: &TapMessageRecord,
    user_did: &str,
    db: DatabaseConnection,
    fetch_record_fn: &FetchRecordFn,
    resolve_handle_fn: &ResolveHandleFn,
    get_state_fn: &GetStateFn,
) -> Result<ColibriServerEvent, serde_json::Error> {
    let uri = make_event_uri(event_record);

    match event_record.collection.as_str() {
        "social.colibri.community" => {
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriCommunity>(event_record)?;
                Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::Community(CommunityEventData {
                        event: String::from("upsert"),
                        uri,
                        category_order: Some(record_data.category_order),
                        description: Some(record_data.description),
                        name: Some(record_data.name),
                        picture: record_data.picture,
                    })),
                    is_relevant: true,
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::Community(CommunityEventData {
                        event: String::from("delete"),
                        uri,
                        category_order: None,
                        description: None,
                        name: None,
                        picture: None,
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.membership" => {
            // Membership is intent recorded on the user's own repo. It does
            // NOT broadcast: the authoritative "you're now in" / "you're now
            // out" signal is the community-side `social.colibri.member`
            // record, handled below.
            //
            // The one exception is membership-delete for the caller — we
            // emit a community-delete event immediately so the user gets
            // instant UX feedback on self-leave, without waiting for the
            // AppView's downstream member-record revoke to round-trip
            // through the firehose. The eventual member-delete event still
            // arrives but maps to a no-op on the client (idempotent).
            if event_record.action == "delete" && event_record.did == user_did {
                let old_record = fetch_record_fn(
                    event_record.did.clone(),
                    event_record.collection.clone(),
                    event_record.rkey.clone(),
                    db.clone(),
                )
                .await?;
                let safe_record = serde_json::from_value::<ColibriMembership>(old_record)?;

                return Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::Community(CommunityEventData {
                        event: String::from("delete"),
                        uri: safe_record.community,
                        category_order: None,
                        description: None,
                        name: None,
                        picture: None,
                    })),
                    is_relevant: true,
                });
            }

            Ok(irrelevant_event())
        }
        "social.colibri.member" => {
            // `member` is the source of truth for community membership.
            // Both join and role-update events are broadcast to ALL connected
            // clients. The client checks `member.did` against its own DID to
            // know when it has been admitted, and checks `community` to scope
            // updates to the right community.
            if event_record.action == "create" {
                let record_data = parse_payload::<ColibriMember>(event_record)?;
                let community_uri =
                    format!("at://{}/social.colibri.community/self", event_record.did);
                let member = build_member_event_member(
                    &record_data.subject.clone(),
                    &record_data,
                    db.clone(),
                    fetch_record_fn,
                    resolve_handle_fn,
                    get_state_fn,
                )
                .await;
                return Ok(ColibriServerEvent {
                    event_type: String::from("member_event"),
                    data: Some(ColibriServerEventData::Member(MemberEventData {
                        event: String::from("join"),
                        community: community_uri,
                        membership: record_data.from_membership,
                        member: Some(member),
                    })),
                    is_relevant: true,
                });
            }

            if event_record.action == "update" {
                let record_data = parse_payload::<ColibriMember>(event_record)?;
                let community_uri =
                    format!("at://{}/social.colibri.community/self", event_record.did);
                let member = build_member_event_member(
                    &record_data.subject.clone(),
                    &record_data,
                    db.clone(),
                    fetch_record_fn,
                    resolve_handle_fn,
                    get_state_fn,
                )
                .await;
                return Ok(ColibriServerEvent {
                    event_type: String::from("member_event"),
                    data: Some(ColibriServerEventData::Member(MemberEventData {
                        event: String::from("roles_updated"),
                        community: community_uri,
                        membership: None,
                        member: Some(member),
                    })),
                    is_relevant: true,
                });
            }

            // Delete → community delete to the subject. Look up the prior
            // member record from cache (still present at this point) to
            // identify which user the revoke targeted.
            let old_record = fetch_record_fn(
                event_record.did.clone(),
                event_record.collection.clone(),
                event_record.rkey.clone(),
                db.clone(),
            )
            .await?;
            let safe_record = serde_json::from_value::<ColibriMember>(old_record)?;
            if safe_record.subject != user_did {
                return Ok(irrelevant_event());
            }
            let community_uri = format!("at://{}/social.colibri.community/self", event_record.did);
            Ok(ColibriServerEvent {
                event_type: String::from("community_event"),
                data: Some(ColibriServerEventData::Community(CommunityEventData {
                    event: String::from("delete"),
                    uri: community_uri,
                    category_order: None,
                    description: None,
                    name: None,
                    picture: None,
                })),
                is_relevant: true,
            })
        }
        "social.colibri.category" => {
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriCategory>(event_record)?;
                Ok(ColibriServerEvent {
                    event_type: String::from("category_event"),
                    data: Some(ColibriServerEventData::Category(CategoryEventData {
                        event: String::from("upsert"),
                        uri,
                        channel_order: Some(record_data.channel_order),
                        community: Some(record_data.community),
                        name: Some(record_data.name),
                    })),
                    is_relevant: true,
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("category_event"),
                    data: Some(ColibriServerEventData::Category(CategoryEventData {
                        event: String::from("delete"),
                        uri,
                        channel_order: None,
                        community: None,
                        name: None,
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.channel" => {
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriChannel>(event_record)?;
                Ok(ColibriServerEvent {
                    event_type: String::from("channel_event"),
                    data: Some(ColibriServerEventData::Channel(ChannelEventData {
                        event: String::from("upsert"),
                        uri,
                        channel_type: Some(record_data.channel_type),
                        community: Some(record_data.community),
                        category: Some(record_data.category),
                        description: record_data.description,
                        name: Some(record_data.name),
                    })),
                    is_relevant: true,
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("channel_event"),
                    data: Some(ColibriServerEventData::Channel(ChannelEventData {
                        event: String::from("delete"),
                        uri,
                        channel_type: None,
                        community: None,
                        category: None,
                        description: None,
                        name: None,
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.message" => {
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriMessage>(event_record)?;

                // Best-effort author enrichment: individual fetch failures fall
                // back to safe defaults so a missing profile never drops the event.
                let handle = resolve_handle_fn(event_record.did.clone())
                    .await
                    .unwrap_or_else(|_| event_record.did.clone());

                let state = get_state_fn(event_record.did.clone(), db.clone())
                    .await
                    .unwrap_or_else(|_| String::from("offline"));

                let profile = fetch_record_fn(
                    event_record.did.clone(),
                    String::from("app.bsky.actor.profile"),
                    String::from("self"),
                    db.clone(),
                )
                .await
                .ok()
                .and_then(|v| serde_json::from_value::<ActorProfile>(v).ok());

                let actor_data = fetch_record_fn(
                    event_record.did.clone(),
                    String::from("social.colibri.actor.data"),
                    String::from("self"),
                    db.clone(),
                )
                .await
                .ok()
                .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok());

                let author = MessageEventAuthor {
                    did: event_record.did.clone(),
                    handle,
                    data: MessageEventAuthorData {
                        display_name: profile
                            .as_ref()
                            .and_then(|p| p.display_name.clone())
                            .unwrap_or_default(),
                        avatar: profile.as_ref().and_then(|p| p.avatar.clone()),
                        banner: profile.as_ref().and_then(|p| p.banner.clone()),
                        description: profile.as_ref().and_then(|p| p.description.clone()),
                        online_state: state,
                        status: MessageEventAuthorStatus {
                            text: actor_data
                                .as_ref()
                                .map(|d| d.status.clone())
                                .unwrap_or_default(),
                            emoji: actor_data.as_ref().and_then(|d| d.emoji.clone()),
                        },
                    },
                };

                Ok(ColibriServerEvent {
                    event_type: String::from("message_event"),
                    data: Some(ColibriServerEventData::Message(MessageEventData {
                        event: String::from("upsert"),
                        uri,
                        attachments: record_data.attachments,
                        channel: Some(record_data.channel),
                        created_at: Some(record_data.created_at),
                        edited: record_data.edited,
                        facets: record_data.facets,
                        parent: record_data.parent,
                        text: Some(record_data.text),
                        author: Some(author),
                    })),
                    is_relevant: true,
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("message_event"),
                    data: Some(ColibriServerEventData::Message(MessageEventData {
                        event: String::from("delete"),
                        uri,
                        attachments: None,
                        channel: None,
                        created_at: None,
                        edited: None,
                        facets: None,
                        parent: None,
                        text: None,
                        author: None,
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.reaction" => {
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriReaction>(event_record)?;
                Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::Reaction(ReactionEventData {
                        event: String::from("added"),
                        uri,
                        emoji: Some(record_data.emoji),
                        target: Some(record_data.parent),
                    })),
                    is_relevant: true,
                })
            } else {
                // The tap pipeline broadcasts first then deletes, so the
                // cached reaction record is still present at this point.
                // Fetch it so we can include emoji + target in the removed
                // event — without them the client can't know which reaction
                // to decrement.
                let cached = fetch_record_fn(
                    event_record.did.clone(),
                    event_record.collection.clone(),
                    event_record.rkey.clone(),
                    db.clone(),
                )
                .await
                .ok()
                .and_then(|v| serde_json::from_value::<ColibriReaction>(v).ok());

                Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::Reaction(ReactionEventData {
                        event: String::from("removed"),
                        uri,
                        emoji: cached.as_ref().map(|r| r.emoji.clone()),
                        target: cached.map(|r| r.parent),
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.actor.data" => {
            let bsky_profile = fetch_record_fn(
                event_record.did.clone(),
                String::from("app.bsky.actor.profile"),
                event_record.rkey.clone(),
                db.clone(),
            )
            .await?;
            let safe_profile = serde_json::from_value::<ActorProfile>(bsky_profile)?;

            let safe_actor_data = parse_payload::<ColibriActorData>(event_record)?;
            let handle = resolve_handle_fn(event_record.did.clone()).await?;
            let state = get_state_fn(event_record.did.clone(), db.clone()).await?;

            Ok(ColibriServerEvent {
                event_type: String::from("user_event"),
                data: Some(ColibriServerEventData::User(UserEventData {
                    did: event_record.did.clone(),
                    profile: UserEventProfile {
                        avatar: safe_profile.avatar,
                        banner: safe_profile.banner,
                        description: safe_profile.description,
                        display_name: safe_profile.display_name,
                        handle,
                    },
                    status: Some(UserEventStatus {
                        emoji: safe_actor_data.emoji,
                        state,
                        text: safe_actor_data.status,
                    }),
                })),
                is_relevant: true,
            })
        }
        "app.bsky.actor.profile" => {
            let colibri_data = fetch_record_fn(
                event_record.did.clone(),
                String::from("social.colibri.actor.data"),
                event_record.rkey.clone(),
                db.clone(),
            )
            .await?;
            let safe_colibri_data = serde_json::from_value::<ColibriActorData>(colibri_data)?;
            let safe_profile = parse_payload::<ActorProfile>(event_record)?;

            let handle = resolve_handle_fn(event_record.did.clone()).await?;
            let state = get_state_fn(event_record.did.clone(), db.clone()).await?;

            Ok(ColibriServerEvent {
                event_type: String::from("user_event"),
                data: Some(ColibriServerEventData::User(UserEventData {
                    did: event_record.did.clone(),
                    profile: UserEventProfile {
                        avatar: safe_profile.avatar,
                        banner: safe_profile.banner,
                        description: safe_profile.description,
                        display_name: safe_profile.display_name,
                        handle,
                    },
                    status: Some(UserEventStatus {
                        emoji: safe_colibri_data.emoji,
                        state,
                        text: safe_colibri_data.status,
                    }),
                })),
                is_relevant: true,
            })
        }
        "social.colibri.richtext.facet" => Err(serde_json::Error::custom("Facet")),
        _ => Err(serde_json::Error::custom("Unknown collection")),
    }
}

pub async fn map_tap_event(
    event_record: &TapMessageRecord,
    user_did: &str,
    db: &DatabaseConnection,
) -> Result<ColibriServerEvent, serde_json::Error> {
    map_tap_event_with(
        event_record,
        user_did,
        db.clone(),
        &|did, nsid, rkey, db| Box::pin(fetch_record_value(did, nsid, rkey, db)),
        &|did| Box::pin(resolve_handle_for_did(did)),
        &|did, db| Box::pin(get_user_state(did, db)),
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use serde_json::json;

    use super::*;

    fn record(collection: &str, action: &str, value: serde_json::Value) -> TapMessageRecord {
        TapMessageRecord {
            live: true,
            did: String::from("did:plc:abc"),
            rev: String::from("1"),
            collection: collection.to_string(),
            rkey: String::from("r1"),
            action: action.to_string(),
            record: Some(value),
            cid: Some(String::from("cid")),
        }
    }

    fn no_fetch(
        _did: String,
        _nsid: String,
        _rkey: String,
        _db: DatabaseConnection,
    ) -> BoxFuture<'static, Result<serde_json::Value, serde_json::Error>> {
        Box::pin(async { panic!("fetch_record_fn should not be called") })
    }

    fn no_resolve(_did: String) -> BoxFuture<'static, Result<String, serde_json::Error>> {
        Box::pin(async { panic!("resolve_handle_fn should not be called") })
    }

    fn no_state(
        _did: String,
        _db: DatabaseConnection,
    ) -> BoxFuture<'static, Result<String, serde_json::Error>> {
        Box::pin(async { panic!("get_state_fn should not be called") })
    }

    #[tokio::test]
    async fn maps_community_upsert_event() {
        let event = map_tap_event_with(
            &record(
                "social.colibri.community",
                "create",
                json!({
                    "$type": "social.colibri.community",
                    "name": "General",
                    "description": "desc",
                    "categoryOrder": ["cat1"],
                    "requiresApprovalToJoin": false
                }),
            ),
            "",
            mock_db(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert_eq!(event.event_type, "community_event");
        assert!(event.is_relevant);
    }

    #[tokio::test]
    async fn maps_membership_delete_for_current_user() {
        let event = map_tap_event_with(
            &record("social.colibri.membership", "delete", json!({})),
            "did:plc:abc",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    if nsid == "social.colibri.membership" {
                        Ok(json!({
                            "$type": "social.colibri.membership",
                            "community": "at://did:plc:owner/social.colibri.community/community1",
                            "createdAt": "2026-01-01T00:00:00Z"
                        }))
                    } else {
                        Err(serde_json::Error::custom("unexpected nsid"))
                    }
                })
            },
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Community(data)) = event.data {
            assert_eq!(data.event, "delete");
            assert_eq!(
                data.uri,
                "at://did:plc:owner/social.colibri.community/community1"
            );
        } else {
            panic!("expected community event");
        }
    }

    #[tokio::test]
    async fn membership_create_is_irrelevant_in_new_model() {
        // Intent-only — broadcasts moved to `social.colibri.member` creates.
        let event = map_tap_event_with(
            &record(
                "social.colibri.membership",
                "create",
                json!({
                    "$type":"social.colibri.membership",
                    "community":"at://did:plc:owner/social.colibri.community/self",
                    "createdAt":"2026-01-01T00:00:00Z"
                }),
            ),
            "did:plc:abc",
            mock_db(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert_eq!(event.event_type, "empty");
        assert!(!event.is_relevant);
    }

    #[tokio::test]
    async fn maps_member_create_to_member_event_with_author_for_subject() {
        let event = map_tap_event_with(
            &TapMessageRecord {
                live: true,
                did: String::from("did:plc:community"),
                rev: String::from("1"),
                collection: String::from("social.colibri.member"),
                rkey: String::from("member-1"),
                action: String::from("create"),
                record: Some(json!({
                    "$type": "social.colibri.member",
                    "subject": "did:plc:abc",
                    "roles": ["owner-role"],
                    "joinedAt": "2026-01-01T00:00:00Z",
                    "fromMembership": "at://did:plc:abc/social.colibri.membership/m1"
                })),
                cid: Some(String::from("cid")),
            },
            "did:plc:abc",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(json!({
                            "displayName": "Alice",
                            "avatar": { "ref": "blob1" }
                        })),
                        "social.colibri.actor.data" => Ok(json!({
                            "$type": "social.colibri.actor.data",
                            "status": "Coding",
                            "emoji": "💻",
                            "communities": []
                        })),
                        _ => Err(serde_json::Error::custom("unexpected nsid")),
                    }
                })
            },
            &|_| Box::pin(async { Ok(String::from("alice.test")) }),
            &|_, _| Box::pin(async { Ok(String::from("online")) }),
        )
        .await
        .unwrap();

        assert!(event.is_relevant);
        if let Some(ColibriServerEventData::Member(data)) = event.data {
            assert_eq!(data.event, "join");
            assert_eq!(
                data.community,
                "at://did:plc:community/social.colibri.community/self"
            );
            assert_eq!(
                data.membership.as_deref(),
                Some("at://did:plc:abc/social.colibri.membership/m1")
            );
            let member = data.member.expect("member should be present");
            assert_eq!(member.did, "did:plc:abc");
            assert_eq!(member.handle, "alice.test");
            assert_eq!(member.roles, vec!["owner-role"]);
            assert_eq!(member.joined_at, "2026-01-01T00:00:00Z");
            assert_eq!(member.data.display_name, "Alice");
            assert_eq!(member.data.online_state, "online");
            assert_eq!(member.data.status.text, "Coding");
            assert_eq!(member.data.status.emoji.as_deref(), Some("💻"));
        } else {
            panic!("expected member_event for member-create");
        }
    }

    #[tokio::test]
    async fn member_create_is_relevant_for_all_subjects() {
        // member_event join is broadcast to everyone — not filtered to the subject only.
        let event = map_tap_event_with(
            &TapMessageRecord {
                live: true,
                did: String::from("did:plc:community"),
                rev: String::from("1"),
                collection: String::from("social.colibri.member"),
                rkey: String::from("member-1"),
                action: String::from("create"),
                record: Some(json!({
                    "$type": "social.colibri.member",
                    "subject": "did:plc:somebody-else",
                    "roles": [],
                    "joinedAt": "2026-01-01T00:00:00Z"
                })),
                cid: Some(String::from("cid")),
            },
            "did:plc:abc",
            mock_db(),
            &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("no profile")) }),
            &|did| Box::pin(async move { Ok(did) }),
            &|_, _| Box::pin(async { Ok(String::from("offline")) }),
        )
        .await
        .unwrap();

        assert!(event.is_relevant);
        assert_eq!(event.event_type, "member_event");
        if let Some(ColibriServerEventData::Member(data)) = event.data {
            assert_eq!(data.event, "join");
        } else {
            panic!("expected member event");
        }
    }

    #[tokio::test]
    async fn maps_member_update_to_roles_updated_event() {
        let event = map_tap_event_with(
            &TapMessageRecord {
                live: true,
                did: String::from("did:plc:community"),
                rev: String::from("2"),
                collection: String::from("social.colibri.member"),
                rkey: String::from("member-1"),
                action: String::from("update"),
                record: Some(json!({
                    "$type": "social.colibri.member",
                    "subject": "did:plc:alice",
                    "roles": ["mod-role", "vip-role"],
                    "joinedAt": "2026-01-01T00:00:00Z"
                })),
                cid: Some(String::from("cid2")),
            },
            "did:plc:abc",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(json!({
                            "displayName": "Alice",
                        })),
                        "social.colibri.actor.data" => Ok(json!({
                            "$type": "social.colibri.actor.data",
                            "status": "",
                            "communities": []
                        })),
                        _ => Err(serde_json::Error::custom("unexpected nsid")),
                    }
                })
            },
            &|_| Box::pin(async { Ok(String::from("alice.test")) }),
            &|_, _| Box::pin(async { Ok(String::from("online")) }),
        )
        .await
        .unwrap();

        assert!(event.is_relevant);
        if let Some(ColibriServerEventData::Member(data)) = event.data {
            assert_eq!(data.event, "roles_updated");
            assert_eq!(
                data.community,
                "at://did:plc:community/social.colibri.community/self"
            );
            assert!(data.membership.is_none());
            let member = data.member.expect("member should be present");
            assert_eq!(member.did, "did:plc:alice");
            assert_eq!(member.handle, "alice.test");
            assert_eq!(member.roles, vec!["mod-role", "vip-role"]);
            assert_eq!(member.data.online_state, "online");
        } else {
            panic!("expected member event for roles_updated");
        }
    }

    #[tokio::test]
    async fn maps_channel_delete_as_delete_event() {
        let event = map_tap_event_with(
            &record("social.colibri.channel", "delete", json!({})),
            "",
            mock_db(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Channel(data)) = event.data {
            assert_eq!(data.event, "delete");
            assert!(data.name.is_none());
        } else {
            panic!("expected channel event");
        }
    }

    #[tokio::test]
    async fn maps_message_delete_event() {
        let event = map_tap_event_with(
            &record(
                "social.colibri.message",
                "delete",
                json!({
                    "$type":"social.colibri.message",
                    "text":"ignored",
                    "createdAt":"2024-01-01T00:00:00Z",
                    "channel":"c1"
                }),
            ),
            "",
            mock_db(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Message(data)) = event.data {
            assert_eq!(data.event, "delete");
            assert_eq!(data.text, None);
            assert!(data.author.is_none());
        } else {
            panic!("expected message event data");
        }
    }

    #[tokio::test]
    async fn maps_message_upsert_event_with_author() {
        let event = map_tap_event_with(
            &record(
                "social.colibri.message",
                "create",
                json!({
                    "$type": "social.colibri.message",
                    "text": "hello",
                    "createdAt": "2026-06-04T00:00:00Z",
                    "channel": "chan-1"
                }),
            ),
            "",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(json!({
                            "displayName": "Alice",
                            "description": "Hi there",
                            "avatar": { "ref": "blob1" }
                        })),
                        "social.colibri.actor.data" => Ok(json!({
                            "$type": "social.colibri.actor.data",
                            "status": "Coding",
                            "emoji": "💻",
                            "communities": []
                        })),
                        _ => Err(serde_json::Error::custom("unexpected nsid")),
                    }
                })
            },
            &|_| Box::pin(async { Ok(String::from("alice.test")) }),
            &|_, _| Box::pin(async { Ok(String::from("online")) }),
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Message(data)) = event.data {
            assert_eq!(data.event, "upsert");
            assert_eq!(data.text.as_deref(), Some("hello"));
            assert_eq!(data.channel.as_deref(), Some("chan-1"));
            let author = data.author.expect("author should be present");
            assert_eq!(author.did, "did:plc:abc");
            assert_eq!(author.handle, "alice.test");
            assert_eq!(author.data.display_name, "Alice");
            assert_eq!(author.data.online_state, "online");
            assert_eq!(author.data.status.text, "Coding");
            assert_eq!(author.data.status.emoji.as_deref(), Some("💻"));
            assert!(author.data.avatar.is_some());
        } else {
            panic!("expected message event data");
        }
    }

    #[tokio::test]
    async fn maps_message_upsert_event_with_missing_profile() {
        // Author enrichment is best-effort: a missing profile/actor-data should
        // not drop the event — it falls back to empty defaults.
        let event = map_tap_event_with(
            &record(
                "social.colibri.message",
                "create",
                json!({
                    "$type": "social.colibri.message",
                    "text": "hi",
                    "createdAt": "2026-06-04T00:00:00Z",
                    "channel": "chan-1"
                }),
            ),
            "",
            mock_db(),
            &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("not found")) }),
            &|did| {
                Box::pin(
                    async move { Err(serde_json::Error::custom(format!("no handle for {did}"))) },
                )
            },
            &|_, _| Box::pin(async { Err(serde_json::Error::custom("no state")) }),
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Message(data)) = event.data {
            assert_eq!(data.event, "upsert");
            let author = data.author.expect("author should still be present");
            // DID is always known
            assert_eq!(author.did, "did:plc:abc");
            // Falls back to DID when handle resolution fails
            assert_eq!(author.handle, "did:plc:abc");
            // Falls back to empty/offline defaults
            assert_eq!(author.data.display_name, "");
            assert_eq!(author.data.online_state, "offline");
            assert_eq!(author.data.status.text, "");
            assert!(author.data.status.emoji.is_none());
        } else {
            panic!("expected message event data");
        }
    }

    #[tokio::test]
    async fn maps_actor_data_to_user_event() {
        let event = map_tap_event_with(
            &record(
                "social.colibri.actor.data",
                "update",
                json!({
                    "$type":"social.colibri.actor.data",
                    "status":"Working",
                    "emoji":"🦜",
                    "communities":[]
                }),
            ),
            "",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    if nsid == "app.bsky.actor.profile" {
                        Ok(json!({
                            "displayName":"Alice",
                            "description":"Hi",
                            "avatar":{"ref":"blob1"}
                        }))
                    } else {
                        Err(serde_json::Error::custom("unexpected nsid"))
                    }
                })
            },
            &|_| Box::pin(async { Ok(String::from("alice.test")) }),
            &|_, _| Box::pin(async { Ok(String::from("away")) }),
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::User(data)) = event.data {
            assert_eq!(data.profile.handle, "alice.test");
            assert_eq!(data.status.unwrap().state, "away");
        } else {
            panic!("expected user event data");
        }
    }

    #[tokio::test]
    async fn maps_bsky_profile_to_user_event() {
        let event = map_tap_event_with(
            &record(
                "app.bsky.actor.profile",
                "update",
                json!({
                    "displayName":"Alice",
                    "description":"Hi",
                    "avatar":{"ref":"blob1"}
                }),
            ),
            "",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    if nsid == "social.colibri.actor.data" {
                        Ok(json!({
                            "$type":"social.colibri.actor.data",
                            "status":"Busy",
                            "emoji":"🔥",
                            "communities":[]
                        }))
                    } else {
                        Err(serde_json::Error::custom("unexpected nsid"))
                    }
                })
            },
            &|_| Box::pin(async { Ok(String::from("alice.test")) }),
            &|_, _| Box::pin(async { Ok(String::from("online")) }),
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::User(data)) = event.data {
            assert_eq!(data.profile.handle, "alice.test");
            assert_eq!(data.status.unwrap().text, "Busy");
        } else {
            panic!("expected user event data");
        }
    }

    #[tokio::test]
    async fn returns_error_for_facet_collection() {
        let err = map_tap_event_with(
            &record("social.colibri.richtext.facet", "create", json!({})),
            "",
            mock_db(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "Facet");
    }

    #[tokio::test]
    async fn returns_error_for_unknown_collection() {
        let err = map_tap_event_with(
            &record("unknown.collection", "create", json!({})),
            "",
            mock_db(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "Unknown collection");
    }

    #[tokio::test]
    async fn reaction_removed_includes_emoji_and_target_from_cache() {
        // The cached record is still present when the delete event fires, so
        // the removed event should carry emoji + target.
        let event = map_tap_event_with(
            &record("social.colibri.reaction", "delete", json!({})),
            "",
            mock_db(),
            &|_, _, _, _| {
                Box::pin(async {
                    Ok(json!({
                        "$type": "social.colibri.reaction",
                        "emoji": "🦜",
                        "targetMessage": "msg-1"
                    }))
                })
            },
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Reaction(data)) = event.data {
            assert_eq!(data.event, "removed");
            assert_eq!(data.emoji.as_deref(), Some("🦜"));
            assert_eq!(data.target.as_deref(), Some("msg-1"));
        } else {
            panic!("expected reaction event");
        }
    }

    #[tokio::test]
    async fn reaction_removed_still_emits_when_cache_miss() {
        // If the cached record is gone (race or already evicted), the event
        // still fires — just without emoji/target, same as before the fix.
        let event = map_tap_event_with(
            &record("social.colibri.reaction", "delete", json!({})),
            "",
            mock_db(),
            &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("not found")) }),
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        if let Some(ColibriServerEventData::Reaction(data)) = event.data {
            assert_eq!(data.event, "removed");
            assert!(data.emoji.is_none());
            assert!(data.target.is_none());
        } else {
            panic!("expected reaction event");
        }
    }
}
