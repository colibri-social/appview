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
    CommunityEventData, MessageEventData, ReactionEventData, UserEventData, UserEventProfile,
    UserEventStatus,
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
            // `member` is the source of truth for community membership in the
            // Variant A model. Broadcasts only fire when the event subject is
            // the receiving client.
            //
            // Create / update → community upsert (auto-admit on open
            // community, or `approveMembership` on closed). The `did` of a
            // member record is the community DID; we synthesise the
            // community URI from it.
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriMember>(event_record)?;
                if record_data.subject != user_did {
                    return Ok(irrelevant_event());
                }
                let community_uri =
                    format!("at://{}/social.colibri.community/self", event_record.did);
                let community_record = fetch_record_fn(
                    event_record.did.clone(),
                    String::from("social.colibri.community"),
                    String::from("self"),
                    db.clone(),
                )
                .await?;
                let safe_community = serde_json::from_value::<ColibriCommunity>(community_record)?;
                return Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::Community(CommunityEventData {
                        event: String::from("upsert"),
                        uri: community_uri,
                        category_order: Some(safe_community.category_order),
                        description: Some(safe_community.description),
                        name: Some(safe_community.name),
                        picture: safe_community.picture,
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
                Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::Reaction(ReactionEventData {
                        event: String::from("removed"),
                        uri,
                        emoji: None,
                        target: None,
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
    async fn maps_member_create_to_community_upsert_for_subject() {
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
                    "roles": [],
                    "joinedAt": "2026-01-01T00:00:00Z"
                })),
                cid: Some(String::from("cid")),
            },
            "did:plc:abc",
            mock_db(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    if nsid == "social.colibri.community" {
                        Ok(json!({
                            "$type": "social.colibri.community",
                            "name": "General",
                            "description": "desc",
                            "categoryOrder": ["cat1"],
                            "requiresApprovalToJoin": false
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
            assert_eq!(data.event, "upsert");
            assert_eq!(
                data.uri,
                "at://did:plc:community/social.colibri.community/self"
            );
            assert_eq!(data.name.as_deref(), Some("General"));
        } else {
            panic!("expected community event for member-create");
        }
    }

    #[tokio::test]
    async fn member_create_is_irrelevant_for_other_subjects() {
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
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert!(!event.is_relevant);
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
}
