use futures::future::BoxFuture;
use sea_orm::DatabaseConnection;
use serde::de::{DeserializeOwned, Error};

use crate::lib::at_uri::AtUri;
use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{
    ColibriActorData, ColibriCategory, ColibriChannel, ColibriCommunity, ColibriMember,
    ColibriMembership, ColibriMessage, ColibriModeration, ColibriReaction, ColibriRole,
};
use crate::lib::events::{
    ApplicationEventData, CategoryEventData, ChannelEventData, ColibriServerEvent,
    ColibriServerEventData, CommunityEventData, MemberEventData, MemberEventMember,
    MemberEventMemberData, MemberEventMemberStatus, MessageEventAuthor, MessageEventAuthorData,
    MessageEventAuthorStatus, MessageEventData, ReactionEventData, RoleEventData, UserEventData,
    UserEventProfile, UserEventStatus,
};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::get_state::get_state;
use crate::lib::moderation;
use crate::lib::pds_client;
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
    match get_atproto_record::<serde_json::Value>(did.clone(), nsid.clone(), rkey.clone(), &db)
        .await
    {
        Ok(v) => return Ok(v),
        Err(sea_orm::DbErr::RecordNotFound(_)) => {} // fall through to live fetch
        Err(e) => return Err(serde_json::Error::custom(e.to_string())),
    }

    // Record missing from local index — fetch live from the user's PDS.
    let doc = resolve_did(&did)
        .await
        .map_err(|e| serde_json::Error::custom(e.body.into_inner().message))?
        .0;

    let pds_url = doc
        .service
        .iter()
        .find(|s| s.service_type == "AtprotoPersonalDataServer")
        .map(|s| s.service_endpoint.clone())
        .ok_or_else(|| serde_json::Error::custom("No PDS endpoint in DID document"))?;

    pds_client::get_record(&pds_url, &did, &nsid, &rkey)
        .await
        .map_err(|e| serde_json::Error::custom(e.to_string()))
        .and_then(|opt| opt.ok_or_else(|| serde_json::Error::custom("Record not found on PDS")))
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

/// Resolves the handle and builds the enriched [`MemberEventMemberData`] for
/// the given subject, using best-effort profile/state lookups. Individual
/// fetch failures fall back to safe defaults so a missing profile never drops
/// the event. Shared by `member_event` (an admitted member) and
/// `application_event` (a pending applicant) construction below.
async fn build_actor_handle_and_data(
    subject_did: &str,
    db: DatabaseConnection,
    fetch_record_fn: &FetchRecordFn,
    resolve_handle_fn: &ResolveHandleFn,
    get_state_fn: &GetStateFn,
) -> (String, MemberEventMemberData) {
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

    let data = MemberEventMemberData {
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
                .map(|d| d.status.clone().unwrap_or(String::from("")))
                .unwrap_or_default(),
            emoji: actor_data.as_ref().and_then(|d| d.emoji.clone()),
        },
    };

    (handle, data)
}

/// Builds a fully-enriched [`MemberEventMember`] for the given subject, using
/// best-effort profile/state lookups. Individual fetch failures fall back to
/// safe defaults so a missing profile never drops the event.
async fn build_member_event_member(
    subject_did: &str,
    member: &ColibriMember,
    community_authority: &str,
    db: DatabaseConnection,
    fetch_record_fn: &FetchRecordFn,
    resolve_handle_fn: &ResolveHandleFn,
    get_state_fn: &GetStateFn,
) -> MemberEventMember {
    let (handle, data) = build_actor_handle_and_data(
        subject_did,
        db,
        fetch_record_fn,
        resolve_handle_fn,
        get_state_fn,
    )
    .await;

    // `member.roles` holds bare rkeys (the on-record storage format); clients
    // compare roles by full AT-URI (see `list_members_handler::build_member`,
    // which expands the same way), so this event must match.
    let roles = member
        .roles
        .iter()
        .map(|rkey| format!("at://{community_authority}/social.colibri.role/{rkey}"))
        .collect();

    MemberEventMember {
        did: subject_did.to_string(),
        handle,
        roles,
        joined_at: member.joined_at.clone(),
        nickname: member.nickname.clone(),
        data,
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
                        requires_approval_to_join: Some(record_data.requires_approval_to_join),
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
                        requires_approval_to_join: None,
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.membership" => {
            // Membership is intent recorded on the user's own repo. For open
            // communities it does NOT broadcast on its own: the authoritative
            // "you're now in" signal is the community-side
            // `social.colibri.member` record, handled below — auto-admit
            // writes that record almost immediately. For closed
            // (`requiresApprovalToJoin`) communities, though, a create here
            // *is* the moderator-facing signal: it's a new entry in the
            // pending-applications queue, broadcast as `application_event`.
            //
            // The other exception is membership-delete for the caller — we
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
                        requires_approval_to_join: None,
                    })),
                    is_relevant: true,
                });
            }

            if event_record.action == "delete" {
                // A non-self membership delete (e.g. an applicant withdrew,
                // or the row was pruned). Nothing pending to resolve client-side
                // beyond what `member_event`/`community_event` already cover.
                return Ok(irrelevant_event());
            }

            // Create: only relevant if it's a *pending* application — i.e. a
            // closed community that hasn't already auto-admitted it.
            let record_data = parse_payload::<ColibriMembership>(event_record)?;
            let Some(community_at_uri) = AtUri::parse(&record_data.community) else {
                return Ok(irrelevant_event());
            };
            // Legacy communities (rkey != "self") have no AppView-held
            // credentials and are never auto- or manually-admitted — see
            // `process_membership_create`.
            if community_at_uri.rkey != "self" {
                return Ok(irrelevant_event());
            }

            let Ok(community_json) = fetch_record_fn(
                community_at_uri.authority.clone(),
                String::from("social.colibri.community"),
                String::from("self"),
                db.clone(),
            )
            .await
            else {
                return Ok(irrelevant_event());
            };
            let Ok(community) = serde_json::from_value::<ColibriCommunity>(community_json) else {
                return Ok(irrelevant_event());
            };

            if !community.requires_approval_to_join {
                // Open community — auto-admit fires a `member_event` separately.
                return Ok(irrelevant_event());
            }

            let banned =
                moderation::is_user_banned(&db, &community_at_uri.authority, &event_record.did)
                    .await
                    .unwrap_or(false);
            if banned {
                // `process_membership_create` refuses to admit banned users and
                // writes a `blockedJoin` audit entry instead — surfacing this as
                // a pending application would let a moderator try to approve it.
                return Ok(irrelevant_event());
            }

            let (handle, member_data) = build_actor_handle_and_data(
                &event_record.did,
                db.clone(),
                fetch_record_fn,
                resolve_handle_fn,
                get_state_fn,
            )
            .await;

            Ok(ColibriServerEvent {
                event_type: String::from("application_event"),
                data: Some(ColibriServerEventData::Application(ApplicationEventData {
                    event: String::from("create"),
                    community: record_data.community.clone(),
                    did: Some(event_record.did.clone()),
                    handle: Some(handle),
                    membership: uri,
                    created_at: Some(record_data.created_at.clone()),
                    data: Some(member_data),
                })),
                is_relevant: true,
            })
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
                    &event_record.did,
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
                        member_did: None,
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
                    &event_record.did,
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
                        member_did: None,
                    })),
                    is_relevant: true,
                });
            }

            // Delete → the removed user gets a `community_event { delete }` so
            // they immediately clear the community from their view. All other
            // connected clients get a `member_event { leave }` so they can
            // remove the entry from their member list without a full reload.
            let old_record = fetch_record_fn(
                event_record.did.clone(),
                event_record.collection.clone(),
                event_record.rkey.clone(),
                db.clone(),
            )
            .await?;
            let safe_record = serde_json::from_value::<ColibriMember>(old_record)?;
            let community_uri = format!("at://{}/social.colibri.community/self", event_record.did);
            if safe_record.subject == user_did {
                return Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::Community(CommunityEventData {
                        event: String::from("delete"),
                        uri: community_uri,
                        category_order: None,
                        description: None,
                        name: None,
                        picture: None,
                        requires_approval_to_join: None,
                    })),
                    is_relevant: true,
                });
            }
            Ok(ColibriServerEvent {
                event_type: String::from("member_event"),
                data: Some(ColibriServerEventData::Member(MemberEventData {
                    event: String::from("leave"),
                    community: community_uri,
                    membership: None,
                    member: None,
                    member_did: Some(safe_record.subject),
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
                                .map(|d| d.status.clone().unwrap_or(String::from("")))
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
                        text: safe_actor_data.status.unwrap_or(String::from("")),
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
            .await
            .ok()
            .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok());

            // User has no Colibri actor data — not a Colibri user, skip.
            let safe_colibri_data = match colibri_data {
                Some(d) => d,
                None => return Ok(irrelevant_event()),
            };

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
                        text: safe_colibri_data.status.unwrap_or(String::from("")),
                    }),
                })),
                is_relevant: true,
            })
        }
        "social.colibri.role" => {
            let community_uri = format!("at://{}/social.colibri.community/self", event_record.did);
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriRole>(event_record)?;
                Ok(ColibriServerEvent {
                    event_type: String::from("role_event"),
                    data: Some(ColibriServerEventData::Role(RoleEventData {
                        event: String::from("upsert"),
                        uri,
                        community: Some(community_uri),
                        name: Some(record_data.name),
                        color: record_data.color,
                        permissions: Some(record_data.permissions),
                        position: Some(record_data.position),
                        hoisted: record_data.hoisted,
                        mentionable: record_data.mentionable,
                    })),
                    is_relevant: true,
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("role_event"),
                    data: Some(ColibriServerEventData::Role(RoleEventData {
                        event: String::from("delete"),
                        uri,
                        community: None,
                        name: None,
                        color: None,
                        permissions: None,
                        position: None,
                        hoisted: None,
                        mentionable: None,
                    })),
                    is_relevant: true,
                })
            }
        }
        "social.colibri.moderation" => {
            // Only `hideMessage` actions need a client-side event: the message
            // record itself is never deleted, so TAP won't fire a message-delete
            // event. Emit one here so connected clients remove the hidden message.
            // All other moderation actions (ban, unban, kick) manifest as member
            // record changes which are already handled above.
            let record_data = parse_payload::<ColibriModeration>(event_record)?;
            if record_data.action == "hideMessage" {
                let message_uri = record_data
                    .subject
                    .uri
                    .ok_or_else(|| serde_json::Error::custom("hideMessage missing subject.uri"))?;
                Ok(ColibriServerEvent {
                    event_type: String::from("message_event"),
                    data: Some(ColibriServerEventData::Message(MessageEventData {
                        event: String::from("delete"),
                        uri: message_uri,
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
            } else {
                Ok(irrelevant_event())
            }
        }
        "social.colibri.approval" => Ok(irrelevant_event()),
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

        // The upsert event must forward `requiresApprovalToJoin` so clients can
        // react to the setting changing without a full refetch.
        if let Some(ColibriServerEventData::Community(data)) = event.data {
            assert_eq!(data.event, "upsert");
            assert_eq!(data.requires_approval_to_join, Some(false));
        } else {
            panic!("expected a community event payload");
        }
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

    fn closed_community_json() -> serde_json::Value {
        json!({
            "$type": "social.colibri.community",
            "name": "Closed Club",
            "description": "desc",
            "categoryOrder": [],
            "requiresApprovalToJoin": true
        })
    }

    fn open_community_json() -> serde_json::Value {
        json!({
            "$type": "social.colibri.community",
            "name": "Open Club",
            "description": "desc",
            "categoryOrder": [],
            "requiresApprovalToJoin": false
        })
    }

    fn membership_record() -> TapMessageRecord {
        record(
            "social.colibri.membership",
            "create",
            json!({
                "$type":"social.colibri.membership",
                "community":"at://did:plc:owner/social.colibri.community/self",
                "createdAt":"2026-01-01T00:00:00Z"
            }),
        )
    }

    #[tokio::test]
    async fn membership_create_for_open_community_is_irrelevant() {
        // Open communities auto-admit — the broadcast-worthy signal is the
        // resulting `social.colibri.member` create, handled separately.
        let event = map_tap_event_with(
            &membership_record(),
            "did:plc:abc",
            mock_db(),
            &|_, nsid, _, _| {
                let nsid = nsid.clone();
                Box::pin(async move {
                    if nsid == "social.colibri.community" {
                        Ok(open_community_json())
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

        assert_eq!(event.event_type, "empty");
        assert!(!event.is_relevant);
    }

    #[tokio::test]
    async fn membership_create_for_banned_user_is_irrelevant() {
        use crate::models::record_data;

        let banned_record = record_data::Model {
            id: 1,
            did: String::from("did:plc:owner"),
            nsid: String::from("social.colibri.moderation"),
            rkey: String::from("mod-1"),
            data: json!({
                "$type": "social.colibri.moderation",
                "action": "ban",
                "subject": { "did": "did:plc:abc" },
                "createdBy": "did:plc:owner",
                "createdAt": "2026-01-01T00:00:00Z"
            }),
            indexed_at: String::from("2026-01-01T00:00:00Z"),
        };
        let db = sea_orm::MockDatabase::new(sea_orm::DatabaseBackend::Postgres)
            .append_query_results([vec![banned_record]])
            .into_connection();

        let event = map_tap_event_with(
            &membership_record(),
            "did:plc:abc",
            db,
            &|_, nsid, _, _| {
                let nsid = nsid.clone();
                Box::pin(async move {
                    if nsid == "social.colibri.community" {
                        Ok(closed_community_json())
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

        assert_eq!(event.event_type, "empty");
        assert!(!event.is_relevant);
    }

    #[tokio::test]
    async fn membership_create_for_closed_community_emits_application_event() {
        use crate::models::record_data;

        let db = sea_orm::MockDatabase::new(sea_orm::DatabaseBackend::Postgres)
            .append_query_results([Vec::<record_data::Model>::new()])
            .into_connection();

        let event = map_tap_event_with(
            &membership_record(),
            "did:plc:abc",
            db,
            &|_, nsid, _, _| {
                let nsid = nsid.clone();
                Box::pin(async move {
                    match nsid.as_str() {
                        "social.colibri.community" => Ok(closed_community_json()),
                        "app.bsky.actor.profile" => Ok(json!({
                            "displayName": "Alice",
                            "avatar": { "ref": "blob1" }
                        })),
                        "social.colibri.actor.data" => Ok(json!({
                            "$type": "social.colibri.actor.data",
                            "status": "Excited",
                            "emoji": "🎉",
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

        assert_eq!(event.event_type, "application_event");
        assert!(event.is_relevant);
        if let Some(ColibriServerEventData::Application(data)) = event.data {
            assert_eq!(data.event, "create");
            assert_eq!(
                data.community,
                "at://did:plc:owner/social.colibri.community/self"
            );
            assert_eq!(data.did.as_deref(), Some("did:plc:abc"));
            assert_eq!(data.handle.as_deref(), Some("alice.test"));
            assert_eq!(
                data.membership,
                "at://did:plc:abc/social.colibri.membership/r1"
            );
            assert_eq!(data.created_at.as_deref(), Some("2026-01-01T00:00:00Z"));
            let member_data = data.data.expect("data should be present on create");
            assert_eq!(member_data.display_name, "Alice");
            assert_eq!(member_data.online_state, "online");
            assert_eq!(member_data.status.text, "Excited");
            assert_eq!(member_data.status.emoji.as_deref(), Some("🎉"));
        } else {
            panic!("expected application_event payload");
        }
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
            assert_eq!(
                member.roles,
                vec!["at://did:plc:community/social.colibri.role/owner-role"]
            );
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
            assert_eq!(
                member.roles,
                vec![
                    "at://did:plc:community/social.colibri.role/mod-role",
                    "at://did:plc:community/social.colibri.role/vip-role"
                ]
            );
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
            assert_eq!(data.status.unwrap().state, String::from("away"));
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
    async fn bsky_profile_without_colibri_actor_data_is_irrelevant() {
        let event = map_tap_event_with(
            &record(
                "app.bsky.actor.profile",
                "update",
                json!({
                    "displayName": "Marius",
                    "description": "lazy by nature"
                }),
            ),
            "",
            mock_db(),
            &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("not found")) }),
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert_eq!(event.event_type, "empty");
        assert!(!event.is_relevant);
    }

    #[tokio::test]
    async fn approval_collection_is_irrelevant() {
        let event = map_tap_event_with(
            &record(
                "social.colibri.approval",
                "create",
                json!({
                    "$type": "social.colibri.approval",
                    "community": "at://did:plc:abc/social.colibri.community/self",
                    "membership": "at://did:plc:xyz/social.colibri.membership/r1",
                    "createdAt": "2026-03-26T19:42:54.652Z"
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

        assert_eq!(event.event_type, "empty");
        assert!(!event.is_relevant);
    }

    #[tokio::test]
    async fn reaction_create_with_parent_field_is_accepted() {
        let event = map_tap_event_with(
            &record(
                "social.colibri.reaction",
                "create",
                json!({
                    "$type": "social.colibri.reaction",
                    "emoji": "🫡",
                    "parent": "3mhydckzgys2d"
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

        if let Some(ColibriServerEventData::Reaction(data)) = event.data {
            assert_eq!(data.event, "added");
            assert_eq!(data.emoji.as_deref(), Some("🫡"));
            assert_eq!(data.target.as_deref(), Some("3mhydckzgys2d"));
        } else {
            panic!("expected reaction event");
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
