use futures::future::BoxFuture;
use sea_orm::DatabaseConnection;
use serde::de::{DeserializeOwned, Error};

use crate::lib::at_uri::AtUri;
use crate::lib::author_cache::{AuthorCache, AuthorEnrichment};
use crate::lib::bsky::ActorProfile;
use crate::lib::colibri::{
    ColibriActorData, ColibriActorProfile, ColibriCategory, ColibriChannel, ColibriCommunity,
    ColibriMember, ColibriMembership, ColibriMessage, ColibriModeration, ColibriReaction,
    ColibriRole, EffectiveProfile, resolve_effective_profile,
};
use crate::lib::event_scope::{CommunityResolver, EventScope};
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

/// One mapped event tagged with the audience that should receive it.
type ScopedServerEvent = (ColibriServerEvent, EventScope);

/// Convenience: a single mapped event with its scope.
fn scoped(event: ColibriServerEvent, scope: EventScope) -> Vec<ScopedServerEvent> {
    vec![(event, scope)]
}

/// Builds a globally-scoped `user_event` from an already-resolved effective
/// profile. Presence/profile updates are low-frequency and small, so they are
/// delivered globally rather than computing the union of the user's communities.
fn build_user_event_global(
    did: String,
    effective: EffectiveProfile,
    is_bot: bool,
    handle: String,
    status: UserEventStatus,
) -> Vec<ScopedServerEvent> {
    scoped(
        ColibriServerEvent {
            event_type: String::from("user_event"),
            data: Some(ColibriServerEventData::User(UserEventData {
                did,
                profile: UserEventProfile {
                    avatar: effective.avatar,
                    banner: effective.banner,
                    description: effective.description,
                    display_name: effective.display_name,
                    is_bot,
                    handle,
                    theme: effective.theme,
                },
                status: Some(status),
            })),
        },
        EventScope::Global,
    )
}

/// Builds a `user_event` for `did` whose **identity** (handle, display name,
/// avatar, banner, description, theme, `is_bot`) is re-derived from this
/// AppView's own view of the subject's repo — never from caller-supplied data —
/// while carrying the supplied ephemeral `status` (online-state + custom status).
///
/// This is the trust boundary for injecting a Hummed presence event: a peer
/// AppView authorized for a user's *presence* must not be able to redefine that
/// user's *identity* to a community. Identity comes from the local cache (with
/// the usual live-PDS fallback in `fetch_record_value`); only the ephemeral
/// state, which is genuinely off-protocol and thus the Hum's reason to exist, is
/// taken from the caller.
pub async fn build_presence_user_event(
    did: &str,
    status: UserEventStatus,
    db: &DatabaseConnection,
    author_cache: &AuthorCache,
) -> ColibriServerEvent {
    let AuthorEnrichment {
        profile,
        colibri_profile,
        ..
    } = cached_enrichment(
        did,
        db,
        &|d, n, r, db| Box::pin(fetch_record_value(d, n, r, db)),
        author_cache,
    )
    .await;

    let is_bot = profile.as_ref().is_some_and(ActorProfile::is_bot);
    let effective = resolve_effective_profile(colibri_profile.as_ref(), profile.as_ref());
    let handle = resolve_handle_for_did(did.to_string())
        .await
        .unwrap_or_else(|_| did.to_string());

    ColibriServerEvent {
        event_type: String::from("user_event"),
        data: Some(ColibriServerEventData::User(UserEventData {
            did: did.to_string(),
            profile: UserEventProfile {
                avatar: effective.avatar,
                banner: effective.banner,
                description: effective.description,
                display_name: effective.display_name,
                is_bot,
                handle,
                theme: effective.theme,
            },
            status: Some(status),
        })),
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

/// Returns the author's `app.bsky.actor.profile`, `social.colibri.actor.profile`,
/// and `social.colibri.actor.data` records, served from `author_cache` when warm
/// and otherwise fetched (DB, with a live PDS fallback inside `fetch_record_fn`)
/// and cached. The Bluesky and Colibri profiles are resolved into the served
/// profile by [`resolve_effective_profile`] at the call sites, so member/author
/// surfaces honour `syncBluesky` exactly like `getData`. The result — including
/// "looked up, not present" — is cached so a backfill's stream of same-author
/// messages performs these lookups once rather than per message.
async fn cached_enrichment(
    did: &str,
    db: &DatabaseConnection,
    fetch_record_fn: &FetchRecordFn,
    author_cache: &AuthorCache,
) -> AuthorEnrichment {
    if let Some(hit) = author_cache.get(did) {
        return hit;
    }

    let profile = fetch_record_fn(
        did.to_string(),
        String::from("app.bsky.actor.profile"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ActorProfile>(v).ok());

    let colibri_profile = fetch_record_fn(
        did.to_string(),
        String::from("social.colibri.actor.profile"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ColibriActorProfile>(v).ok());

    let actor_data = fetch_record_fn(
        did.to_string(),
        String::from("social.colibri.actor.data"),
        String::from("self"),
        db.clone(),
    )
    .await
    .ok()
    .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok());

    let enrichment = AuthorEnrichment {
        profile,
        colibri_profile,
        actor_data,
    };
    author_cache.put(did, enrichment.clone());
    enrichment
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
    author_cache: &AuthorCache,
) -> (String, MemberEventMemberData) {
    let handle = resolve_handle_fn(subject_did.to_string())
        .await
        .unwrap_or_else(|_| subject_did.to_string());

    let state = get_state_fn(subject_did.to_string(), db.clone())
        .await
        .unwrap_or_else(|_| String::from("offline"));

    let AuthorEnrichment {
        profile,
        colibri_profile,
        actor_data,
    } = cached_enrichment(subject_did, &db, fetch_record_fn, author_cache).await;

    // `is_bot` always comes from the Bluesky profile; the served display
    // name/avatar/banner/description/theme come from the effective profile so a
    // non-synced Colibri user shows their Colibri identity here, matching `getData`.
    let is_bot = profile.as_ref().is_some_and(ActorProfile::is_bot);
    let effective = resolve_effective_profile(colibri_profile.as_ref(), profile.as_ref());

    let data = MemberEventMemberData {
        display_name: effective.display_name.unwrap_or_default(),
        avatar: effective.avatar,
        banner: effective.banner,
        description: effective.description,
        is_bot,
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
#[allow(clippy::too_many_arguments)]
async fn build_member_event_member(
    subject_did: &str,
    member: &ColibriMember,
    community_authority: &str,
    db: DatabaseConnection,
    fetch_record_fn: &FetchRecordFn,
    resolve_handle_fn: &ResolveHandleFn,
    get_state_fn: &GetStateFn,
    author_cache: &AuthorCache,
) -> MemberEventMember {
    let (handle, data) = build_actor_handle_and_data(
        subject_did,
        db,
        fetch_record_fn,
        resolve_handle_fn,
        get_state_fn,
        author_cache,
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

#[allow(clippy::too_many_arguments)]
async fn map_tap_event_with(
    event_record: &TapMessageRecord,
    db: DatabaseConnection,
    resolver: &CommunityResolver,
    fetch_record_fn: &FetchRecordFn,
    resolve_handle_fn: &ResolveHandleFn,
    get_state_fn: &GetStateFn,
    author_cache: &AuthorCache,
) -> Result<Vec<ScopedServerEvent>, serde_json::Error> {
    let uri = make_event_uri(event_record);

    match event_record.collection.as_str() {
        "social.colibri.community" => {
            // Community records live on the community's repo, so `record.did`
            // is the community DID.
            let scope = EventScope::Community(event_record.did.clone());
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriCommunity>(event_record)?;
                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    scope,
                ))
            } else {
                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    scope,
                ))
            }
        }
        "social.colibri.membership" => {
            // Membership is intent recorded on the user's own repo, so
            // `record.did` is always the user the membership belongs to. For
            // open communities a create does NOT broadcast on its own: the
            // authoritative "you're now in" signal is the community-side
            // `social.colibri.member` record, handled below — auto-admit writes
            // that record almost immediately. For closed
            // (`requiresApprovalToJoin`) communities, though, a create here *is*
            // the moderator-facing signal: a new entry in the pending-applications
            // queue, broadcast as `application_event` to the community's members.
            //
            // A delete is the leaver's self-leave: we emit a community-delete
            // scoped to that user (`User(record.did)`) so they get instant UX
            // feedback without waiting for the AppView's downstream member-record
            // revoke to round-trip through the firehose. The eventual
            // member-delete event still arrives but maps to a no-op (idempotent).
            if event_record.action == "delete" {
                let Ok(old_record) = fetch_record_fn(
                    event_record.did.clone(),
                    event_record.collection.clone(),
                    event_record.rkey.clone(),
                    db.clone(),
                )
                .await
                else {
                    // Cached membership already gone — nothing to resolve the
                    // community URI from; downstream member-delete still covers it.
                    return Ok(vec![]);
                };
                let Ok(safe_record) = serde_json::from_value::<ColibriMembership>(old_record)
                else {
                    return Ok(vec![]);
                };

                return Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    EventScope::User(event_record.did.clone()),
                ));
            }

            // Create: only relevant if it's a *pending* application — i.e. a
            // closed community that hasn't already auto-admitted it.
            let record_data = parse_payload::<ColibriMembership>(event_record)?;
            let Some(community_at_uri) = AtUri::parse(&record_data.community) else {
                return Ok(vec![]);
            };
            // Legacy communities (rkey != "self") have no AppView-held
            // credentials and are never auto- or manually-admitted — see
            // `process_membership_create`.
            if community_at_uri.rkey != "self" {
                return Ok(vec![]);
            }

            let Ok(community_json) = fetch_record_fn(
                community_at_uri.authority.clone(),
                String::from("social.colibri.community"),
                String::from("self"),
                db.clone(),
            )
            .await
            else {
                return Ok(vec![]);
            };
            let Ok(community) = serde_json::from_value::<ColibriCommunity>(community_json) else {
                return Ok(vec![]);
            };

            if !community.requires_approval_to_join {
                // Open community — auto-admit fires a `member_event` separately.
                return Ok(vec![]);
            }

            let banned =
                moderation::is_user_banned(&db, &community_at_uri.authority, &event_record.did)
                    .await
                    .unwrap_or(false);
            if banned {
                // `process_membership_create` refuses to admit banned users and
                // writes a `blockedJoin` audit entry instead — surfacing this as
                // a pending application would let a moderator try to approve it.
                return Ok(vec![]);
            }

            let (handle, member_data) = build_actor_handle_and_data(
                &event_record.did,
                db.clone(),
                fetch_record_fn,
                resolve_handle_fn,
                get_state_fn,
                author_cache,
            )
            .await;

            Ok(scoped(
                ColibriServerEvent {
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
                },
                EventScope::Community(community_at_uri.authority),
            ))
        }
        "social.colibri.member" => {
            // `member` is the source of truth for community membership and
            // lives on the community's repo, so `record.did` is the community
            // DID. Events scope to that community's members — except that the
            // affected subject must also be reached directly (via `User`) on
            // join (they aren't in the community's member set yet) and on
            // delete (so they clear the community even after losing membership).
            let community_did = event_record.did.clone();
            let community_uri = format!("at://{community_did}/social.colibri.community/self");

            if event_record.action == "create" {
                let record_data = parse_payload::<ColibriMember>(event_record)?;
                let subject = record_data.subject.clone();
                let member = build_member_event_member(
                    &subject,
                    &record_data,
                    &event_record.did,
                    db.clone(),
                    fetch_record_fn,
                    resolve_handle_fn,
                    get_state_fn,
                    author_cache,
                )
                .await;
                let join_event = ColibriServerEvent {
                    event_type: String::from("member_event"),
                    data: Some(ColibriServerEventData::Member(MemberEventData {
                        event: String::from("join"),
                        community: community_uri,
                        membership: record_data.from_membership,
                        member: Some(member),
                        member_did: None,
                    })),
                };
                // Existing members learn of the new member; the joiner gets a
                // direct copy (they aren't yet in their own community set, and
                // receiving it triggers their connection to refresh that set).
                return Ok(vec![
                    (join_event.clone(), EventScope::Community(community_did)),
                    (join_event, EventScope::User(subject)),
                ]);
            }

            if event_record.action == "update" {
                let record_data = parse_payload::<ColibriMember>(event_record)?;
                let member = build_member_event_member(
                    &record_data.subject.clone(),
                    &record_data,
                    &event_record.did,
                    db.clone(),
                    fetch_record_fn,
                    resolve_handle_fn,
                    get_state_fn,
                    author_cache,
                )
                .await;
                return Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("member_event"),
                        data: Some(ColibriServerEventData::Member(MemberEventData {
                            event: String::from("roles_updated"),
                            community: community_uri,
                            membership: None,
                            member: Some(member),
                            member_did: None,
                        })),
                    },
                    EventScope::Community(community_did),
                ));
            }

            // Delete → the removed user gets a `community_event { delete }`
            // scoped to them so they immediately clear the community from their
            // view, while the rest of the community gets a `member_event { leave }`
            // so they can drop the entry from their member list without a reload.
            let Ok(old_record) = fetch_record_fn(
                event_record.did.clone(),
                event_record.collection.clone(),
                event_record.rkey.clone(),
                db.clone(),
            )
            .await
            else {
                return Ok(vec![]);
            };
            let Ok(safe_record) = serde_json::from_value::<ColibriMember>(old_record) else {
                return Ok(vec![]);
            };
            let community_delete = ColibriServerEvent {
                event_type: String::from("community_event"),
                data: Some(ColibriServerEventData::Community(CommunityEventData {
                    event: String::from("delete"),
                    uri: community_uri.clone(),
                    category_order: None,
                    description: None,
                    name: None,
                    picture: None,
                    requires_approval_to_join: None,
                })),
            };
            let leave = ColibriServerEvent {
                event_type: String::from("member_event"),
                data: Some(ColibriServerEventData::Member(MemberEventData {
                    event: String::from("leave"),
                    community: community_uri,
                    membership: None,
                    member: None,
                    member_did: Some(safe_record.subject.clone()),
                })),
            };
            Ok(vec![
                (community_delete, EventScope::User(safe_record.subject)),
                (leave, EventScope::Community(community_did)),
            ])
        }
        "social.colibri.category" => {
            // Category records live on the community's repo → `record.did` is
            // the community DID.
            let scope = EventScope::Community(event_record.did.clone());
            // The record stores `community` as a bare record-key ("self"), but
            // the client compares against the full community AT-URI, so emit
            // that (matching `member_event`/`role_event`) — otherwise the
            // client's community-scope guard drops the event and nothing
            // updates live.
            let community_uri = format!("at://{}/social.colibri.community/self", event_record.did);
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriCategory>(event_record)?;
                Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("category_event"),
                        data: Some(ColibriServerEventData::Category(CategoryEventData {
                            event: String::from("upsert"),
                            uri,
                            channel_order: Some(record_data.channel_order),
                            community: Some(community_uri),
                            name: Some(record_data.name),
                        })),
                    },
                    scope,
                ))
            } else {
                Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("category_event"),
                        data: Some(ColibriServerEventData::Category(CategoryEventData {
                            event: String::from("delete"),
                            uri,
                            channel_order: None,
                            community: None,
                            name: None,
                        })),
                    },
                    scope,
                ))
            }
        }
        "social.colibri.channel" => {
            // Channel records live on the community's repo → `record.did` is
            // the community DID.
            let scope = EventScope::Community(event_record.did.clone());
            // The record stores `community` as a bare record-key ("self"), but
            // the client compares against the full community AT-URI, so emit
            // that (matching `member_event`/`role_event`) — otherwise the
            // client's community-scope guard drops the event and nothing
            // updates live.
            let community_uri = format!("at://{}/social.colibri.community/self", event_record.did);
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriChannel>(event_record)?;
                Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("channel_event"),
                        data: Some(ColibriServerEventData::Channel(ChannelEventData {
                            event: String::from("upsert"),
                            uri,
                            channel_type: Some(record_data.channel_type),
                            community: Some(community_uri),
                            category: Some(record_data.category),
                            description: record_data.description,
                            name: Some(record_data.name),
                            owner_only: record_data.owner_only,
                            allowed_roles: Some(record_data.allowed_roles),
                            allowed_members: Some(record_data.allowed_members),
                        })),
                    },
                    scope,
                ))
            } else {
                Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("channel_event"),
                        data: Some(ColibriServerEventData::Channel(ChannelEventData {
                            event: String::from("delete"),
                            uri,
                            channel_type: None,
                            community: None,
                            category: None,
                            description: None,
                            name: None,
                            owner_only: None,
                            allowed_roles: None,
                            allowed_members: None,
                        })),
                    },
                    scope,
                ))
            }
        }
        "social.colibri.message" => {
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriMessage>(event_record)?;

                // Messages live on the AUTHOR's repo and carry only a bare
                // channel rkey, so the owning community is resolved via the
                // channel record. The message row isn't indexed yet at
                // create-time (ack runs after mapping), so resolve from the
                // payload's channel rkey, not the message rkey.
                let scope = resolver
                    .community_for_channel(&db, &record_data.channel)
                    .await
                    .map(EventScope::Community)
                    .unwrap_or(EventScope::Global);

                // Best-effort author enrichment: individual fetch failures fall
                // back to safe defaults so a missing profile never drops the event.
                let handle = resolve_handle_fn(event_record.did.clone())
                    .await
                    .unwrap_or_else(|_| event_record.did.clone());

                let state = get_state_fn(event_record.did.clone(), db.clone())
                    .await
                    .unwrap_or_else(|_| String::from("offline"));

                let AuthorEnrichment {
                    profile,
                    colibri_profile,
                    actor_data,
                } = cached_enrichment(&event_record.did, &db, fetch_record_fn, author_cache).await;

                // `is_bot` always comes from Bluesky; the served mirrored fields
                // come from the effective profile so a non-synced Colibri author
                // shows their Colibri identity here, matching `getData`.
                let is_bot = profile.as_ref().is_some_and(ActorProfile::is_bot);
                let effective =
                    resolve_effective_profile(colibri_profile.as_ref(), profile.as_ref());

                let author = MessageEventAuthor {
                    did: event_record.did.clone(),
                    handle,
                    data: MessageEventAuthorData {
                        display_name: effective.display_name.unwrap_or_default(),
                        avatar: effective.avatar,
                        banner: effective.banner,
                        description: effective.description,
                        is_bot,
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

                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    scope,
                ))
            } else {
                // The message row is still indexed at delete-time (ack runs
                // after mapping), so resolve the community by message rkey.
                let scope = resolver
                    .community_for_message(&db, &event_record.rkey)
                    .await
                    .map(EventScope::Community)
                    .unwrap_or(EventScope::Global);
                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    scope,
                ))
            }
        }
        "social.colibri.reaction" => {
            // Reactions live on the REACTOR's repo and reference a bare target
            // message rkey, so the owning community is resolved message -> channel
            // -> community. `Global` fallback on a miss keeps the (small,
            // idempotent) event from being silently dropped.
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriReaction>(event_record)?;
                let scope = resolver
                    .community_for_message(&db, &record_data.parent)
                    .await
                    .map(EventScope::Community)
                    .unwrap_or(EventScope::Global);
                Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("reaction_event"),
                        data: Some(ColibriServerEventData::Reaction(ReactionEventData {
                            event: String::from("added"),
                            uri,
                            emoji: Some(record_data.emoji),
                            target: Some(record_data.parent),
                        })),
                    },
                    scope,
                ))
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

                let scope = match cached.as_ref() {
                    Some(reaction) => resolver
                        .community_for_message(&db, &reaction.parent)
                        .await
                        .map(EventScope::Community)
                        .unwrap_or(EventScope::Global),
                    None => EventScope::Global,
                };

                Ok(scoped(
                    ColibriServerEvent {
                        event_type: String::from("reaction_event"),
                        data: Some(ColibriServerEventData::Reaction(ReactionEventData {
                            event: String::from("removed"),
                            uri,
                            emoji: cached.as_ref().map(|r| r.emoji.clone()),
                            target: cached.map(|r| r.parent),
                        })),
                    },
                    scope,
                ))
            }
        }
        "social.colibri.actor.data" => {
            // A status/communities change. The profile fields in the broadcast
            // still come from the effective profile (Colibri profile, or Bluesky
            // when synced / un-onboarded).
            let colibri_profile = fetch_record_fn(
                event_record.did.clone(),
                String::from("social.colibri.actor.profile"),
                String::from("self"),
                db.clone(),
            )
            .await
            .ok()
            .and_then(|v| serde_json::from_value::<ColibriActorProfile>(v).ok());
            let bsky_profile = fetch_record_fn(
                event_record.did.clone(),
                String::from("app.bsky.actor.profile"),
                String::from("self"),
                db.clone(),
            )
            .await
            .ok()
            .and_then(|v| serde_json::from_value::<ActorProfile>(v).ok());

            let effective =
                resolve_effective_profile(colibri_profile.as_ref(), bsky_profile.as_ref());
            let is_bot = bsky_profile.as_ref().is_some_and(ActorProfile::is_bot);

            let safe_actor_data = parse_payload::<ColibriActorData>(event_record)?;
            let handle = resolve_handle_fn(event_record.did.clone()).await?;
            let state = get_state_fn(event_record.did.clone(), db.clone()).await?;

            Ok(build_user_event_global(
                event_record.did.clone(),
                effective,
                is_bot,
                handle,
                UserEventStatus {
                    emoji: safe_actor_data.emoji,
                    state,
                    text: safe_actor_data.status.unwrap_or_default(),
                },
            ))
        }
        "social.colibri.actor.profile" => {
            // On create/update the payload is the new Colibri profile; on delete
            // it is absent, so we fall back to `None` — which reverts the served
            // profile to Bluesky.
            let colibri_profile = parse_payload::<ColibriActorProfile>(event_record).ok();
            let bsky_profile = fetch_record_fn(
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
            .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok())
            .unwrap_or_default();

            let effective =
                resolve_effective_profile(colibri_profile.as_ref(), bsky_profile.as_ref());
            let is_bot = bsky_profile.as_ref().is_some_and(ActorProfile::is_bot);
            let handle = resolve_handle_fn(event_record.did.clone()).await?;
            let state = get_state_fn(event_record.did.clone(), db.clone()).await?;

            Ok(build_user_event_global(
                event_record.did.clone(),
                effective,
                is_bot,
                handle,
                UserEventStatus {
                    emoji: actor_data.emoji,
                    state,
                    text: actor_data.status.unwrap_or_default(),
                },
            ))
        }
        "app.bsky.actor.profile" => {
            let colibri_data = fetch_record_fn(
                event_record.did.clone(),
                String::from("social.colibri.actor.data"),
                String::from("self"),
                db.clone(),
            )
            .await
            .ok()
            .and_then(|v| serde_json::from_value::<ColibriActorData>(v).ok());

            // User has no Colibri actor data — not a Colibri user, skip.
            let safe_colibri_data = match colibri_data {
                Some(d) => d,
                None => return Ok(vec![]),
            };

            let colibri_profile = fetch_record_fn(
                event_record.did.clone(),
                String::from("social.colibri.actor.profile"),
                String::from("self"),
                db.clone(),
            )
            .await
            .ok()
            .and_then(|v| serde_json::from_value::<ColibriActorProfile>(v).ok());

            // A Bluesky edit only affects the Colibri-served profile when the
            // user has no Colibri profile (Bluesky is the fallback source) or has
            // sync enabled. A non-synced Colibri profile owns its fields.
            if matches!(&colibri_profile, Some(p) if !p.sync_bluesky) {
                return Ok(vec![]);
            }

            // Resolve against the freshly-updated payload so synced users see the
            // new Bluesky values immediately.
            let safe_profile = parse_payload::<ActorProfile>(event_record)?;
            let is_bot = safe_profile.is_bot();
            let effective =
                resolve_effective_profile(colibri_profile.as_ref(), Some(&safe_profile));

            let handle = resolve_handle_fn(event_record.did.clone()).await?;
            let state = get_state_fn(event_record.did.clone(), db.clone()).await?;

            Ok(build_user_event_global(
                event_record.did.clone(),
                effective,
                is_bot,
                handle,
                UserEventStatus {
                    emoji: safe_colibri_data.emoji,
                    state,
                    text: safe_colibri_data.status.unwrap_or_default(),
                },
            ))
        }
        "social.colibri.role" => {
            // Role records live on the community's repo → `record.did` is the
            // community DID.
            let scope = EventScope::Community(event_record.did.clone());
            let community_uri = format!("at://{}/social.colibri.community/self", event_record.did);
            if event_record.action != "delete" {
                let record_data = parse_payload::<ColibriRole>(event_record)?;
                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    scope,
                ))
            } else {
                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    scope,
                ))
            }
        }
        "social.colibri.moderation" => {
            // Only `hideMessage` actions need a client-side event: the message
            // record itself is never deleted, so TAP won't fire a message-delete
            // event. Emit one here so connected clients remove the hidden message.
            // All other moderation actions (ban, unban, kick) manifest as member
            // record changes which are already handled above. Moderation records
            // live on the community's repo → `record.did` is the community DID.
            let record_data = parse_payload::<ColibriModeration>(event_record)?;
            if record_data.action == "hideMessage" {
                let message_uri = record_data
                    .subject
                    .uri
                    .ok_or_else(|| serde_json::Error::custom("hideMessage missing subject.uri"))?;
                Ok(scoped(
                    ColibriServerEvent {
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
                    },
                    EventScope::Community(event_record.did.clone()),
                ))
            } else {
                Ok(vec![])
            }
        }
        "social.colibri.approval" => Ok(vec![]),
        // Read cursors are handled directly in `tap.rs`, which emits the
        // cross-device read-state sync event. No server event maps here.
        "social.colibri.channel.read" => Ok(vec![]),
        // GIF favorites are a purely client-owned PDS record
        "social.colibri.actor.gifFavorites" => Ok(vec![]),
        "social.colibri.richtext.facet" => Err(serde_json::Error::custom("Facet")),
        _ => Err(serde_json::Error::custom("Unknown collection")),
    }
}

/// Maps a single tap record into zero or more server events, each tagged with
/// the audience that should receive it. Enrichment (handle/profile/state
/// lookups) and community resolution happen here exactly once per record — the
/// tap loop is the sole caller, so this work is no longer repeated per client.
pub async fn map_tap_event(
    event_record: &TapMessageRecord,
    db: &DatabaseConnection,
    resolver: &CommunityResolver,
    author_cache: &AuthorCache,
) -> Result<Vec<ScopedServerEvent>, serde_json::Error> {
    map_tap_event_with(
        event_record,
        db.clone(),
        resolver,
        &|did, nsid, rkey, db| Box::pin(fetch_record_value(did, nsid, rkey, db)),
        &|did| Box::pin(resolve_handle_for_did(did)),
        &|did, db| Box::pin(get_user_state(did, db)),
        author_cache,
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

    /// Fresh, empty community resolver. Community-repo events derive their scope
    /// from `record.did` and never consult it; message/reaction tests seed it.
    fn resolver() -> CommunityResolver {
        CommunityResolver::new()
    }

    /// Test shim: forwards to the real `map_tap_event_with` with a fresh
    /// (empty) author cache, so the cache is exercised but each test stays
    /// isolated. Shadows the glob-imported `super::map_tap_event_with`, letting
    /// the existing six-argument test call sites stay unchanged.
    async fn map_tap_event_with(
        event_record: &TapMessageRecord,
        db: DatabaseConnection,
        resolver: &CommunityResolver,
        fetch_record_fn: &FetchRecordFn,
        resolve_handle_fn: &ResolveHandleFn,
        get_state_fn: &GetStateFn,
    ) -> Result<Vec<ScopedServerEvent>, serde_json::Error> {
        super::map_tap_event_with(
            event_record,
            db,
            resolver,
            fetch_record_fn,
            resolve_handle_fn,
            get_state_fn,
            &AuthorCache::new(),
        )
        .await
    }

    /// Asserts exactly one scoped event was produced and returns it.
    fn only(events: Vec<ScopedServerEvent>) -> ScopedServerEvent {
        assert_eq!(events.len(), 1, "expected exactly one scoped event");
        events.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn maps_community_upsert_event() {
        let (event, scope) = only(
            map_tap_event_with(
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
                mock_db(),
                &resolver(),
                &no_fetch,
                &no_resolve,
                &no_state,
            )
            .await
            .unwrap(),
        );

        assert_eq!(event.event_type, "community_event");
        // Community records live on the community repo, so scope is that DID.
        assert_eq!(scope, EventScope::Community(String::from("did:plc:abc")));

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
    async fn maps_membership_delete_to_self_scoped_community_delete() {
        // A membership lives on the user's own repo, so a delete is always the
        // leaver's self-leave: emit a community-delete scoped to that user only.
        let (event, scope) = only(
            map_tap_event_with(
                &record("social.colibri.membership", "delete", json!({})),
                mock_db(),
                &resolver(),
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
            .unwrap(),
        );

        assert_eq!(scope, EventScope::User(String::from("did:plc:abc")));
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
        let events = map_tap_event_with(
            &membership_record(),
            mock_db(),
            &resolver(),
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

        assert!(events.is_empty());
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

        let events = map_tap_event_with(
            &membership_record(),
            db,
            &resolver(),
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

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn membership_create_for_closed_community_emits_application_event() {
        use crate::models::record_data;

        let db = sea_orm::MockDatabase::new(sea_orm::DatabaseBackend::Postgres)
            .append_query_results([Vec::<record_data::Model>::new()])
            .into_connection();

        let (event, scope) = only(
            map_tap_event_with(
                &membership_record(),
                db,
                &resolver(),
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
            .unwrap(),
        );

        assert_eq!(event.event_type, "application_event");
        // The pending application is scoped to the community's members (moderators).
        assert_eq!(scope, EventScope::Community(String::from("did:plc:owner")));
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
        let events = map_tap_event_with(
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
            mock_db(),
            &resolver(),
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

        // A join fans out twice: to the community's members, and directly to
        // the joining subject (who isn't in their own community set yet).
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].1,
            EventScope::Community(String::from("did:plc:community"))
        );
        assert_eq!(events[1].1, EventScope::User(String::from("did:plc:abc")));

        let event = &events[0].0;
        if let Some(ColibriServerEventData::Member(data)) = &event.data {
            assert_eq!(data.event, "join");
            assert_eq!(
                data.community,
                "at://did:plc:community/social.colibri.community/self"
            );
            assert_eq!(
                data.membership.as_deref(),
                Some("at://did:plc:abc/social.colibri.membership/m1")
            );
            let member = data.member.as_ref().expect("member should be present");
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
    async fn member_create_fans_out_to_community_and_subject() {
        // A join scopes to the whole community (so existing members see the new
        // member) plus the subject directly (so they learn they're admitted).
        let events = map_tap_event_with(
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
            mock_db(),
            &resolver(),
            &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("no profile")) }),
            &|did| Box::pin(async move { Ok(did) }),
            &|_, _| Box::pin(async { Ok(String::from("offline")) }),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].1,
            EventScope::Community(String::from("did:plc:community"))
        );
        assert_eq!(
            events[1].1,
            EventScope::User(String::from("did:plc:somebody-else"))
        );
        assert_eq!(events[0].0.event_type, "member_event");
        if let Some(ColibriServerEventData::Member(data)) = &events[0].0.data {
            assert_eq!(data.event, "join");
        } else {
            panic!("expected member event");
        }
    }

    #[tokio::test]
    async fn member_delete_fans_out_self_delete_and_community_leave() {
        // A member delete tells the removed user (scoped to them) to drop the
        // community, and the rest of the community to drop the member entry.
        let events = map_tap_event_with(
            &TapMessageRecord {
                live: true,
                did: String::from("did:plc:community"),
                rev: String::from("3"),
                collection: String::from("social.colibri.member"),
                rkey: String::from("member-1"),
                action: String::from("delete"),
                record: None,
                cid: Some(String::from("cid3")),
            },
            mock_db(),
            &resolver(),
            &|_, _, _, _| {
                Box::pin(async {
                    Ok(json!({
                        "$type": "social.colibri.member",
                        "subject": "did:plc:gone",
                        "roles": [],
                        "joinedAt": "2026-01-01T00:00:00Z"
                    }))
                })
            },
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].1, EventScope::User(String::from("did:plc:gone")));
        assert_eq!(events[0].0.event_type, "community_event");
        assert_eq!(
            events[1].1,
            EventScope::Community(String::from("did:plc:community"))
        );
        if let Some(ColibriServerEventData::Member(data)) = &events[1].0.data {
            assert_eq!(data.event, "leave");
            assert_eq!(data.member_did.as_deref(), Some("did:plc:gone"));
        } else {
            panic!("expected member leave event");
        }
    }

    #[tokio::test]
    async fn maps_member_update_to_roles_updated_event() {
        let (event, scope) = only(
            map_tap_event_with(
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
                mock_db(),
                &resolver(),
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
            .unwrap(),
        );

        assert_eq!(
            scope,
            EventScope::Community(String::from("did:plc:community"))
        );
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
        let (event, scope) = only(
            map_tap_event_with(
                &record("social.colibri.channel", "delete", json!({})),
                mock_db(),
                &resolver(),
                &no_fetch,
                &no_resolve,
                &no_state,
            )
            .await
            .unwrap(),
        );

        assert_eq!(scope, EventScope::Community(String::from("did:plc:abc")));
        if let Some(ColibriServerEventData::Channel(data)) = event.data {
            assert_eq!(data.event, "delete");
            assert!(data.name.is_none());
        } else {
            panic!("expected channel event");
        }
    }

    #[tokio::test]
    async fn maps_channel_upsert_emits_full_community_uri() {
        // The record stores `community` as a bare record-key ("self"), but the
        // event must carry the full community AT-URI — the client filters
        // incoming events by comparing `data.community` against the community
        // URI it holds, so a bare key would be dropped and never live-update.
        let (event, scope) = only(
            map_tap_event_with(
                &record(
                    "social.colibri.channel",
                    "create",
                    json!({
                        "$type": "social.colibri.channel",
                        "name": "General",
                        "description": "Teste",
                        "type": "social.colibri.channel.text",
                        "category": "cat-rkey",
                        "community": "self"
                    }),
                ),
                mock_db(),
                &resolver(),
                &no_fetch,
                &no_resolve,
                &no_state,
            )
            .await
            .unwrap(),
        );

        assert_eq!(scope, EventScope::Community(String::from("did:plc:abc")));
        if let Some(ColibriServerEventData::Channel(data)) = event.data {
            assert_eq!(data.event, "upsert");
            assert_eq!(data.name.as_deref(), Some("General"));
            assert_eq!(data.description.as_deref(), Some("Teste"));
            assert_eq!(
                data.community.as_deref(),
                Some("at://did:plc:abc/social.colibri.community/self")
            );
        } else {
            panic!("expected channel event");
        }
    }

    #[tokio::test]
    async fn maps_message_delete_event() {
        // The message row is still indexed at delete-time, so the community is
        // resolved from the message rkey ("r1" from the `record` helper).
        let resolver = resolver();
        resolver.seed_message("r1", "did:plc:community");
        let (event, scope) = only(
            map_tap_event_with(
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
                mock_db(),
                &resolver,
                &no_fetch,
                &no_resolve,
                &no_state,
            )
            .await
            .unwrap(),
        );

        assert_eq!(
            scope,
            EventScope::Community(String::from("did:plc:community"))
        );
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
        // Upserts resolve the community from the payload's channel rkey.
        let resolver = resolver();
        resolver.seed_channel("chan-1", "did:plc:community");
        let (event, scope) = only(
            map_tap_event_with(
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
                mock_db(),
                &resolver,
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
            .unwrap(),
        );

        assert_eq!(
            scope,
            EventScope::Community(String::from("did:plc:community"))
        );
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
        let resolver = resolver();
        resolver.seed_channel("chan-1", "did:plc:community");
        let (event, _scope) = only(
            map_tap_event_with(
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
                mock_db(),
                &resolver,
                &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("not found")) }),
                &|did| {
                    Box::pin(async move {
                        Err(serde_json::Error::custom(format!("no handle for {did}")))
                    })
                },
                &|_, _| Box::pin(async { Err(serde_json::Error::custom("no state")) }),
            )
            .await
            .unwrap(),
        );

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
        let (event, scope) = only(
            map_tap_event_with(
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
                mock_db(),
                &resolver(),
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
            .unwrap(),
        );

        // Presence/profile updates are delivered globally.
        assert_eq!(scope, EventScope::Global);
        if let Some(ColibriServerEventData::User(data)) = event.data {
            assert_eq!(data.profile.handle, "alice.test");
            assert_eq!(data.status.unwrap().state, String::from("away"));
        } else {
            panic!("expected user event data");
        }
    }

    #[tokio::test]
    async fn maps_bsky_profile_to_user_event() {
        let (event, scope) = only(
            map_tap_event_with(
                &record(
                    "app.bsky.actor.profile",
                    "update",
                    json!({
                        "displayName":"Alice",
                        "description":"Hi",
                        "avatar":{"ref":"blob1"}
                    }),
                ),
                mock_db(),
                &resolver(),
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
            .unwrap(),
        );

        assert_eq!(scope, EventScope::Global);
        if let Some(ColibriServerEventData::User(data)) = event.data {
            assert_eq!(data.profile.handle, "alice.test");
            assert_eq!(data.status.unwrap().text, "Busy");
        } else {
            panic!("expected user event data");
        }
    }

    #[tokio::test]
    async fn bsky_profile_without_colibri_actor_data_is_irrelevant() {
        let events = map_tap_event_with(
            &record(
                "app.bsky.actor.profile",
                "update",
                json!({
                    "displayName": "Marius",
                    "description": "lazy by nature"
                }),
            ),
            mock_db(),
            &resolver(),
            &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("not found")) }),
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn maps_actor_profile_to_user_event_with_theme() {
        let (event, scope) = only(
            map_tap_event_with(
                &record(
                    "social.colibri.actor.profile",
                    "update",
                    json!({
                        "$type": "social.colibri.actor.profile",
                        "displayName": "Colibri Alice",
                        "syncBluesky": false,
                        "theme": { "accentColor": "#ff0000" }
                    }),
                ),
                mock_db(),
                &resolver(),
                &|_, nsid, _, _| {
                    Box::pin(async move {
                        match nsid.as_str() {
                            "social.colibri.actor.data" => Ok(json!({
                                "$type": "social.colibri.actor.data",
                                "status": "Busy",
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
            .unwrap(),
        );

        assert_eq!(scope, EventScope::Global);
        if let Some(ColibriServerEventData::User(data)) = event.data {
            assert_eq!(data.profile.display_name.as_deref(), Some("Colibri Alice"));
            assert_eq!(
                data.profile.theme.unwrap().accent_color.as_deref(),
                Some("#ff0000")
            );
            assert_eq!(data.status.unwrap().text, "Busy");
        } else {
            panic!("expected user event data");
        }
    }

    #[tokio::test]
    async fn bsky_edit_skipped_for_unsynced_colibri_profile() {
        // A non-synced Colibri profile owns its fields, so a Bluesky edit must
        // not change the Colibri-served profile.
        let events = map_tap_event_with(
            &record(
                "app.bsky.actor.profile",
                "update",
                json!({ "displayName": "New Bsky Name" }),
            ),
            mock_db(),
            &resolver(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "social.colibri.actor.data" => Ok(json!({
                            "$type": "social.colibri.actor.data",
                            "status": "Busy",
                            "communities": []
                        })),
                        "social.colibri.actor.profile" => Ok(json!({
                            "$type": "social.colibri.actor.profile",
                            "displayName": "Colibri Name",
                            "syncBluesky": false
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

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn bsky_edit_propagates_for_synced_colibri_profile() {
        let (event, _scope) = only(
            map_tap_event_with(
                &record(
                    "app.bsky.actor.profile",
                    "update",
                    json!({ "displayName": "New Bsky Name" }),
                ),
                mock_db(),
                &resolver(),
                &|_, nsid, _, _| {
                    Box::pin(async move {
                        match nsid.as_str() {
                            "social.colibri.actor.data" => Ok(json!({
                                "$type": "social.colibri.actor.data",
                                "status": "Busy",
                                "communities": []
                            })),
                            "social.colibri.actor.profile" => Ok(json!({
                                "$type": "social.colibri.actor.profile",
                                "syncBluesky": true,
                                "theme": { "accentColor": "#abcdef" }
                            })),
                            _ => Err(serde_json::Error::custom("unexpected nsid")),
                        }
                    })
                },
                &|_| Box::pin(async { Ok(String::from("alice.test")) }),
                &|_, _| Box::pin(async { Ok(String::from("online")) }),
            )
            .await
            .unwrap(),
        );

        if let Some(ColibriServerEventData::User(data)) = event.data {
            // Synced: the new Bluesky name flows through, Colibri theme is kept.
            assert_eq!(data.profile.display_name.as_deref(), Some("New Bsky Name"));
            assert_eq!(
                data.profile.theme.unwrap().accent_color.as_deref(),
                Some("#abcdef")
            );
        } else {
            panic!("expected user event data");
        }
    }

    #[tokio::test]
    async fn approval_collection_is_irrelevant() {
        let events = map_tap_event_with(
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
            mock_db(),
            &resolver(),
            &no_fetch,
            &no_resolve,
            &no_state,
        )
        .await
        .unwrap();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn reaction_create_with_parent_field_is_accepted() {
        let resolver = resolver();
        resolver.seed_message("3mhydckzgys2d", "did:plc:community");
        let (event, scope) = only(
            map_tap_event_with(
                &record(
                    "social.colibri.reaction",
                    "create",
                    json!({
                        "$type": "social.colibri.reaction",
                        "emoji": "🫡",
                        "parent": "3mhydckzgys2d"
                    }),
                ),
                mock_db(),
                &resolver,
                &no_fetch,
                &no_resolve,
                &no_state,
            )
            .await
            .unwrap(),
        );

        assert_eq!(
            scope,
            EventScope::Community(String::from("did:plc:community"))
        );
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
            mock_db(),
            &resolver(),
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
            mock_db(),
            &resolver(),
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
        let resolver = resolver();
        resolver.seed_message("msg-1", "did:plc:community");
        let (event, scope) = only(
            map_tap_event_with(
                &record("social.colibri.reaction", "delete", json!({})),
                mock_db(),
                &resolver,
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
            .unwrap(),
        );

        assert_eq!(
            scope,
            EventScope::Community(String::from("did:plc:community"))
        );
        if let Some(ColibriServerEventData::Reaction(data)) = event.data {
            assert_eq!(data.event, "removed");
            assert_eq!(data.emoji.as_deref(), Some("🦜"));
            assert_eq!(data.target.as_deref(), Some("msg-1"));
        } else {
            panic!("expected reaction event");
        }
    }

    // A non-synced Colibri profile must win over the Bluesky profile in member
    // and message-author enrichment, exactly like `getData`. Mirrors the
    // `get_data_handler` "unsynced" case for the firehose enrichment paths.
    #[tokio::test]
    async fn member_create_enrichment_uses_non_synced_colibri_profile() {
        let events = map_tap_event_with(
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
            mock_db(),
            &resolver(),
            &|_, nsid, _, _| {
                Box::pin(async move {
                    match nsid.as_str() {
                        "app.bsky.actor.profile" => Ok(json!({ "displayName": "Bsky Alice" })),
                        "social.colibri.actor.profile" => Ok(json!({
                            "displayName": "Colibri Alice",
                            "syncBluesky": false
                        })),
                        "social.colibri.actor.data" => Ok(json!({
                            "$type": "social.colibri.actor.data",
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

        let event = &events[0].0;
        if let Some(ColibriServerEventData::Member(data)) = &event.data {
            let member = data.member.as_ref().expect("member should be present");
            assert_eq!(member.data.display_name, "Colibri Alice");
        } else {
            panic!("expected member_event");
        }
    }

    #[tokio::test]
    async fn message_author_enrichment_uses_non_synced_colibri_profile() {
        let (event, _scope) = only(
            map_tap_event_with(
                &record(
                    "social.colibri.message",
                    "create",
                    json!({
                        "$type": "social.colibri.message",
                        "text": "hi",
                        "channel": "chan-a",
                        "createdAt": "2026-01-01T00:00:00Z"
                    }),
                ),
                mock_db(),
                &resolver(),
                &|_, nsid, _, _| {
                    Box::pin(async move {
                        match nsid.as_str() {
                            "app.bsky.actor.profile" => Ok(json!({ "displayName": "Bsky Bob" })),
                            "social.colibri.actor.profile" => Ok(json!({
                                "displayName": "Colibri Bob",
                                "syncBluesky": false
                            })),
                            "social.colibri.actor.data" => Ok(json!({
                                "$type": "social.colibri.actor.data",
                                "communities": []
                            })),
                            _ => Err(serde_json::Error::custom("unexpected nsid")),
                        }
                    })
                },
                &|_| Box::pin(async { Ok(String::from("bob.test")) }),
                &|_, _| Box::pin(async { Ok(String::from("online")) }),
            )
            .await
            .unwrap(),
        );

        if let Some(ColibriServerEventData::Message(data)) = event.data {
            let author = data.author.expect("author should be present");
            assert_eq!(author.data.display_name, "Colibri Bob");
        } else {
            panic!("expected message_event");
        }
    }

    #[tokio::test]
    async fn reaction_removed_still_emits_when_cache_miss() {
        // If the cached record is gone (race or already evicted), the event
        // still fires — just without emoji/target, scoped Global as a fallback.
        let (event, scope) = only(
            map_tap_event_with(
                &record("social.colibri.reaction", "delete", json!({})),
                mock_db(),
                &resolver(),
                &|_, _, _, _| Box::pin(async { Err(serde_json::Error::custom("not found")) }),
                &no_resolve,
                &no_state,
            )
            .await
            .unwrap(),
        );

        assert_eq!(scope, EventScope::Global);
        if let Some(ColibriServerEventData::Reaction(data)) = event.data {
            assert_eq!(data.event, "removed");
            assert!(data.emoji.is_none());
            assert!(data.target.is_none());
        } else {
            panic!("expected reaction event");
        }
    }
}
