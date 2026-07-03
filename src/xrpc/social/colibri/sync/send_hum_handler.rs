//! `social.colibri.sync.sendHum` — inbound off-protocol event relay (Humming).
//!
//! A peer AppView POSTs a [`HumEnvelope`] describing an off-protocol event
//! (status, typing, voice presence) about one of its users. This AppView, acting
//! as the community's hub, authenticates the peer, authorizes the claim against
//! the subject's own repo, injects the event to local subscribers, and — while
//! the envelope's `ttl` permits — republishes it to its own `subscribeHums`
//! peers.
//!
//! ## Trust checks (all must pass)
//!
//! 1. **Peer authentication.** The inter-service-auth JWT (`aud` = this AppView,
//!    `lxm` = `social.colibri.sync.sendHum`) verifies against the issuer's
//!    published key; the issuer is the `peer_did`.
//! 2. **Origin binding.** `peer_did` must equal the envelope `origin` — a peer
//!    may only relay Hums it is itself the origin of.
//! 3. **Presence authorization.** `origin` must equal the subject's declared
//!    `presenceService` (`social.colibri.actor.profile`). A missing key means the
//!    user has not opted into cross-instance presence, so the Hum is dropped.
//!
//! On-protocol forgery is impossible by construction: [`HumEvent`] only
//! deserializes the off-protocol event subset, so a message/member/role payload
//! fails to parse into the envelope and Rocket rejects the request body before
//! this handler runs.

use std::sync::Arc;

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::tokio::sync::broadcast::{Receiver, Sender};
use rocket::{State, post};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};

use crate::EventNotification;
use crate::lib::at_uri::AtUri;
use crate::lib::author_cache::AuthorCache;
use crate::lib::colibri::ColibriActorProfile;
use crate::lib::event_scope::{EventScope, ScopedEvent, SharedScopedEvent};
use crate::lib::events::{
    ColibriServerEvent, ColibriServerEventData, HumEnvelope, HumEvent, UserEventStatus,
    VoicePresenceEventData,
};
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::map_tap_event::build_presence_user_event;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::lib::tap::CommsBridge;
use crate::models::record_data;

const PROFILE_NSID: &str = "social.colibri.actor.profile";
const MEMBER_NSID: &str = "social.colibri.member";

type VerifyAuthFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> + Send + Sync;
type LoadPresenceFn =
    dyn Fn(String) -> BoxFuture<'static, Result<Option<String>, DbErr>> + Send + Sync;
type LoadMembershipFn =
    dyn Fn(String, String) -> BoxFuture<'static, Result<bool, DbErr>> + Send + Sync;
type BuildUserEventFn =
    dyn Fn(String, UserEventStatus) -> BoxFuture<'static, ColibriServerEvent> + Send + Sync;
/// Records a Hum id and reports whether it is new (true) or a duplicate to drop
/// (false). Shared by both ingest paths so a Hum arriving over more than one
/// relay is injected once.
type DedupFn = dyn Fn(String) -> bool + Send + Sync;
/// Reports whether an inbound `sendHum` from `peer_did` is within its rate
/// budget (true) or should be dropped (false).
type RateOkFn = dyn Fn(String) -> bool + Send + Sync;

#[allow(clippy::too_many_arguments)]
async fn receive_hum_with(
    auth: String,
    envelope: HumEnvelope,
    verify_auth_fn: &VerifyAuthFn,
    rate_ok_fn: &RateOkFn,
    load_presence_service_fn: &LoadPresenceFn,
    load_membership_fn: &LoadMembershipFn,
    build_user_event_fn: &BuildUserEventFn,
    dedup_fn: &DedupFn,
    broadcast_tx: &Sender<SharedScopedEvent>,
    c2c_tx: &Sender<EventNotification>,
    hums_tx: &Sender<HumEnvelope>,
) -> Result<(), ErrorResponse> {
    let peer_did = verify_auth_fn(auth, String::from("social.colibri.sync.sendHum"))
        .await
        .map_err(auth_error)?;

    // Per-peer rate limit (presence/typing are low-rate). Checked on the
    // authenticated peer so a flood from one AppView can't amplify past its
    // budget; the trusted egress-in path is a single stream and isn't limited.
    if !rate_ok_fn(peer_did.clone()) {
        return Err(rate_limited());
    }

    // A peer may only relay Hums it is itself the origin of. The rest of the
    // trust checks (origin must be an AppView, presenceService, membership, …)
    // are shared with the trusted egress-in path and live in `ingest_hum_with`.
    if peer_did != envelope.origin {
        return Err(forbidden(
            "envelope origin does not match the authenticated peer",
        ));
    }

    // Received via `sendHum` in our hub role → relay onward to `subscribeHums`
    // peers (subject to ttl).
    ingest_hum_with(
        envelope,
        load_presence_service_fn,
        load_membership_fn,
        build_user_event_fn,
        dedup_fn,
        broadcast_tx,
        c2c_tx,
        Some(hums_tx),
    )
    .await
}

/// Authorizes an off-protocol Hum against the subject's own repo and injects it
/// into the local realtime fan-out, optionally relaying it onward. Shared by the
/// authenticated `sendHum` handler (hub role: `relay_to = Some`) and the trusted
/// `subscribeHums` egress-in path (leaf role: `relay_to = None` — leaves never
/// re-forward). The caller is responsible for authenticating *who sent* the Hum;
/// this function authorizes *what it may claim*.
#[allow(clippy::too_many_arguments)]
async fn ingest_hum_with(
    envelope: HumEnvelope,
    load_presence_service_fn: &LoadPresenceFn,
    load_membership_fn: &LoadMembershipFn,
    build_user_event_fn: &BuildUserEventFn,
    dedup_fn: &DedupFn,
    broadcast_tx: &Sender<SharedScopedEvent>,
    c2c_tx: &Sender<EventNotification>,
    relay_to: Option<&Sender<HumEnvelope>>,
) -> Result<(), ErrorResponse> {
    // A Hum originates from an AppView, identified by `did:web`. Rejecting other
    // DID methods stops a user from minting a service-auth token (`iss` = their
    // own `did:plc`) and becoming their own presence origin, which — absent the
    // membership gate below — would let them inject spoofed presence anywhere.
    if !envelope.origin.starts_with("did:web:") {
        return Err(forbidden("hum origin must be an AppView (did:web)"));
    }

    // Loop guard: a Hum we originated must never be re-ingested. A peer can't
    // forge our DID with a valid signature, and the hub relays our own Hums back
    // to us over egress-in, so this fires on that echo.
    if envelope.origin == service_auth::appview_did() {
        return Ok(());
    }

    // Dedup on the envelope id: a Hum can reach us over more than one relay
    // path (e.g. two hub connections for overlapping communities). Injecting it
    // once keeps a duplicate from double-counting presence. Runs before the DB
    // authorization work so repeats are cheap to discard.
    if !dedup_fn(envelope.id.clone()) {
        return Ok(());
    }

    // The origin may only speak for a subject that named it as their presence
    // origin. This is the sole authority a valid signature grants — it says who
    // sent the Hum, not who they may represent.
    match load_presence_service_fn(envelope.subject.clone())
        .await
        .map_err(|e| internal_error(format!("presence lookup failed: {e}")))?
    {
        Some(ps) if ps == envelope.origin => {}
        _ => {
            return Err(forbidden(
                "origin is not the subject's declared presenceService",
            ));
        }
    }

    let community_did = AtUri::parse(&envelope.community)
        .map(|uri| uri.authority)
        .ok_or_else(|| invalid_request("malformed community URI"))?;

    // The subject must actually belong to the target community. Without this a
    // rogue (or self-appointed) presence origin could inject its user into any
    // community's realtime feed — appearing to type / be in voice / be present
    // somewhere they never joined.
    if !load_membership_fn(envelope.subject.clone(), community_did.clone())
        .await
        .map_err(|e| internal_error(format!("membership lookup failed: {e}")))?
    {
        return Err(forbidden("subject is not a member of the community"));
    }

    // Any channel the event references must belong to the target community, so a
    // Hum can't place the subject in a channel of an unrelated community.
    if let Some(channel) = event_channel(&envelope.event) {
        match AtUri::parse(channel) {
            Some(uri) if uri.authority == community_did => {}
            _ => return Err(forbidden("event channel does not belong to the community")),
        }
    }

    inject_local(
        &envelope.event,
        &envelope.subject,
        &community_did,
        build_user_event_fn,
        broadcast_tx,
        c2c_tx,
    )
    .await;

    // Hub role only: forward to peer `subscribeHums` streams, decrementing the
    // hop budget. A Hum at ttl 0 is delivered locally (above) but never relayed;
    // a leaf (egress-in) passes `None` and never forwards.
    if let Some(hums_tx) = relay_to
        && envelope.ttl > 0
    {
        let mut relayed = envelope;
        relayed.ttl -= 1;
        let _ = hums_tx.send(relayed);
    }

    Ok(())
}

/// The channel AT-URI an event is scoped to, if any. `user_event` carries no
/// channel; typing and voice presence do.
fn event_channel(event: &HumEvent) -> Option<&str> {
    match event {
        HumEvent::Typing(t) => Some(&t.channel),
        HumEvent::VoicePresence(v) => Some(&v.channel),
        HumEvent::User(_) => None,
    }
}

/// Injects a Hum's event into the local realtime fan-out, mirroring how locally
/// originated presence/typing is routed in `subscribe_events_handler`:
///
/// - `user_event` / `voice_presence_event` → a `Community`-scoped [`ScopedEvent`]
///   on the tap broadcast, so only local clients who share the community see it
///   (never `Global` — that would leak a remote user's presence to unrelated
///   clients).
/// - `typing_event` → the client-to-client typing channel, where each subscriber
///   further filters by the channel it is currently viewing.
///
/// Every event is attributed to `subject` (the authorized subject), never to the
/// `did` the peer embedded in the payload — a peer authorized for one user must
/// not be able to emit an event attributed to another. For `user_event` the
/// identity is rebuilt locally via `build_user_event_fn`, so only the ephemeral
/// status the peer sent survives.
async fn inject_local(
    event: &HumEvent,
    subject: &str,
    community_did: &str,
    build_user_event_fn: &BuildUserEventFn,
    broadcast_tx: &Sender<SharedScopedEvent>,
    c2c_tx: &Sender<EventNotification>,
) {
    match event {
        HumEvent::Typing(typing) => {
            let _ = c2c_tx.send(EventNotification {
                event_type: String::from("typing"),
                data: vec![subject.to_string(), typing.channel.clone()],
            });
        }
        HumEvent::User(user) => {
            let status = user.status.clone().unwrap_or_else(|| UserEventStatus {
                emoji: None,
                text: String::new(),
                state: String::from("online"),
            });
            let event = build_user_event_fn(subject.to_string(), status).await;
            broadcast_community(broadcast_tx, community_did, event);
        }
        HumEvent::VoicePresence(voice) => {
            broadcast_community(
                broadcast_tx,
                community_did,
                ColibriServerEvent {
                    event_type: String::from("voice_presence_event"),
                    data: Some(ColibriServerEventData::VoicePresence(
                        VoicePresenceEventData {
                            event: voice.event.clone(),
                            channel: voice.channel.clone(),
                            did: subject.to_string(),
                        },
                    )),
                },
            );
        }
    }
}

fn broadcast_community(
    broadcast_tx: &Sender<SharedScopedEvent>,
    community_did: &str,
    event: ColibriServerEvent,
) {
    let scoped = Arc::new(ScopedEvent {
        scope: EventScope::Community(community_did.to_string()),
        payload: event.serialize(),
    });
    let _ = broadcast_tx.send(scoped);
}

/// Reads the subject's declared `presenceService` from their cached Colibri
/// profile. `Ok(None)` when the profile is absent or the user hasn't opted in;
/// either way the caller drops the Hum.
async fn presence_service_for(
    db: &DatabaseConnection,
    subject: &str,
) -> Result<Option<String>, DbErr> {
    match get_atproto_record::<ColibriActorProfile>(
        subject.to_string(),
        PROFILE_NSID.to_string(),
        String::from("self"),
        db,
    )
    .await
    {
        Ok(profile) => Ok(profile.presence_service),
        Err(DbErr::RecordNotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Whether `subject` holds a `social.colibri.member` record on the community's
/// repo — the source of truth for native (Variant A) community membership.
async fn is_community_member(
    db: &DatabaseConnection,
    community_did: &str,
    subject: &str,
) -> Result<bool, DbErr> {
    let row = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(record_data::Column::Did.eq(community_did))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(subject)],
        ))
        .one(db)
        .await?;
    Ok(row.is_some())
}

/// Builds the production authorization/injection closures over a database
/// handle, shared by the `sendHum` handler and the trusted egress-in path so the
/// two agree on how presence, membership, and identity are resolved.
fn prod_deps(
    db: DatabaseConnection,
) -> (
    Box<LoadPresenceFn>,
    Box<LoadMembershipFn>,
    Box<BuildUserEventFn>,
    Box<DedupFn>,
) {
    let db_presence = db.clone();
    let load_presence: Box<LoadPresenceFn> = Box::new(
        move |subject: String| -> BoxFuture<'static, Result<Option<String>, DbErr>> {
            let db = db_presence.clone();
            Box::pin(async move { presence_service_for(&db, &subject).await })
        },
    );

    let db_member = db.clone();
    let load_membership: Box<LoadMembershipFn> = Box::new(
        move |subject: String, community: String| -> BoxFuture<'static, Result<bool, DbErr>> {
            let db = db_member.clone();
            Box::pin(async move { is_community_member(&db, &community, &subject).await })
        },
    );

    let build_user_event: Box<BuildUserEventFn> = Box::new(
        move |subject: String, status: UserEventStatus| -> BoxFuture<'static, ColibriServerEvent> {
            let db = db.clone();
            Box::pin(async move {
                // A throwaway per-Hum author cache is fine: presence is
                // low-frequency, so re-deriving identity on each event is cheap
                // relative to a status change, and it never serves stale identity.
                let cache = AuthorCache::new();
                build_presence_user_event(&subject, status, &db, &cache).await
            })
        },
    );

    let dedup: Box<DedupFn> = Box::new(move |id: String| crate::lib::hum_guard::seen_or_new(&id));

    (load_presence, load_membership, build_user_event, dedup)
}

/// Authorizes and injects a Hum received over an already-authenticated
/// `subscribeHums` stream (leaf/egress-in role). The peer hub authenticated the
/// transport; this still re-authorizes the claim against the subject's own repo
/// and never relays onward.
pub(crate) async fn ingest_trusted_hum(
    envelope: HumEnvelope,
    db: DatabaseConnection,
    broadcast_tx: &Sender<SharedScopedEvent>,
    c2c_tx: &Sender<EventNotification>,
) -> Result<(), ErrorResponse> {
    let (load_presence, load_membership, build_user_event, dedup) = prod_deps(db);
    ingest_hum_with(
        envelope,
        &*load_presence,
        &*load_membership,
        &*build_user_event,
        &*dedup,
        broadcast_tx,
        c2c_tx,
        None,
    )
    .await
}

fn auth_error(err: ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthRequired"),
            message: err.to_string(),
        }),
    }
}

fn not_enabled() -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("NotEnabled"),
            message: String::from("Humming is disabled on this AppView"),
        }),
    }
}

fn rate_limited() -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("RateLimited"),
            message: String::from("peer exceeded the sendHum rate limit"),
        }),
    }
}

fn forbidden(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("Forbidden"),
            message: message.into(),
        }),
    }
}

fn invalid_request(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: message.into(),
        }),
    }
}

fn internal_error(message: String) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InternalServerError"),
            message,
        }),
    }
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_appview_auth(&auth, &lxm).await })
}

#[post("/xrpc/social.colibri.sync.sendHum?<auth>", data = "<body>")]
/// Ingests a Hum from a peer AppView: authenticates, authorizes against the
/// subject's `presenceService`, injects locally, and relays onward.
pub async fn send_hum(
    auth: &str,
    body: Json<HumEnvelope>,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
    c2c_broadcast_channel: &State<(Sender<EventNotification>, Receiver<EventNotification>)>,
) -> Result<(), ErrorResponse> {
    if !crate::lib::hum_client::humming_enabled() {
        return Err(not_enabled());
    }

    let (load_presence, load_membership, build_user_event, dedup) = prod_deps(db.inner().clone());
    let rate_ok: Box<RateOkFn> =
        Box::new(move |peer: String| crate::lib::hum_guard::rate_ok(&peer));

    receive_hum_with(
        auth.to_string(),
        body.into_inner(),
        &verify_auth_boxed,
        &*rate_ok,
        &*load_presence,
        &*load_membership,
        &*build_user_event,
        &*dedup,
        &bridge.broadcast,
        &c2c_broadcast_channel.0,
        &bridge.hums,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::events::{TypingEventData, UserEventData, UserEventProfile};
    use rocket::tokio;
    use rocket::tokio::sync::broadcast;

    const COMMUNITY: &str = "at://did:plc:community/social.colibri.community/self";

    /// Builds a user Hum. `inner_did`/`inner_handle` are the peer-supplied
    /// identity fields, deliberately distinct from `subject` in some tests to
    /// prove they are ignored.
    fn user_envelope(origin: &str, subject: &str, ttl: u8) -> HumEnvelope {
        HumEnvelope {
            origin: origin.to_string(),
            id: String::from("hum-1"),
            ttl,
            subject: subject.to_string(),
            community: COMMUNITY.to_string(),
            event: HumEvent::User(UserEventData {
                did: String::from("did:plc:peer-claimed-victim"),
                status: None,
                profile: UserEventProfile {
                    display_name: Some(String::from("Spoofed Name")),
                    avatar: None,
                    banner: None,
                    description: None,
                    is_bot: false,
                    handle: String::from("spoofed.handle"),
                    theme: None,
                },
            }),
        }
    }

    fn ok_verify(
        peer: &'static str,
    ) -> impl Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
        move |_, _| Box::pin(async move { Ok(peer.to_string()) })
    }

    fn presence(
        value: Option<&'static str>,
    ) -> impl Fn(String) -> BoxFuture<'static, Result<Option<String>, DbErr>> {
        move |_| Box::pin(async move { Ok(value.map(str::to_string)) })
    }

    fn member(
        is_member: bool,
    ) -> impl Fn(String, String) -> BoxFuture<'static, Result<bool, DbErr>> {
        move |_, _| Box::pin(async move { Ok(is_member) })
    }

    /// A rate check with the given verdict for every peer.
    fn rate(ok: bool) -> impl Fn(String) -> bool {
        move |_| ok
    }

    /// A dedup that reports every id as new (never a duplicate).
    fn fresh() -> impl Fn(String) -> bool {
        |_| true
    }

    /// Stand-in for the production `build_presence_user_event`: attributes the
    /// event to the DID it is *called with* (the authorized subject) and stamps
    /// a fixed locally-derived handle, so a test can prove the peer's identity
    /// fields never reach the broadcast.
    fn build_ue() -> impl Fn(String, UserEventStatus) -> BoxFuture<'static, ColibriServerEvent> {
        move |subject: String, status: UserEventStatus| {
            Box::pin(async move {
                ColibriServerEvent {
                    event_type: String::from("user_event"),
                    data: Some(ColibriServerEventData::User(UserEventData {
                        did: subject,
                        status: Some(status),
                        profile: UserEventProfile {
                            display_name: Some(String::from("Local Name")),
                            avatar: None,
                            banner: None,
                            description: None,
                            is_bot: false,
                            handle: String::from("local.handle"),
                            theme: None,
                        },
                    })),
                }
            })
        }
    }

    #[tokio::test]
    async fn accepts_authorized_user_hum_and_broadcasts_community_scoped() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<SharedScopedEvent>(4);
        let (c2c_tx, _c2c_rx) = broadcast::channel::<EventNotification>(4);
        let (hums_tx, mut hums_rx) = broadcast::channel::<HumEnvelope>(4);

        receive_hum_with(
            String::from("token"),
            user_envelope("did:web:peer", "did:plc:subject", 1),
            &ok_verify("did:web:peer"),
            &rate(true),
            &presence(Some("did:web:peer")),
            &member(true),
            &build_ue(),
            &fresh(),
            &broadcast_tx,
            &c2c_tx,
            &hums_tx,
        )
        .await
        .unwrap();

        let scoped = broadcast_rx.try_recv().unwrap();
        assert_eq!(
            scoped.scope,
            EventScope::Community(String::from("did:plc:community"))
        );

        // ttl 1 → relayed once at ttl 0.
        let relayed = hums_rx.try_recv().unwrap();
        assert_eq!(relayed.ttl, 0);
    }

    #[tokio::test]
    async fn user_hum_uses_locally_rebuilt_identity_not_peer_claim() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<SharedScopedEvent>(4);
        let (c2c_tx, _c2c_rx) = broadcast::channel::<EventNotification>(4);
        let (hums_tx, _hums_rx) = broadcast::channel::<HumEnvelope>(4);

        receive_hum_with(
            String::from("token"),
            // Envelope claims did:plc:peer-claimed-victim / spoofed.handle inside.
            user_envelope("did:web:peer", "did:plc:subject", 0),
            &ok_verify("did:web:peer"),
            &rate(true),
            &presence(Some("did:web:peer")),
            &member(true),
            &build_ue(),
            &fresh(),
            &broadcast_tx,
            &c2c_tx,
            &hums_tx,
        )
        .await
        .unwrap();

        let scoped = broadcast_rx.try_recv().unwrap();
        let json: serde_json::Value = serde_json::from_str(&scoped.payload).unwrap();
        // Attributed to the authorized subject, with the locally-derived handle —
        // the peer's inner `did`/`handle` are discarded.
        assert_eq!(json["data"]["did"], "did:plc:subject");
        assert_eq!(json["data"]["profile"]["handle"], "local.handle");
    }

    #[tokio::test]
    async fn typing_hum_routes_to_c2c_attributed_to_subject() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<SharedScopedEvent>(4);
        let (c2c_tx, mut c2c_rx) = broadcast::channel::<EventNotification>(4);
        let (hums_tx, _hums_rx) = broadcast::channel::<HumEnvelope>(4);

        let channel = "at://did:plc:community/social.colibri.channel/chan-1";
        let mut envelope = user_envelope("did:web:peer", "did:plc:subject", 0);
        envelope.event = HumEvent::Typing(TypingEventData {
            event: String::from("start"),
            channel: String::from(channel),
            did: String::from("did:plc:peer-claimed-victim"),
        });

        receive_hum_with(
            String::from("token"),
            envelope,
            &ok_verify("did:web:peer"),
            &rate(true),
            &presence(Some("did:web:peer")),
            &member(true),
            &build_ue(),
            &fresh(),
            &broadcast_tx,
            &c2c_tx,
            &hums_tx,
        )
        .await
        .unwrap();

        let notif = c2c_rx.try_recv().unwrap();
        assert_eq!(notif.event_type, "typing");
        // Attributed to the authorized subject, not the peer's claimed did.
        assert_eq!(notif.data, vec!["did:plc:subject", channel]);
        assert!(broadcast_rx.try_recv().is_err());
    }

    /// Drives `receive_hum_with` with the given envelope/verify/presence/member
    /// and returns the error name, asserting it rejected.
    async fn expect_reject(
        envelope: HumEnvelope,
        verify: impl Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>
        + Send
        + Sync
        + 'static,
        presence_fn: impl Fn(String) -> BoxFuture<'static, Result<Option<String>, DbErr>>
        + Send
        + Sync
        + 'static,
        member_fn: impl Fn(String, String) -> BoxFuture<'static, Result<bool, DbErr>>
        + Send
        + Sync
        + 'static,
    ) -> String {
        let (broadcast_tx, _) = broadcast::channel::<SharedScopedEvent>(4);
        let (c2c_tx, _) = broadcast::channel::<EventNotification>(4);
        let (hums_tx, _) = broadcast::channel::<HumEnvelope>(4);

        let result = receive_hum_with(
            String::from("token"),
            envelope,
            &verify,
            &rate(true),
            &presence_fn,
            &member_fn,
            &build_ue(),
            &fresh(),
            &broadcast_tx,
            &c2c_tx,
            &hums_tx,
        )
        .await;

        result.err().unwrap().body.into_inner().error
    }

    #[tokio::test]
    async fn rejects_non_did_web_origin() {
        // A user minting their own service-auth token (did:plc origin) is refused
        // before any presence/membership work.
        let error = expect_reject(
            user_envelope("did:plc:selfish", "did:plc:selfish", 1),
            ok_verify("did:plc:selfish"),
            presence(Some("did:plc:selfish")),
            member(true),
        )
        .await;
        assert_eq!(error, "Forbidden");
    }

    #[tokio::test]
    async fn rejects_when_origin_differs_from_peer() {
        let error = expect_reject(
            user_envelope("did:web:claimed-origin", "did:plc:subject", 1),
            ok_verify("did:web:actual-peer"),
            presence(Some("did:web:claimed-origin")),
            member(true),
        )
        .await;
        assert_eq!(error, "Forbidden");
    }

    #[tokio::test]
    async fn rejects_when_origin_is_not_presence_service() {
        let error = expect_reject(
            user_envelope("did:web:peer", "did:plc:subject", 1),
            ok_verify("did:web:peer"),
            // Subject authorized a different AppView (or opted out → None).
            presence(Some("did:web:other")),
            member(true),
        )
        .await;
        assert_eq!(error, "Forbidden");
    }

    #[tokio::test]
    async fn rejects_when_subject_is_not_a_member() {
        let error = expect_reject(
            user_envelope("did:web:peer", "did:plc:subject", 1),
            ok_verify("did:web:peer"),
            presence(Some("did:web:peer")),
            member(false),
        )
        .await;
        assert_eq!(error, "Forbidden");
    }

    #[tokio::test]
    async fn rejects_when_channel_not_in_community() {
        let mut envelope = user_envelope("did:web:peer", "did:plc:subject", 1);
        envelope.event = HumEvent::Typing(TypingEventData {
            event: String::from("start"),
            // Channel belongs to a different community than the envelope's.
            channel: String::from("at://did:plc:other-community/social.colibri.channel/x"),
            did: String::from("did:plc:subject"),
        });

        let error = expect_reject(
            envelope,
            ok_verify("did:web:peer"),
            presence(Some("did:web:peer")),
            member(true),
        )
        .await;
        assert_eq!(error, "Forbidden");
    }

    #[tokio::test]
    async fn does_not_relay_at_ttl_zero() {
        let (broadcast_tx, _broadcast_rx) = broadcast::channel::<SharedScopedEvent>(4);
        let (c2c_tx, _c2c_rx) = broadcast::channel::<EventNotification>(4);
        let (hums_tx, mut hums_rx) = broadcast::channel::<HumEnvelope>(4);

        receive_hum_with(
            String::from("token"),
            user_envelope("did:web:peer", "did:plc:subject", 0),
            &ok_verify("did:web:peer"),
            &rate(true),
            &presence(Some("did:web:peer")),
            &member(true),
            &build_ue(),
            &fresh(),
            &broadcast_tx,
            &c2c_tx,
            &hums_tx,
        )
        .await
        .unwrap();

        assert!(hums_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn rejects_when_peer_is_rate_limited() {
        let error = {
            let (broadcast_tx, _) = broadcast::channel::<SharedScopedEvent>(4);
            let (c2c_tx, _) = broadcast::channel::<EventNotification>(4);
            let (hums_tx, _) = broadcast::channel::<HumEnvelope>(4);
            receive_hum_with(
                String::from("token"),
                user_envelope("did:web:peer", "did:plc:subject", 1),
                &ok_verify("did:web:peer"),
                &rate(false),
                &presence(Some("did:web:peer")),
                &member(true),
                &build_ue(),
                &fresh(),
                &broadcast_tx,
                &c2c_tx,
                &hums_tx,
            )
            .await
            .err()
            .unwrap()
            .body
            .into_inner()
            .error
        };
        assert_eq!(error, "RateLimited");
    }

    #[tokio::test]
    async fn drops_duplicate_hum_without_broadcasting() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<SharedScopedEvent>(4);
        let (c2c_tx, _c2c_rx) = broadcast::channel::<EventNotification>(4);
        let (hums_tx, mut hums_rx) = broadcast::channel::<HumEnvelope>(4);

        // Dedup reports the id as already-seen → injection is skipped entirely.
        receive_hum_with(
            String::from("token"),
            user_envelope("did:web:peer", "did:plc:subject", 1),
            &ok_verify("did:web:peer"),
            &rate(true),
            &presence(Some("did:web:peer")),
            &member(true),
            &build_ue(),
            &(|_| false),
            &broadcast_tx,
            &c2c_tx,
            &hums_tx,
        )
        .await
        .unwrap();

        assert!(broadcast_rx.try_recv().is_err());
        assert!(hums_rx.try_recv().is_err());
    }
}
