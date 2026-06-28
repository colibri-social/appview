use crate::EventNotification;
use crate::lib::at_uri::AtUri;
use crate::lib::event_scope::{EventScope, SharedScopedEvent};
use crate::lib::events::{
    ColibriServerEventData, CommunityCreationProgressEvent, MuteEvent, NotificationEventData,
    NotificationEventMessage, SeenEvent, TypingEventData, TypingMessageData, ViewData,
    VoiceChannelData,
};
use crate::lib::get_state::get_did_states;
use crate::lib::notifications::IndexedNotification;
use crate::lib::state::{broadcast_state_change, join_vc, leave_vc, view_channel};
use crate::lib::tap::CommsBridge;
use crate::xrpc::social::colibri::actor::list_communities_handler::get_authorized_communities;
use crate::{
    lib::{
        events::{ColibriClientEvent, ColibriServerEvent},
        responses::{ErrorBody, ErrorResponse},
        service_auth,
        tap::register_dids,
    },
    xrpc::social::colibri::actor::set_state_handler::save_state,
};
use ::serde::Serialize;
use futures_util::{SinkExt, StreamExt};
use rocket::request::{FromRequest, Outcome};
use rocket::response::{self, Responder, Response};
use rocket::tokio::sync::broadcast::error::RecvError;
use rocket::tokio::sync::broadcast::{Receiver, Sender};
use rocket::{Request, State, get, serde::json::Json, tokio};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use sea_orm::DatabaseConnection;
use std::collections::HashSet;

type WsSink = futures_util::stream::SplitSink<DuplexStream, WsMessage>;

#[derive(Serialize)]
pub struct DidStruct {
    pub dids: Vec<String>,
}

async fn parse_client_event(
    text: &str,
    did: String,
    to_c2c_broadcast: &Sender<EventNotification>,
    db: &DatabaseConnection,
) -> Option<String> {
    let user_message = serde_json::from_str::<ColibriClientEvent>(text).ok()?;

    // `view`, `voice_join`, `voice_leave`
    match user_message.event_type.as_str() {
        "heartbeat" => {
            let ack_res = ColibriServerEvent {
                event_type: String::from("ack"),
                data: None,
            };

            Some(serde_json::to_string(&ack_res).unwrap())
        }
        "typing" => {
            let typing_msg_data: TypingMessageData =
                serde_json::from_value(user_message.data?).ok()?;

            let _ = to_c2c_broadcast.send(EventNotification {
                event_type: String::from("typing"),
                data: vec![did, typing_msg_data.channel],
            });

            None
        }
        "view" => {
            let view_msg_data: ViewData = serde_json::from_value(user_message.data?).ok()?;

            view_channel(did, view_msg_data.channel, db).await;

            None
        }
        "voice_join" => {
            let vc_msg_data: VoiceChannelData = serde_json::from_value(user_message.data?).ok()?;

            join_vc(did, vc_msg_data.channel, vc_msg_data.community, db).await;

            None
        }
        "voice_leave" => {
            // TODO: Get current VC before removing, notify others
            leave_vc(did, db).await;
            None
        }
        _ => None,
    }
}

async fn serialize_typing_broadcast(
    msg: EventNotification,
    did: String,
    db: &DatabaseConnection,
) -> Option<String> {
    if msg.event_type != "typing" || msg.data.len() < 2 {
        return None;
    }

    let msg_did = msg.data[0].clone();
    let msg_channel = msg.data[1].clone();

    // Filter out user who is typing
    if msg_did == did {
        return None;
    }

    let states = get_did_states(did.clone(), db).await;

    if states.is_err() {
        log::warn!(
            "Unable to fetch states for {}: {:?}",
            did,
            states.map_err(|x| x.to_string())
        );
        return None;
    }

    if states.unwrap().channel.as_deref() != Some(msg_channel.as_str()) {
        return None;
    }

    let event = ColibriServerEvent {
        event_type: String::from("typing_event"),
        data: Some(ColibriServerEventData::Typing(TypingEventData {
            channel: msg_channel,
            did: msg_did,
            event: String::from("start"),
        })),
    };

    Some(event.serialize())
}

/// Loads the set of community DIDs the user belongs to. Events scoped to a
/// community are only forwarded to connections whose set contains it. Returns an
/// empty set (and logs) on failure — degraded, but never leaks other
/// communities' events.
async fn community_set(db: &DatabaseConnection, did: &str) -> HashSet<String> {
    match get_authorized_communities(db, did).await {
        Ok(list) => list.into_iter().map(|c| c.community.did).collect(),
        Err(e) => {
            log::error!("failed to load community set for {did}: {e}");
            HashSet::new()
        }
    }
}

/// Whether an event with the given scope should be delivered to the connection
/// for `did` whose communities are `communities`.
fn scope_matches(scope: &EventScope, did: &str, communities: &HashSet<String>) -> bool {
    match scope {
        EventScope::Global => true,
        EventScope::User(target) => target == did,
        EventScope::Community(community) => communities.contains(community),
    }
}

/// Forwards a pre-mapped, scope-tagged event to this connection iff the
/// connected user is in its audience. The heavy mapping/enrichment already
/// happened once in the tap loop, so this is a cheap in-memory check plus the
/// socket write — no per-client database work.
///
/// Returns false if the client has disconnected.
async fn forward_scoped_event(
    ws_sink: &mut WsSink,
    event: SharedScopedEvent,
    did: &str,
    communities: &mut HashSet<String>,
    db: &DatabaseConnection,
) -> bool {
    if !scope_matches(&event.scope, did, communities) {
        return true;
    }

    // A delivered `User`-scoped event is always one of this user's own
    // membership changes (their join, leave, or kick), so refresh the cached
    // community set to keep subsequent community-scoped routing correct.
    if matches!(event.scope, EventScope::User(_)) {
        *communities = community_set(db, did).await;
    }

    ws_sink
        .send(WsMessage::Text(event.payload.clone()))
        .await
        .is_ok()
}

/// Returns false if the tap stream has closed or errored.
async fn handle_tap_message(
    ws_sink: &mut WsSink,
    event: Result<SharedScopedEvent, RecvError>,
    did: &str,
    communities: &mut HashSet<String>,
    db: &DatabaseConnection,
) -> bool {
    match event {
        Ok(ev) => forward_scoped_event(ws_sink, ev, did, communities, db).await,
        Err(e) => {
            eprintln!("Tap stream error: {e}");
            false
        }
    }
}

/// Returns false if the client has closed or errored.
async fn handle_client_message(
    ws_sink: &mut WsSink,
    msg: Option<Result<WsMessage, rocket_ws::result::Error>>,
    did: String,
    to_c2c_broadcast: &Sender<EventNotification>,
    db: &DatabaseConnection,
) -> bool {
    match msg {
        Some(Ok(WsMessage::Close(_))) | None => false,
        Some(Ok(WsMessage::Text(text))) => {
            if let Some(serialized_ack_res) =
                parse_client_event(&text, did, to_c2c_broadcast, db).await
            {
                let _ = ws_sink.send(WsMessage::Text(serialized_ack_res)).await;
            }

            true
        }
        _ => true,
    }
}

async fn handle_client_broadcast_msg(
    ws_sink: &mut WsSink,
    msg: Result<EventNotification, RecvError>,
    did: String,
    db: &DatabaseConnection,
) -> bool {
    match msg {
        Ok(msg) => {
            if let Some(payload) = serialize_typing_broadcast(msg, did, db).await {
                let _ = ws_sink.send(WsMessage::Text(payload)).await;
            }
            true
        }
        Err(e) => {
            eprintln!("Client broadcast error: {e}");
            false
        }
    }
}

fn serialize_notification_for(indexed: IndexedNotification, did: &str) -> Option<String> {
    if indexed.row.recipient_did != did {
        return None;
    }
    if !indexed.row.channel_uri.starts_with("at://") {
        return None;
    }
    let event = ColibriServerEvent {
        event_type: String::from("notification_event"),
        data: Some(ColibriServerEventData::Notification(
            NotificationEventData {
                id: indexed.row.id,
                kind: indexed.row.kind,
                message_uri: indexed.row.message_uri,
                author_did: indexed.row.author_did,
                channel_uri: indexed.row.channel_uri,
                indexed_at: indexed.row.indexed_at,
                message: NotificationEventMessage {
                    text: indexed.message.text,
                    facets: indexed.message.facets,
                    created_at: indexed.message.created_at,
                    parent: indexed.message.parent,
                    attachments: indexed.message.attachments,
                    edited: indexed.message.edited,
                },
            },
        )),
    };
    Some(event.serialize())
}

async fn handle_notification_msg(
    ws_sink: &mut WsSink,
    msg: Result<IndexedNotification, RecvError>,
    did: &str,
) -> bool {
    match msg {
        Ok(indexed) => {
            if let Some(payload) = serialize_notification_for(indexed, did) {
                let _ = ws_sink.send(WsMessage::Text(payload)).await;
            }
            true
        }
        Err(e) => {
            eprintln!("Notification broadcast error: {e}");
            false
        }
    }
}

/// The community DID an `application_event` targets, parsed from its
/// `ApplicationEventData.community` AT-URI. `None` for non-application events
/// or a malformed URI.
fn application_community_did(event: &ColibriServerEvent) -> Option<String> {
    match &event.data {
        Some(ColibriServerEventData::Application(data)) => {
            AtUri::parse(&data.community).map(|uri| uri.authority)
        }
        _ => None,
    }
}

/// Forwards a handler-originated `application_event` (see
/// `CommsBridge::applications`) to this connection iff the user is a member of
/// the target community. A moderator is always a member, so member-scoping is a
/// safe superset of the moderators who can act on the application — and it
/// keeps pending-application traffic off connections in other communities.
async fn handle_application_msg(
    ws_sink: &mut WsSink,
    msg: Result<ColibriServerEvent, RecvError>,
    communities: &HashSet<String>,
) -> bool {
    match msg {
        Ok(event) => {
            match application_community_did(&event) {
                Some(community_did) if communities.contains(&community_did) => {
                    let _ = ws_sink.send(WsMessage::Text(event.serialize())).await;
                }
                Some(_) => {} // application for a community this user isn't in
                None => {
                    log::warn!("dropping application_event with unparseable community");
                }
            }
            true
        }
        Err(e) => {
            eprintln!("Application broadcast error: {e}");
            false
        }
    }
}

/// Builds the `seen_event` payload for a subscriber, or `None` if the event
/// belongs to a different user — a `seen_event` is delivered only to the
/// originating user's own connections (cross-device read-state sync).
fn serialize_seen_for(seen: SeenEvent, did: &str) -> Option<String> {
    if seen.recipient_did != did {
        return None;
    }
    let event = ColibriServerEvent {
        event_type: String::from("seen_event"),
        data: Some(ColibriServerEventData::Seen(seen.data)),
    };
    Some(event.serialize())
}

async fn handle_seen_msg(
    ws_sink: &mut WsSink,
    msg: Result<SeenEvent, RecvError>,
    did: &str,
) -> bool {
    match msg {
        Ok(seen) => {
            if let Some(payload) = serialize_seen_for(seen, did) {
                let _ = ws_sink.send(WsMessage::Text(payload)).await;
            }
            true
        }
        Err(e) => {
            eprintln!("Seen broadcast error: {e}");
            false
        }
    }
}

/// Builds the `mute_event` payload for a subscriber, or `None` if the event
/// belongs to a different user — a `mute_event` is delivered only to the
/// originating user's own connections (cross-device mute sync).
fn serialize_mute_for(mute: MuteEvent, did: &str) -> Option<String> {
    if mute.recipient_did != did {
        return None;
    }
    let event = ColibriServerEvent {
        event_type: String::from("mute_event"),
        data: Some(ColibriServerEventData::Mute(mute.data)),
    };
    Some(event.serialize())
}

async fn handle_mute_msg(
    ws_sink: &mut WsSink,
    msg: Result<MuteEvent, RecvError>,
    did: &str,
) -> bool {
    match msg {
        Ok(mute) => {
            if let Some(payload) = serialize_mute_for(mute, did) {
                let _ = ws_sink.send(WsMessage::Text(payload)).await;
            }
            true
        }
        Err(e) => {
            eprintln!("Mute broadcast error: {e}");
            false
        }
    }
}

/// Builds the `community_creation_progress` payload for a subscriber, or `None`
/// if the event belongs to a different user — progress is delivered only to the
/// creating user's own connections, so other clients never learn that a
/// community is being created.
fn serialize_progress_for(event: CommunityCreationProgressEvent, did: &str) -> Option<String> {
    if event.recipient_did != did {
        return None;
    }
    let server_event = ColibriServerEvent {
        event_type: String::from("community_creation_progress"),
        data: Some(ColibriServerEventData::CommunityCreationProgress(
            event.data,
        )),
    };
    Some(server_event.serialize())
}

async fn handle_progress_msg(
    ws_sink: &mut WsSink,
    msg: Result<CommunityCreationProgressEvent, RecvError>,
    did: &str,
) -> bool {
    match msg {
        Ok(event) => {
            if let Some(payload) = serialize_progress_for(event, did) {
                let _ = ws_sink.send(WsMessage::Text(payload)).await;
            }
            true
        }
        Err(e) => {
            eprintln!("Progress broadcast error: {e}");
            false
        }
    }
}

/// Handles the event loop and allows both messages from Tap and the Client to get processed.
#[allow(clippy::too_many_arguments)]
async fn run_event_loop(
    io: DuplexStream,
    from_tap: Receiver<SharedScopedEvent>,
    to_tap_broadcast: Sender<SharedScopedEvent>,
    did: String,
    db: DatabaseConnection,
    to_c2c_broadcast: Sender<EventNotification>,
    from_c2c_broadcast: Receiver<EventNotification>,
    from_notifications: Receiver<IndexedNotification>,
    from_applications: Receiver<ColibriServerEvent>,
    from_seen: Receiver<SeenEvent>,
    from_mute: Receiver<MuteEvent>,
    from_progress: Receiver<CommunityCreationProgressEvent>,
) {
    let (mut ws_sink, mut ws_source) = io.split();
    let mut from_tap = from_tap;
    let mut from_c2c_broadcast = from_c2c_broadcast;
    let mut from_notifications = from_notifications;
    let mut from_applications = from_applications;
    let mut from_seen = from_seen;
    let mut from_mute = from_mute;
    let mut from_progress = from_progress;

    save_state(&db, did.clone(), String::from("online")).await;
    register_dids(vec![did.clone()]).await;
    broadcast_state_change(&to_tap_broadcast, &did, &db).await;

    // The communities this user belongs to, used to route `Community`-scoped
    // events. Refreshed whenever the user's own membership changes (see
    // `forward_scoped_event`).
    let mut communities = community_set(&db, &did).await;

    loop {
        let connected = tokio::select! {
            msg = from_tap.recv() => handle_tap_message(&mut ws_sink, msg, &did, &mut communities, &db).await,
            msg = ws_source.next() => handle_client_message(&mut ws_sink, msg, did.clone(), &to_c2c_broadcast, &db).await,
            msg = from_c2c_broadcast.recv() => handle_client_broadcast_msg(&mut ws_sink, msg, did.clone(), &db).await,
            msg = from_notifications.recv() => handle_notification_msg(&mut ws_sink, msg, &did).await,
            msg = from_applications.recv() => handle_application_msg(&mut ws_sink, msg, &communities).await,
            msg = from_seen.recv() => handle_seen_msg(&mut ws_sink, msg, &did).await,
            msg = from_mute.recv() => handle_mute_msg(&mut ws_sink, msg, &did).await,
            msg = from_progress.recv() => handle_progress_msg(&mut ws_sink, msg, &did).await,
        };

        if !connected {
            break;
        }
    }

    save_state(&db, did.clone(), String::from("offline")).await;
    broadcast_state_change(&to_tap_broadcast, &did, &db).await;
}

/// Sentinel `Sec-WebSocket-Protocol` value that flags the *next* offered
/// subprotocol as a service-auth token.
const AUTH_SUBPROTOCOL: &str = "colibri.auth.bearer";

/// The service-auth token offered via `Sec-WebSocket-Protocol`, if any.
pub struct SubprotocolAuth {
    token: Option<String>,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for SubprotocolAuth {
    type Error = std::convert::Infallible;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let token = req
            .headers()
            .get_one("Sec-WebSocket-Protocol")
            .and_then(parse_auth_subprotocol)
            .map(str::to_owned);
        Outcome::Success(SubprotocolAuth { token })
    }
}

/// Returns the token that follows the [`AUTH_SUBPROTOCOL`] sentinel in a
/// comma-separated `Sec-WebSocket-Protocol` header value, or `None` if the
/// sentinel is absent or has nothing after it.
fn parse_auth_subprotocol(header: &str) -> Option<&str> {
    let protocols: Vec<&str> = header.split(',').map(str::trim).collect();
    let sentinel = protocols.iter().position(|&p| p == AUTH_SUBPROTOCOL)?;
    protocols.get(sentinel + 1).copied()
}

/// Wraps a rocket_ws [`Channel`](rocket_ws::Channel) so the handshake response
/// echoes the negotiated subprotocol.
pub struct ChannelWithProtocol {
    channel: rocket_ws::Channel<'static>,
    protocol: Option<&'static str>,
}

impl<'r> Responder<'r, 'static> for ChannelWithProtocol {
    fn respond_to(self, request: &'r Request<'_>) -> response::Result<'static> {
        let mut response: Response<'static> = self.channel.respond_to(request)?;
        if let Some(protocol) = self.protocol {
            response.set_raw_header("Sec-WebSocket-Protocol", protocol);
        }
        Ok(response)
    }
}

#[get("/xrpc/social.colibri.sync.subscribeEvents?<auth>")]
pub async fn subscribe_events(
    auth: Option<&str>,
    subprotocol_auth: SubprotocolAuth,
    ws: WebSocket,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
    c2c_broadcast_channel: &State<(Sender<EventNotification>, Receiver<EventNotification>)>,
) -> Result<ChannelWithProtocol, ErrorResponse> {
    // Current clients smuggle the token through the subprotocol; the `?auth=`
    // query parameter is kept as a transitional / local-dev fallback.
    let used_subprotocol = subprotocol_auth.token.is_some();
    let token = subprotocol_auth
        .token
        .or_else(|| auth.map(str::to_owned))
        .unwrap_or_default();

    let did = service_auth::verify_service_auth(&token, "social.colibri.sync.subscribeEvents")
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    log::info!("User connected to social.colibri.sync.subscribeEvents: {did}");

    let cloned_db = db.inner().clone();

    let from_tap = bridge.broadcast.subscribe();
    let to_tap_broadcast = bridge.broadcast.clone();
    let from_notifications = bridge.notifications.subscribe();
    let from_applications = bridge.applications.subscribe();
    let from_seen = bridge.seen.subscribe();
    let from_mute = bridge.mute.subscribe();
    let from_progress = bridge.progress.subscribe();

    let to_c2c_broadcast = c2c_broadcast_channel.0.clone();
    let from_c2c_broadcast = to_c2c_broadcast.subscribe();

    let channel = ws.channel(move |io| {
        Box::pin(async move {
            run_event_loop(
                io,
                from_tap,
                to_tap_broadcast,
                did,
                cloned_db,
                to_c2c_broadcast,
                from_c2c_broadcast,
                from_notifications,
                from_applications,
                from_seen,
                from_mute,
                from_progress,
            )
            .await;
            Ok(())
        })
    });

    Ok(ChannelWithProtocol {
        channel,
        // Only reflect the sentinel when the client actually used it, so the
        // browser accepts the handshake. The legacy query-param path leaves the
        // response untouched.
        protocol: used_subprotocol.then_some(AUTH_SUBPROTOCOL),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AUTH_SUBPROTOCOL, application_community_did, parse_auth_subprotocol, parse_client_event,
        scope_matches, serialize_typing_broadcast,
    };
    use crate::EventNotification;
    use crate::lib::event_scope::EventScope;
    use crate::lib::events::{ApplicationEventData, ColibriServerEvent, ColibriServerEventData};
    use crate::lib::test_fixtures::mock_db;
    use crate::models::user_states;
    use rocket::tokio;
    use rocket::tokio::sync::broadcast;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};
    use std::collections::HashSet;

    #[test]
    fn scope_matches_routes_each_scope() {
        let did = "did:plc:me";
        let communities: HashSet<String> =
            [String::from("did:plc:community-a")].into_iter().collect();

        // Global → everyone.
        assert!(scope_matches(&EventScope::Global, did, &communities));

        // User → only the targeted DID.
        assert!(scope_matches(
            &EventScope::User(String::from("did:plc:me")),
            did,
            &communities
        ));
        assert!(!scope_matches(
            &EventScope::User(String::from("did:plc:other")),
            did,
            &communities
        ));

        // Community → only members of that community.
        assert!(scope_matches(
            &EventScope::Community(String::from("did:plc:community-a")),
            did,
            &communities
        ));
        assert!(!scope_matches(
            &EventScope::Community(String::from("did:plc:community-b")),
            did,
            &communities
        ));
    }

    #[test]
    fn parse_auth_subprotocol_reads_token_after_sentinel() {
        assert_eq!(
            parse_auth_subprotocol(&format!("{AUTH_SUBPROTOCOL}, jwt-token")),
            Some("jwt-token")
        );
        // A realistic JWT (base64url, dot-separated) survives intact.
        let jwt = "eyJhbGciOiJFUzI1NiJ9.eyJpc3MiOiJkaWQ6cGxjOmFiYyJ9.sig-_123";
        assert_eq!(
            parse_auth_subprotocol(&format!("{AUTH_SUBPROTOCOL},{jwt}")),
            Some(jwt)
        );
    }

    #[test]
    fn parse_auth_subprotocol_rejects_missing_or_empty() {
        // No sentinel at all.
        assert_eq!(parse_auth_subprotocol("some.other.protocol"), None);
        // Sentinel present but nothing follows it.
        assert_eq!(parse_auth_subprotocol(AUTH_SUBPROTOCOL), None);
    }

    #[test]
    fn application_community_did_extracts_authority() {
        let event = ColibriServerEvent {
            event_type: String::from("application_event"),
            data: Some(ColibriServerEventData::Application(ApplicationEventData {
                event: String::from("create"),
                community: String::from("at://did:plc:owner/social.colibri.community/self"),
                membership: String::from("at://did:plc:abc/social.colibri.membership/r1"),
                did: Some(String::from("did:plc:abc")),
                handle: None,
                created_at: None,
                data: None,
            })),
        };
        assert_eq!(
            application_community_did(&event),
            Some(String::from("did:plc:owner"))
        );
    }

    #[tokio::test]
    async fn creates_ack_for_heartbeat_event() {
        let (tx, _) = broadcast::channel(4);
        let db = mock_db();
        let ack = parse_client_event(
            r#"{"type":"heartbeat","data":null}"#,
            String::from("did:plc:me"),
            &tx,
            &db,
        )
        .await
        .expect("expected ack");
        let json: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(json["type"], "ack");
    }

    #[tokio::test]
    async fn emits_typing_notification_for_typing_event() {
        let (tx, mut rx) = broadcast::channel(4);
        let db = mock_db();
        let res = parse_client_event(
            r#"{"type":"typing","data":{"channel":"community-1"}}"#,
            String::from("did:plc:me"),
            &tx,
            &db,
        )
        .await;

        assert!(res.is_none());
        let notif = rx.try_recv().unwrap();
        assert_eq!(notif.event_type, "typing");
        assert_eq!(notif.data, vec!["did:plc:me", "community-1"]);
    }

    #[tokio::test]
    async fn ignores_typing_event_without_data() {
        let (tx, mut rx) = broadcast::channel(4);
        let db = mock_db();
        let res = parse_client_event(
            r#"{"type":"typing","data":null}"#,
            String::from("did:plc:me"),
            &tx,
            &db,
        )
        .await;

        assert!(res.is_none());
        assert!(rx.try_recv().is_err());
    }

    /// Regression test: `TypingMessageData` and `ViewData` are both `{
    /// channel: String }`. Before the fix, `data` was an untagged
    /// `ColibriClientEventData` enum, which always deserialized such a
    /// payload as `TypingMessage` (declared first) regardless of the
    /// `event_type` field — so a `"view"` event never reached `view_channel`
    /// and no DB write ever happened. This asserts the write actually fires.
    #[tokio::test]
    async fn view_event_writes_viewed_channel() {
        let (tx, _) = broadcast::channel(4);
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_exec_results([MockExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            }])
            .into_connection();

        let res = parse_client_event(
            r#"{"type":"view","data":{"channel":"community-1"}}"#,
            String::from("did:plc:me"),
            &tx,
            &db,
        )
        .await;

        assert!(res.is_none());
        let log = db.into_transaction_log();
        assert_eq!(
            log.len(),
            1,
            "expected exactly one DB write from view_channel"
        );
    }

    #[tokio::test]
    async fn ignores_unknown_client_event() {
        let (tx, _) = broadcast::channel(4);
        let db = mock_db();
        let res = parse_client_event(
            r#"{"type":"unknown","data":null}"#,
            String::from("did:plc:me"),
            &tx,
            &db,
        )
        .await;
        assert!(res.is_none());
    }

    #[tokio::test]
    async fn serializes_typing_broadcast_for_other_users() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![user_states::Model {
                did: String::from("did:plc:me"),
                state: String::from("online"),
                vc: None,
                vc_community: None,
                channel: Some(String::from("community-1")),
            }]])
            .into_connection();
        let payload = serialize_typing_broadcast(
            EventNotification {
                event_type: String::from("typing"),
                data: vec![String::from("did:plc:other"), String::from("community-1")],
            },
            String::from("did:plc:me"),
            &db,
        )
        .await
        .unwrap();

        let json: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(json["type"], "typing_event");
        assert_eq!(json["data"]["did"], "did:plc:other");
        assert_eq!(json["data"]["channel"], "community-1");
    }

    #[tokio::test]
    async fn ignores_typing_broadcast_from_same_user_or_invalid_payload() {
        let db = mock_db();
        let same_user = serialize_typing_broadcast(
            EventNotification {
                event_type: String::from("typing"),
                data: vec![String::from("did:plc:me"), String::from("community-1")],
            },
            String::from("did:plc:me"),
            &db,
        )
        .await;
        assert!(same_user.is_none());

        let invalid = serialize_typing_broadcast(
            EventNotification {
                event_type: String::from("typing"),
                data: vec![String::from("did:plc:other")],
            },
            String::from("did:plc:me"),
            &db,
        )
        .await;
        assert!(invalid.is_none());
    }
}
