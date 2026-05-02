use crate::lib::events::{ColibriClientEventData, ColibriServerEventData, TypingEventData};
use crate::lib::map_tap_event::map_tap_event;
use crate::lib::tap::TapMessageRecord;
use crate::{CommsBridge, EventNotification};
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
use rocket::tokio::sync::broadcast::error::RecvError;
use rocket::tokio::sync::broadcast::{Receiver, Sender};
use rocket::{State, get, serde::json::Json, tokio};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use sea_orm::DatabaseConnection;

type WsSink = futures_util::stream::SplitSink<DuplexStream, WsMessage>;

#[derive(Serialize)]
pub struct DidStruct {
    pub dids: Vec<String>,
}

fn parse_client_event(
    text: &str,
    did: String,
    to_c2c_broadcast: &Sender<EventNotification>,
) -> Option<String> {
    let user_message = serde_json::from_str::<ColibriClientEvent>(text).ok()?;

    match user_message.event_type.as_str() {
        "heartbeat" => {
            let ack_res = ColibriServerEvent {
                event_type: String::from("ack"),
                data: None,
                is_relevant: true,
            };

            Some(serde_json::to_string(&ack_res).unwrap())
        }
        "typing" => {
            let user_message_data: ColibriClientEventData = user_message.data?;

            let typing_msg_data = match user_message_data {
                ColibriClientEventData::TypingMessageData(data) => Some(data),
            };

            if typing_msg_data.is_none() {
                // Event data is invalid
                return None;
            }

            let _ = to_c2c_broadcast.send(EventNotification {
                event_type: String::from("typing"),
                data: vec![did, typing_msg_data.unwrap().channel],
            });

            None
        }
        _ => None,
    }
}

fn serialize_typing_broadcast(msg: EventNotification, did: String) -> Option<String> {
    if msg.event_type != "typing" || msg.data.len() < 2 {
        return None;
    }

    let msg_did = msg.data[0].clone();
    if msg_did == did {
        return None;
    }

    let event = ColibriServerEvent {
        event_type: String::from("typing_event"),
        data: Some(ColibriServerEventData::TypingEventData(TypingEventData {
            channel: msg.data[1].clone(),
            did: msg_did,
            event: String::from("start"),
        })),
        is_relevant: true,
    };

    Some(event.serialize())
}

/// Returns false if the client has disconnected.
async fn forward_to_client(
    ws_sink: &mut WsSink,
    record: TapMessageRecord,
    did: String,
    db: &DatabaseConnection,
) -> bool {
    // if record.did != did {
    //     return true;
    // };

    let mapped_tap_event = map_tap_event(&record, &did, &db).await;

    if mapped_tap_event.is_err() {
        let err = mapped_tap_event.unwrap_err().to_string();

        if err != "Facet" {
            log::error!("Unable to handle tap message {:?}: {}", record, err);
        }
        return true;
    }

    let safe_event = mapped_tap_event.unwrap();

    if !safe_event.is_relevant {
        // The mapper has determined that an event is not needed for this client
        return true;
    }

    if ws_sink
        .send(WsMessage::Text(safe_event.serialize()))
        .await
        .is_err()
    {
        return false;
    }

    true
}

/// Returns false if the tap stream has closed or errored.
async fn handle_tap_message(
    ws_sink: &mut WsSink,
    record: Result<TapMessageRecord, RecvError>,
    did: String,
    db: &DatabaseConnection,
) -> bool {
    match record {
        Ok(msg) => forward_to_client(ws_sink, msg, did, db).await,
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
) -> bool {
    match msg {
        Some(Ok(WsMessage::Close(_))) | None => false,
        Some(Ok(WsMessage::Text(text))) => {
            // TODO: Handle messages (https://next.colibri.social/docs/specification/appview/#messages)
            //       `heartbeat` and `typing` are already handled.
            //       Left: `subscribe`, `unsubscribe`, `view`, `voice_join`,  `voice_leave`
            if let Some(serialized_ack_res) = parse_client_event(&text, did, to_c2c_broadcast) {
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
) -> bool {
    match msg {
        Ok(msg) => {
            if let Some(payload) = serialize_typing_broadcast(msg, did) {
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

/// Handles the event loop and allows both messages from Tap and the Client to get processed.
async fn run_event_loop(
    io: DuplexStream,
    from_tap: Receiver<TapMessageRecord>,
    did: String,
    db: DatabaseConnection,
    to_c2c_broadcast: Sender<EventNotification>,
    from_c2c_broadcast: Receiver<EventNotification>,
) {
    let (mut ws_sink, mut ws_source) = io.split();
    let mut from_tap = from_tap;
    let mut from_c2c_broadcast = from_c2c_broadcast;

    save_state(&db, did.clone(), String::from("online")).await;
    register_dids(vec![did.clone()]).await;

    loop {
        let connected = tokio::select! {
            msg = from_tap.recv() => handle_tap_message(&mut ws_sink, msg, did.clone(), &db).await,
            msg = ws_source.next() => handle_client_message(&mut ws_sink, msg, did.clone(), &to_c2c_broadcast).await,
            msg = from_c2c_broadcast.recv() => handle_client_broadcast_msg(&mut ws_sink, msg, did.clone()).await,
        };

        if !connected {
            break;
        }
    }

    save_state(&db, did.clone(), String::from("offline")).await;
}

#[get("/xrpc/social.colibri.sync.subscribeEvents?<auth>")]
pub async fn subscribe_events(
    auth: &str,
    ws: WebSocket,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
    c2c_broadcast_channel: &State<(Sender<EventNotification>, Receiver<EventNotification>)>,
) -> Result<rocket_ws::Channel<'static>, ErrorResponse> {
    let did = service_auth::verify_service_auth(auth, "social.colibri.sync.subscribeEvents")
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

    let to_c2c_broadcast = c2c_broadcast_channel.0.clone();
    let from_c2c_broadcast = to_c2c_broadcast.subscribe();

    Ok(ws.channel(move |io| {
        Box::pin(async move {
            run_event_loop(
                io,
                from_tap,
                did,
                cloned_db,
                to_c2c_broadcast,
                from_c2c_broadcast,
            )
            .await;
            Ok(())
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::{parse_client_event, serialize_typing_broadcast};
    use crate::EventNotification;
    use rocket::tokio::sync::broadcast;

    #[test]
    fn creates_ack_for_heartbeat_event() {
        let (tx, _) = broadcast::channel(4);
        let ack = parse_client_event(
            r#"{"type":"heartbeat","data":null}"#,
            String::from("did:plc:me"),
            &tx,
        )
        .expect("expected ack");
        let json: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(json["type"], "ack");
    }

    #[test]
    fn emits_typing_notification_for_typing_event() {
        let (tx, mut rx) = broadcast::channel(4);
        let res = parse_client_event(
            r#"{"type":"typing","data":{"TypingMessageData":{"channel":"community-1"}}}"#,
            String::from("did:plc:me"),
            &tx,
        );

        assert!(res.is_none());
        let notif = rx.try_recv().unwrap();
        assert_eq!(notif.event_type, "typing");
        assert_eq!(notif.data, vec!["did:plc:me", "community-1"]);
    }

    #[test]
    fn ignores_typing_event_without_data() {
        let (tx, mut rx) = broadcast::channel(4);
        let res = parse_client_event(
            r#"{"type":"typing","data":null}"#,
            String::from("did:plc:me"),
            &tx,
        );

        assert!(res.is_none());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ignores_unknown_client_event() {
        let (tx, _) = broadcast::channel(4);
        let res = parse_client_event(
            r#"{"type":"unknown","data":null}"#,
            String::from("did:plc:me"),
            &tx,
        );
        assert!(res.is_none());
    }

    #[test]
    fn serializes_typing_broadcast_for_other_users() {
        let payload = serialize_typing_broadcast(
            EventNotification {
                event_type: String::from("typing"),
                data: vec![String::from("did:plc:other"), String::from("community-1")],
            },
            String::from("did:plc:me"),
        )
        .unwrap();

        let json: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(json["type"], "typing_event");
        assert_eq!(json["data"]["TypingEventData"]["did"], "did:plc:other");
        assert_eq!(json["data"]["TypingEventData"]["channel"], "community-1");
    }

    #[test]
    fn ignores_typing_broadcast_from_same_user_or_invalid_payload() {
        let same_user = serialize_typing_broadcast(
            EventNotification {
                event_type: String::from("typing"),
                data: vec![String::from("did:plc:me"), String::from("community-1")],
            },
            String::from("did:plc:me"),
        );
        assert!(same_user.is_none());

        let invalid = serialize_typing_broadcast(
            EventNotification {
                event_type: String::from("typing"),
                data: vec![String::from("did:plc:other")],
            },
            String::from("did:plc:me"),
        );
        assert!(invalid.is_none());
    }
}
