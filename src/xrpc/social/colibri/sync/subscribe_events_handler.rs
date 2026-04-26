use crate::TapBridge;
use crate::lib::map_tap_event::map_tap_event;
use crate::lib::tap::TapMessageRecord;
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
use rocket::tokio::sync::broadcast::Receiver;
use rocket::tokio::sync::broadcast::error::RecvError;
use rocket::{State, get, serde::json::Json, tokio};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use sea_orm::DatabaseConnection;

type WsSink = futures_util::stream::SplitSink<DuplexStream, WsMessage>;

#[derive(Serialize)]
pub struct DidStruct {
    pub dids: Vec<String>,
}

/// Returns false if the client has disconnected.
async fn forward_to_client(ws_sink: &mut WsSink, record: TapMessageRecord, did: String) -> bool {
    if record.did != did {
        return true;
    };

    let mapped_tap_event = map_tap_event(&record);

    if mapped_tap_event.is_err() {
        let err = mapped_tap_event.unwrap_err().to_string();

        if err != "Facet" {
            log::error!("Unable to handle tap message {:?}: {}", record, err);
        }
        return true;
    }

    let safe_event = mapped_tap_event.unwrap();

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
) -> bool {
    match record {
        Ok(msg) => forward_to_client(ws_sink, msg, did).await,
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
) -> bool {
    match msg {
        Some(Ok(WsMessage::Close(_))) | None => false,
        Some(Ok(WsMessage::Text(text))) => {
            let user_message = serde_json::from_str::<ColibriClientEvent>(&text);

            if user_message.is_ok() {
                match user_message.unwrap().event_type.as_str() {
                    "heartbeat" => {
                        let ack_res = ColibriServerEvent {
                            event_type: String::from("ack"),
                            data: None,
                        };

                        let serialized_ack_res = serde_json::to_string(&ack_res).unwrap();

                        let _ = ws_sink.send(WsMessage::Text(serialized_ack_res)).await;
                    }
                    _ => {}
                }
            }

            true
        }
        _ => {
            return true;
        }
    }
}

/// Handles the event loop and allows both messages from Tap and the Client to get processed.
async fn run_event_loop(
    io: DuplexStream,
    from_tap: &mut Receiver<TapMessageRecord>,
    did: String,
    db: DatabaseConnection,
) {
    let (mut ws_sink, mut ws_source) = io.split();

    save_state(&db, did.clone(), String::from("online")).await;
    register_dids(vec![did.clone()]).await;

    loop {
        let connected = tokio::select! {
            msg = from_tap.recv() => handle_tap_message(&mut ws_sink, msg, did.clone()).await,
            msg = ws_source.next() => handle_client_message(&mut ws_sink, msg).await,
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
    bridge: &State<TapBridge>,
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

    let mut from_tap = bridge.from_tap.subscribe();

    Ok(ws.channel(move |io| {
        Box::pin(async move {
            run_event_loop(io, &mut from_tap, did, cloned_db).await;
            Ok(())
        })
    }))
}
