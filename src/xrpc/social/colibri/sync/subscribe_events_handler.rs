use crate::lib::{
    responses::{ErrorBody, ErrorResponse},
    service_auth, tap,
};
use ::serde::Serialize;
use futures_util::{SinkExt, StreamExt};
use rocket::{get, serde::json::Json, tokio};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use serde::Deserialize;
use serde_json::{Number, Value};
use tokio_tungstenite::tungstenite::Message as TungMessage;

type TapSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<rocket::tokio::net::TcpStream>,
    >,
    TungMessage,
>;
type WsSink = futures_util::stream::SplitSink<DuplexStream, WsMessage>;

#[derive(Serialize)]
struct DidStruct {
    dids: Vec<String>,
}

#[derive(Deserialize)]
struct TapMessageRecord {
    live: bool,
}

#[derive(Deserialize)]
struct TapMessage {
    id: Number,
    #[serde(rename = "type")]
    message_type: String,
    record: Option<TapMessageRecord>,
}

#[derive(Serialize)]
struct TapAck {
    #[serde(rename = "type")]
    tap_type: String,
    id: Number,
}

/// Sends a DID to Tap to register for backfilling.
async fn register_did(did: &String) {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let client = reqwest::Client::new();

    let did_struct = DidStruct {
        dids: vec![did.clone()],
    };

    let res = client
        .post(format!("http://{tap_hostname}/repos/add"))
        .basic_auth(String::from("admin"), Some(tap_password))
        .json(&did_struct)
        .send()
        .await;

    if res.is_err() {
        log::error!("Unable to add DID {did}: {}", res.unwrap_err().to_string())
    } else {
        log::info!("Now tracking DID {did}");
    }
}

/// Returns false if the client has disconnected.
async fn forward_to_client(ws_sink: &mut WsSink, tap_sink: &mut TapSink, text: String) -> bool {
    let Ok(_) = serde_json::from_str::<Value>(&text) else {
        return true; // Not JSON, skip but stay connected
    };

    let tap_msg = serde_json::from_str::<TapMessage>(&text).unwrap();

    if tap_msg.message_type.eq("record")
        && tap_msg.record.is_some()
        && tap_msg.record.unwrap().live == false
    {
        return true; // Event isn't live, skip but stay connected
    }

    if ws_sink.send(WsMessage::Text(text.clone())).await.is_err() {
        return false;
    }

    ack_tap_msg(tap_sink, text).await;
    true
}

/// Acknowledges a message from Tap.
async fn ack_tap_msg(tap_sink: &mut TapSink, text: String) {
    dbg!(&text);
    let msg_data: TapMessage = serde_json::from_str(text.as_str()).unwrap();

    let ack = TapAck {
        tap_type: String::from("ack"),
        id: msg_data.id.clone(),
    };

    let serialized_ack = serde_json::to_string(&ack).unwrap();

    let ack_res = tap_sink
        .send(TungMessage::Text(serialized_ack.into()))
        .await;

    if ack_res.is_err() {
        log::error!(
            "Unable to acknowledge event with ID {}: {}",
            msg_data.id,
            ack_res.unwrap_err().to_string()
        )
    }
}

/// Returns false if the tap stream has closed or errored.
async fn handle_tap_message(
    ws_sink: &mut WsSink,
    tap_sink: &mut TapSink,
    msg: Option<Result<TungMessage, tokio_tungstenite::tungstenite::Error>>,
) -> bool {
    match msg {
        Some(Ok(TungMessage::Text(text))) => {
            forward_to_client(ws_sink, tap_sink, text.to_string()).await
        }
        Some(Ok(TungMessage::Close(_))) | None => false,
        Some(Err(e)) => {
            eprintln!("Tap stream error: {e}");
            false
        }
        _ => true, // Ignore ping/pong/binary
    }
}

/// Returns false if the client has closed or errored.
async fn handle_client_message(
    tap_sink: &mut TapSink,
    msg: Option<Result<WsMessage, rocket_ws::result::Error>>,
) -> bool {
    match msg {
        Some(Ok(WsMessage::Text(text))) => {
            if serde_json::from_str::<Value>(&text).is_ok() {
                let _ = tap_sink.send(TungMessage::Text(text.into())).await;
            }
            true
        }
        Some(Ok(WsMessage::Close(_))) | None => false,
        _ => true, // Ignore binary, ping, pong
    }
}

/// Handles the event loop and allows both messages from Tap and the Client to get processed.
async fn run_event_loop(
    io: DuplexStream,
    tap_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<rocket::tokio::net::TcpStream>,
    >,
    did: String,
) {
    let (mut ws_sink, mut ws_source) = io.split();
    let (mut tap_sink, mut tap_source) = tap_stream.split();

    register_did(&did).await;

    loop {
        let connected = tokio::select! {
            msg = tap_source.next() => handle_tap_message(&mut ws_sink, &mut tap_sink, msg).await,
            msg = ws_source.next() => handle_client_message(&mut tap_sink, msg).await,
        };

        if !connected {
            break;
        }
    }
}

#[get("/xrpc/social.colibri.sync.subscribeEvents?<auth>")]
pub async fn subscribe_events(
    auth: &str,
    ws: WebSocket,
) -> Result<rocket_ws::Channel<'static>, ErrorResponse> {
    let did = service_auth::verify_service_auth(auth)
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    let tap_stream = tap::connect_to_tap()
        .await
        .expect("Failed to connect to tap");

    log::info!("User connected to social.colibri.sync.subscribeEvents: {did}");

    Ok(ws.channel(move |io| {
        Box::pin(async move {
            run_event_loop(io, tap_stream, did).await;
            Ok(())
        })
    }))
}
