use crate::lib::{
    events::{ColibriClientEvent, ColibriServerEvent},
    responses::{ErrorBody, ErrorResponse},
    service_auth,
    tap::{self, register_did},
};
use crate::models::record_data::{self, ActiveModel as RecordDataModel, Entity as RecordData};
use ::serde::Serialize;
use futures_util::{SinkExt, StreamExt};
use rocket::{State, get, serde::json::Json, tokio};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, sea_query};
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
pub struct DidStruct {
    pub dids: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct TapMessageRecord {
    live: bool,
    did: String,
    #[allow(dead_code)]
    rev: String,
    collection: String,
    rkey: String,
    #[allow(dead_code)]
    action: String,
    record: Value,
    #[allow(dead_code)]
    cid: String,
}

#[derive(Deserialize, Debug)]
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

// async fn map_tap_event(event_message: TapMessage) -> ColibriEvent {}

/// Returns false if the client has disconnected.
async fn forward_to_client(
    ws_sink: &mut WsSink,
    tap_sink: &mut TapSink,
    text: String,
    db: &DatabaseConnection,
) -> bool {
    let Ok(_) = serde_json::from_str::<Value>(&text) else {
        return true; // Not JSON, skip but stay connected
    };

    let tap_msg = serde_json::from_str::<TapMessage>(&text).unwrap();

    if tap_msg.message_type.eq("record")
        && tap_msg.record.is_some()
        && tap_msg.record.as_ref().unwrap().live == false
    {
        // Event isn't live, acknowledge and skip, but stay connected
        ack_tap_msg(db, tap_sink, text).await;
        return true;
    }

    // let mapped_tap_event = map_tap_event(tap_msg);

    if ws_sink.send(WsMessage::Text(text.clone())).await.is_err() {
        return false;
    }

    ack_tap_msg(db, tap_sink, text).await;
    true
}

/// Acknowledges a message from Tap and saves the data in the database.
async fn ack_tap_msg(db: &DatabaseConnection, tap_sink: &mut TapSink, text: String) {
    let msg_data: TapMessage = serde_json::from_str(text.as_str()).unwrap();

    if msg_data.record.is_some() {
        let safe_record = msg_data.record.unwrap();
        let json_data: String = serde_json::to_string(&safe_record.record).unwrap();

        dbg!(format!(
            "at://{}/{}/{}",
            safe_record.did, safe_record.collection, safe_record.rkey
        ));

        let insert_result = RecordData::insert(RecordDataModel {
            data: ActiveValue::Set(json_data),
            did: ActiveValue::Set(safe_record.did),
            nsid: ActiveValue::Set(safe_record.collection),
            rkey: ActiveValue::Set(safe_record.rkey),
            ..Default::default()
        })
        .on_conflict(
            sea_query::OnConflict::columns([
                record_data::Column::Did,
                record_data::Column::Nsid,
                record_data::Column::Rkey,
            ])
            .update_column(record_data::Column::Data)
            .to_owned(),
        )
        .exec(db)
        .await;

        if insert_result.is_err() {
            log::error!(
                "Unable to save record in database: {}",
                insert_result.unwrap_err().to_string()
            );

            return;
        }
    }

    let ack = TapAck {
        tap_type: String::from("ack"),
        id: msg_data.id.clone(),
    };

    let serialized_ack = serde_json::to_string(&ack).unwrap();

    dbg!(&serialized_ack);

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
    db: &DatabaseConnection,
) -> bool {
    match msg {
        Some(Ok(TungMessage::Text(text))) => {
            forward_to_client(ws_sink, tap_sink, text.to_string(), db).await
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
    tap_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<rocket::tokio::net::TcpStream>,
    >,
    did: String,
    db: DatabaseConnection,
) {
    let (mut ws_sink, mut ws_source) = io.split();
    let (mut tap_sink, mut tap_source) = tap_stream.split();

    register_did(&did).await;

    loop {
        let connected = tokio::select! {
            msg = tap_source.next() => handle_tap_message(&mut ws_sink, &mut tap_sink, msg, &db).await,
            msg = ws_source.next() => handle_client_message(&mut ws_sink, msg).await,
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
    db: &State<DatabaseConnection>,
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

    let cloned = db.inner().clone();

    Ok(ws.channel(move |io| {
        Box::pin(async move {
            run_event_loop(io, tap_stream, did, cloned).await;
            Ok(())
        })
    }))
}
