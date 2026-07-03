//! `social.colibri.sync.subscribeHums` — egress-only Hum stream for peer AppViews.
//!
//! A peer AppView opens this WebSocket to receive the off-protocol Hums this
//! AppView relays in its hub role. The peer authenticates with an inter-service
//! auth JWT carried in the `Sec-WebSocket-Protocol` subprotocol (mirroring
//! `subscribeEvents`). The stream is one-directional: inbound frames are ignored
//! except for the close signal — peers push Hums via `sendHum`, never here.

use crate::lib::events::HumEnvelope;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use crate::lib::tap::CommsBridge;
use crate::xrpc::social::colibri::sync::subscribe_events_handler::{
    AUTH_SUBPROTOCOL, ChannelWithProtocol, SubprotocolAuth,
};
use futures_util::{SinkExt, StreamExt};
use rocket::tokio::sync::broadcast::Receiver;
use rocket::tokio::sync::broadcast::error::RecvError;
use rocket::{State, get, serde::json::Json};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};

async fn run_hum_egress_loop(io: DuplexStream, from_hums: Receiver<HumEnvelope>, peer_did: String) {
    let (mut ws_sink, mut ws_source) = io.split();
    let mut from_hums = from_hums;

    loop {
        let connected = rocket::tokio::select! {
            hum = from_hums.recv() => match hum {
                Ok(envelope) => match serde_json::to_string(&envelope) {
                    Ok(payload) => ws_sink.send(WsMessage::Text(payload)).await.is_ok(),
                    Err(e) => {
                        log::warn!("failed to serialize Hum for {peer_did}: {e}");
                        true
                    }
                },
                Err(RecvError::Lagged(skipped)) => {
                    log::warn!("Hum egress for {peer_did} lagged, skipped {skipped}");
                    true
                }
                Err(RecvError::Closed) => false,
            },
            // Egress-only: the peer sends nothing meaningful. Watch only for the
            // close/disconnect so we can wind the connection down.
            msg = ws_source.next() => !matches!(msg, Some(Ok(WsMessage::Close(_))) | None),
        };

        if !connected {
            break;
        }
    }
}

#[get("/xrpc/social.colibri.sync.subscribeHums?<auth>")]
pub async fn subscribe_hums(
    auth: Option<&str>,
    subprotocol_auth: SubprotocolAuth,
    ws: WebSocket,
    bridge: &State<CommsBridge>,
) -> Result<ChannelWithProtocol, ErrorResponse> {
    let used_subprotocol = subprotocol_auth.token().is_some();
    let token = subprotocol_auth
        .token()
        .map(str::to_owned)
        .or_else(|| auth.map(str::to_owned))
        .unwrap_or_default();

    let peer_did = service_auth::verify_appview_auth(&token, "social.colibri.sync.subscribeHums")
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthRequired"),
                message: e.to_string(),
            }),
        })?;

    log::info!("Peer AppView subscribed to social.colibri.sync.subscribeHums: {peer_did}");

    let from_hums = bridge.hums.subscribe();

    let channel = ws.channel(move |io| {
        Box::pin(async move {
            run_hum_egress_loop(io, from_hums, peer_did).await;
            Ok(())
        })
    });

    Ok(ChannelWithProtocol::new(
        channel,
        used_subprotocol.then_some(AUTH_SUBPROTOCOL),
    ))
}
