use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use mediasoup::prelude::*;
use rocket::tokio::sync::broadcast::error::RecvError;
use rocket::{State, get, serde::json::Json, tokio};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use sea_orm::DatabaseConnection;

use crate::lib::at_uri::AtUri;
use crate::lib::colibri::ColibriChannel;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::{channel_authz, community_authz, community_write, service_auth};
use crate::sfu::{ChannelSfu, RoomEvent, Sfu};
use crate::xrpc::social::colibri::sync::subscribe_events_handler::{
    AUTH_SUBPROTOCOL, ChannelWithProtocol, SubprotocolAuth,
};

use super::messages::{ClientMessage, ServerMessage, transport_options};

type WsSink = futures_util::stream::SplitSink<DuplexStream, WsMessage>;

const LXM: &str = "social.colibri.voice.signal";

fn ice_servers() -> serde_json::Value {
    std::env::var("SFU_ICE_SERVERS")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .and_then(|v| serde_json::from_str(&v).ok())
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()))
}

async fn authorize_channel_access(
    db: &DatabaseConnection,
    channel_uri: &str,
    did: &str,
) -> Result<(), ErrorResponse> {
    let Some(parsed) = AtUri::parse(channel_uri) else {
        return Ok(());
    };

    let chan_json = match community_write::read_cached(
        db,
        &parsed.authority,
        "social.colibri.channel",
        &parsed.rkey,
    )
    .await
    {
        Ok(Some(json)) => json,
        Ok(None) => return Ok(()),
        Err(e) => {
            log::warn!("voice authz channel read failed for {channel_uri}: {e}; allowing");
            return Ok(());
        }
    };

    let Ok(channel) = serde_json::from_value::<ColibriChannel>(chan_json) else {
        return Ok(());
    };

    let restricted = channel.owner_only == Some(true)
        || !channel.allowed_roles.is_empty()
        || !channel.allowed_members.is_empty();
    if !restricted || did == parsed.authority {
        return Ok(());
    }

    let community_uri = format!("at://{}/social.colibri.community/self", parsed.authority);
    match community_authz::load_actor_authz(db, &community_uri, did).await {
        Ok(authz) if !channel_authz::can_post(&channel, &authz, did) => Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("Forbidden"),
                message: String::from("You are not permitted to join this voice channel."),
            }),
        }),
        Ok(_) => Ok(()),
        Err(e) => {
            log::warn!("voice authz lookup failed for {did}: {e}; allowing");
            Ok(())
        }
    }
}

async fn send(ws_sink: &mut WsSink, msg: &ServerMessage) -> bool {
    let Ok(text) = serde_json::to_string(msg) else {
        return true;
    };
    ws_sink.send(WsMessage::Text(text)).await.is_ok()
}

async fn handle_client_message(
    text: &str,
    ws_sink: &mut WsSink,
    did: &str,
    channel: &ChannelSfu,
    producer_transport: &WebRtcTransport,
    consumer_transport: &WebRtcTransport,
    client_rtp_capabilities: &mut Option<RtpCapabilities>,
    producers: &mut Vec<Producer>,
    consumers: &mut HashMap<ConsumerId, Consumer>,
    my_producer_ids: &mut Vec<ProducerId>,
) {
    let message = match serde_json::from_str::<ClientMessage>(text) {
        Ok(message) => message,
        Err(e) => {
            send(
                ws_sink,
                &ServerMessage::Error {
                    message: format!("invalid message: {e}"),
                },
            )
            .await;
            return;
        }
    };

    match message {
        ClientMessage::Init { rtp_capabilities } => {
            client_rtp_capabilities.replace(rtp_capabilities);
        }
        ClientMessage::ConnectProducerTransport { dtls_parameters } => {
            match producer_transport
                .connect(WebRtcTransportRemoteParameters { dtls_parameters })
                .await
            {
                Ok(_) => {
                    send(ws_sink, &ServerMessage::ConnectedProducerTransport).await;
                }
                Err(e) => {
                    send(
                        ws_sink,
                        &ServerMessage::Error {
                            message: format!("connect producer transport: {e}"),
                        },
                    )
                    .await;
                }
            }
        }
        ClientMessage::Produce {
            kind,
            rtp_parameters,
            source,
        } => match producer_transport
            .produce(ProducerOptions::new(kind, rtp_parameters))
            .await
        {
            Ok(producer) => {
                let id = producer.id();
                if kind == MediaKind::Audio {
                    channel.observe_audio(id).await;
                }
                channel.add_producer(id, did.to_string(), kind, source);
                my_producer_ids.push(id);
                producers.push(producer);
                send(ws_sink, &ServerMessage::Produced { id }).await;
            }
            Err(e) => {
                send(
                    ws_sink,
                    &ServerMessage::Error {
                        message: format!("produce: {e}"),
                    },
                )
                .await;
            }
        },
        ClientMessage::ConnectConsumerTransport { dtls_parameters } => {
            match consumer_transport
                .connect(WebRtcTransportRemoteParameters { dtls_parameters })
                .await
            {
                Ok(_) => {
                    send(ws_sink, &ServerMessage::ConnectedConsumerTransport).await;
                }
                Err(e) => {
                    send(
                        ws_sink,
                        &ServerMessage::Error {
                            message: format!("connect consumer transport: {e}"),
                        },
                    )
                    .await;
                }
            }
        }
        ClientMessage::Consume { producer_id } => {
            let Some(rtp_capabilities) = client_rtp_capabilities.clone() else {
                send(
                    ws_sink,
                    &ServerMessage::Error {
                        message: "send Init with rtpCapabilities before consuming".to_string(),
                    },
                )
                .await;
                return;
            };

            let mut options = ConsumerOptions::new(producer_id, rtp_capabilities);
            options.paused = true;

            match consumer_transport.consume(options).await {
                Ok(consumer) => {
                    let id = consumer.id();
                    let kind = consumer.kind();
                    let rtp_parameters = consumer.rtp_parameters().clone();
                    consumers.insert(id, consumer);
                    send(
                        ws_sink,
                        &ServerMessage::Consumed {
                            id,
                            producer_id,
                            kind,
                            rtp_parameters,
                        },
                    )
                    .await;
                }
                Err(e) => {
                    send(
                        ws_sink,
                        &ServerMessage::Error {
                            message: format!("consume: {e}"),
                        },
                    )
                    .await;
                }
            }
        }
        ClientMessage::ConsumerResume { id } => {
            if let Some(consumer) = consumers.get(&id).cloned()
                && let Err(e) = consumer.resume().await
            {
                log::warn!("failed to resume consumer {id}: {e}");
            }
        }
        ClientMessage::CloseProducer { producer_id } => {
            if let Some(index) = producers.iter().position(|p| p.id() == producer_id) {
                let _ = producers.remove(index);
                my_producer_ids.retain(|id| *id != producer_id);
                channel.remove_producer(&producer_id);
            }
        }
    }
}

async fn forward_room_event(
    ws_sink: &mut WsSink,
    event: RoomEvent,
    my_producer_ids: &[ProducerId],
) -> bool {
    let message = match event {
        RoomEvent::ProducerAdded {
            did,
            producer_id,
            kind,
            source,
        } => {
            if my_producer_ids.contains(&producer_id) {
                return true;
            }
            ServerMessage::ProducerAdded {
                did,
                producer_id,
                kind,
                source,
            }
        }
        RoomEvent::ProducerRemoved { did, producer_id } => {
            if my_producer_ids.contains(&producer_id) {
                return true;
            }
            ServerMessage::ProducerRemoved { did, producer_id }
        }
        RoomEvent::ActiveSpeakers { dids } => ServerMessage::ActiveSpeakers { dids },
        RoomEvent::Silence => ServerMessage::ActiveSpeakers { dids: Vec::new() },
    };
    send(ws_sink, &message).await
}

#[allow(clippy::too_many_arguments)]
async fn run_voice_loop(
    io: DuplexStream,
    did: String,
    channel_uri: String,
    sfu: Arc<Sfu>,
    channel: Arc<ChannelSfu>,
    producer_transport: WebRtcTransport,
    consumer_transport: WebRtcTransport,
    mut events_rx: rocket::tokio::sync::broadcast::Receiver<RoomEvent>,
) {
    let (mut ws_sink, mut ws_source) = io.split();

    let init = ServerMessage::Init {
        router_rtp_capabilities: channel.rtp_capabilities(),
        producer_transport_options: transport_options(&producer_transport),
        consumer_transport_options: transport_options(&consumer_transport),
        ice_servers: ice_servers(),
    };
    if !send(&mut ws_sink, &init).await {
        return;
    }

    for (producer_id, info) in channel.snapshot_producers() {
        send(
            &mut ws_sink,
            &ServerMessage::ProducerAdded {
                did: info.did,
                producer_id,
                kind: info.kind,
                source: info.source,
            },
        )
        .await;
    }

    let mut client_rtp_capabilities: Option<RtpCapabilities> = None;
    let mut producers: Vec<Producer> = Vec::new();
    let mut consumers: HashMap<ConsumerId, Consumer> = HashMap::new();
    let mut my_producer_ids: Vec<ProducerId> = Vec::new();

    loop {
        let connected = tokio::select! {
            msg = ws_source.next() => match msg {
                Some(Ok(WsMessage::Text(text))) => {
                    handle_client_message(
                        &text,
                        &mut ws_sink,
                        &did,
                        &channel,
                        &producer_transport,
                        &consumer_transport,
                        &mut client_rtp_capabilities,
                        &mut producers,
                        &mut consumers,
                        &mut my_producer_ids,
                    )
                    .await;
                    true
                }
                Some(Ok(WsMessage::Close(_))) | None => false,
                Some(Ok(_)) => true,
                Some(Err(_)) => false,
            },
            event = events_rx.recv() => match event {
                Ok(event) => forward_room_event(&mut ws_sink, event, &my_producer_ids).await,
                Err(RecvError::Lagged(_)) => true,
                Err(RecvError::Closed) => false,
            },
        };

        if !connected {
            break;
        }
    }

    for producer_id in &my_producer_ids {
        channel.remove_producer(producer_id);
    }

    drop(producers);
    drop(consumers);
    drop(events_rx);
    drop(channel);
    sfu.cleanup_channel_if_empty(&channel_uri).await;

    log::info!("Voice signaling closed for {did} in {channel_uri}");
}

#[get("/xrpc/social.colibri.voice.signal?<channel>&<auth>")]
pub async fn signal(
    channel: String,
    auth: Option<&str>,
    subprotocol_auth: SubprotocolAuth,
    ws: WebSocket,
    sfu: &State<Arc<Sfu>>,
    db: &State<DatabaseConnection>,
) -> Result<ChannelWithProtocol, ErrorResponse> {
    let used_subprotocol = subprotocol_auth.token().is_some();
    let token = subprotocol_auth
        .token()
        .map(str::to_owned)
        .or_else(|| auth.map(str::to_owned))
        .unwrap_or_default();

    let did = service_auth::verify_service_auth(&token, LXM)
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: e.to_string(),
            }),
        })?;

    authorize_channel_access(db.inner(), &channel, &did).await?;

    let channel_sfu = sfu
        .get_or_create_channel(&channel)
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("SfuError"),
                message: e,
            }),
        })?;

    let producer_transport =
        channel_sfu
            .create_webrtc_transport()
            .await
            .map_err(|e| ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("SfuError"),
                    message: e,
                }),
            })?;
    let consumer_transport =
        channel_sfu
            .create_webrtc_transport()
            .await
            .map_err(|e| ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from("SfuError"),
                    message: e,
                }),
            })?;

    log::info!("User connected to {LXM}: {did} in {channel}");

    let events_rx = channel_sfu.subscribe();
    let sfu = sfu.inner().clone();

    let ws_channel = ws.channel(move |io| {
        Box::pin(async move {
            run_voice_loop(
                io,
                did,
                channel,
                sfu,
                channel_sfu,
                producer_transport,
                consumer_transport,
                events_rx,
            )
            .await;
            Ok(())
        })
    });

    Ok(ChannelWithProtocol::new(
        ws_channel,
        used_subprotocol.then_some(AUTH_SUBPROTOCOL),
    ))
}
