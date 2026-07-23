#[cfg(not(windows))]
mod backend {
    use std::collections::HashMap;
    use std::net::IpAddr;
    use std::num::{NonZeroU8, NonZeroU16, NonZeroU32};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use mediasoup::prelude::*;
    use mediasoup::rtp_observer::RtpObserverAddProducerOptions;
    use rocket::tokio::sync::{Notify, RwLock, broadcast};

    use crate::lib::voice_control::{VoiceAction, VoiceControlCommand};

    pub fn status() -> String {
        "enabled (mediasoup)".to_string()
    }

    pub fn media_codecs() -> Vec<RtpCodecCapability> {
        vec![
            RtpCodecCapability::Audio {
                mime_type: MimeTypeAudio::Opus,
                preferred_payload_type: None,
                clock_rate: NonZeroU32::new(48000).unwrap(),
                channels: NonZeroU8::new(2).unwrap(),
                parameters: RtpCodecParametersParameters::from([("useinbandfec", 1_u32.into())]),
                rtcp_feedback: vec![RtcpFeedback::TransportCc],
            },
            RtpCodecCapability::Video {
                mime_type: MimeTypeVideo::Vp9,
                preferred_payload_type: None,
                clock_rate: NonZeroU32::new(90000).unwrap(),
                parameters: RtpCodecParametersParameters::default(),
                rtcp_feedback: vec![
                    RtcpFeedback::Nack,
                    RtcpFeedback::NackPli,
                    RtcpFeedback::CcmFir,
                    RtcpFeedback::GoogRemb,
                    RtcpFeedback::TransportCc,
                ],
            },
        ]
    }

    #[derive(Clone, Debug)]
    pub enum RoomEvent {
        ProducerAdded {
            did: String,
            producer_id: ProducerId,
            kind: MediaKind,
            source: String,
        },
        ProducerRemoved {
            did: String,
            producer_id: ProducerId,
        },
        ActiveSpeakers {
            dids: Vec<String>,
        },
        Silence,
        ForceMute {
            did: String,
            muted: bool,
        },
        ForceDeafen {
            did: String,
            deafened: bool,
        },
        ForceDisconnect {
            did: String,
        },
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct ModState {
        pub muted: bool,
        pub deafened: bool,
    }

    #[derive(Clone, Debug)]
    pub struct ProducerInfo {
        pub did: String,
        pub kind: MediaKind,
        pub source: String,
    }

    fn worker_count() -> usize {
        std::env::var("SFU_WORKER_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            })
    }

    fn webrtc_transport_options() -> WebRtcTransportOptions {
        let listen_ip: IpAddr = std::env::var("SFU_LISTEN_IP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| IpAddr::from([0, 0, 0, 0]));

        let announced_address = std::env::var("SFU_ANNOUNCED_IP")
            .ok()
            .filter(|v| !v.trim().is_empty());

        let min = std::env::var("SFU_RTC_MIN_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok());
        let max = std::env::var("SFU_RTC_MAX_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok());
        let port_range = match (min, max) {
            (Some(min), Some(max)) if min <= max => Some(min..=max),
            _ => None,
        };

        let listen_info = |protocol| ListenInfo {
            protocol,
            ip: listen_ip,
            announced_address: announced_address.clone(),
            expose_internal_ip: false,
            port: None,
            port_range: port_range.clone(),
            flags: None,
            send_buffer_size: None,
            recv_buffer_size: None,
        };

        WebRtcTransportOptions::new(
            WebRtcTransportListenInfos::new(listen_info(Protocol::Udp))
                .insert(listen_info(Protocol::Tcp)),
        )
    }

    pub struct ChannelSfu {
        router: Router,
        audio_observer: AudioLevelObserver,
        events: broadcast::Sender<RoomEvent>,
        producers: Arc<Mutex<HashMap<ProducerId, ProducerInfo>>>,
        moderation: Arc<Mutex<HashMap<String, ModState>>>,
    }

    impl ChannelSfu {
        pub fn rtp_capabilities(&self) -> RtpCapabilitiesFinalized {
            self.router.rtp_capabilities().clone()
        }

        pub async fn create_webrtc_transport(&self) -> Result<WebRtcTransport, String> {
            self.router
                .create_webrtc_transport(webrtc_transport_options())
                .await
                .map_err(|e| format!("create_webrtc_transport: {e}"))
        }

        pub fn subscribe(&self) -> broadcast::Receiver<RoomEvent> {
            self.events.subscribe()
        }

        pub fn snapshot_moderation(&self, did: &str) -> (bool, bool) {
            let state = self
                .moderation
                .lock()
                .unwrap()
                .get(did)
                .copied()
                .unwrap_or_default();
            (state.muted, state.deafened)
        }

        fn set_moderation(&self, did: &str, muted: Option<bool>, deafened: Option<bool>) {
            let mut guard = self.moderation.lock().unwrap();
            let entry = guard.entry(did.to_string()).or_default();
            if let Some(muted) = muted {
                entry.muted = muted;
            }
            if let Some(deafened) = deafened {
                entry.deafened = deafened;
            }
        }

        fn emit(&self, event: RoomEvent) {
            let _ = self.events.send(event);
        }

        pub fn snapshot_producers(&self) -> Vec<(ProducerId, ProducerInfo)> {
            self.producers
                .lock()
                .unwrap()
                .iter()
                .map(|(id, info)| (*id, info.clone()))
                .collect()
        }

        pub fn add_producer(
            &self,
            producer_id: ProducerId,
            did: String,
            kind: MediaKind,
            source: String,
        ) {
            self.producers.lock().unwrap().insert(
                producer_id,
                ProducerInfo {
                    did: did.clone(),
                    kind,
                    source: source.clone(),
                },
            );
            let _ = self.events.send(RoomEvent::ProducerAdded {
                did,
                producer_id,
                kind,
                source,
            });
        }

        pub fn remove_producer(&self, producer_id: &ProducerId) {
            let info = self.producers.lock().unwrap().remove(producer_id);
            if let Some(info) = info {
                let _ = self.events.send(RoomEvent::ProducerRemoved {
                    did: info.did,
                    producer_id: *producer_id,
                });
            }
        }

        pub async fn observe_audio(&self, producer_id: ProducerId) {
            if let Err(e) = self
                .audio_observer
                .add_producer(RtpObserverAddProducerOptions::new(producer_id))
                .await
            {
                log::warn!("audio observer add_producer failed: {e}");
            }
        }
    }

    struct VoiceSession {
        id: u64,
        evict: Arc<Notify>,
    }

    pub struct Sfu {
        #[allow(dead_code)]
        worker_manager: WorkerManager,
        workers: Vec<Worker>,
        next_worker: AtomicUsize,
        channels: RwLock<HashMap<String, Arc<ChannelSfu>>>,
        sessions: RwLock<HashMap<String, Vec<VoiceSession>>>,
        next_session_id: AtomicU64,
    }

    impl Sfu {
        pub async fn new() -> Self {
            let worker_manager = WorkerManager::new();
            let count = worker_count();
            let mut workers = Vec::with_capacity(count);
            for _ in 0..count {
                let worker = worker_manager
                    .create_worker(WorkerSettings::default())
                    .await
                    .expect("failed to create mediasoup worker");
                workers.push(worker);
            }
            log::info!("Voice SFU worker pool up: {count} worker(s)");
            Self {
                worker_manager,
                workers,
                next_worker: AtomicUsize::new(0),
                channels: RwLock::new(HashMap::new()),
                sessions: RwLock::new(HashMap::new()),
                next_session_id: AtomicU64::new(0),
            }
        }

        pub async fn register_session(&self, did: &str) -> (u64, Arc<Notify>) {
            let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
            let evict = Arc::new(Notify::new());
            let mut sessions = self.sessions.write().await;
            let entry = sessions.entry(did.to_string()).or_default();
            for existing in entry.drain(..) {
                existing.evict.notify_one();
            }
            entry.push(VoiceSession {
                id,
                evict: evict.clone(),
            });
            (id, evict)
        }

        pub async fn deregister_session(&self, did: &str, id: u64) {
            let mut sessions = self.sessions.write().await;
            if let Some(entry) = sessions.get_mut(did) {
                entry.retain(|s| s.id != id);
                if entry.is_empty() {
                    sessions.remove(did);
                }
            }
        }

        fn pick_worker(&self) -> &Worker {
            let i = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
            &self.workers[i]
        }

        pub async fn get_or_create_channel(&self, uri: &str) -> Result<Arc<ChannelSfu>, String> {
            if let Some(channel) = self.channels.read().await.get(uri) {
                return Ok(channel.clone());
            }

            let mut channels = self.channels.write().await;
            if let Some(channel) = channels.get(uri) {
                return Ok(channel.clone());
            }

            let worker = self.pick_worker();
            let router = worker
                .create_router(RouterOptions::new(media_codecs()))
                .await
                .map_err(|e| format!("create_router: {e}"))?;
            let mut audio_observer_options = AudioLevelObserverOptions::default();
            audio_observer_options.max_entries = NonZeroU16::new(20).unwrap();
            audio_observer_options.interval = 400;
            let audio_observer = router
                .create_audio_level_observer(audio_observer_options)
                .await
                .map_err(|e| format!("create_audio_level_observer: {e}"))?;

            let (events, _) = broadcast::channel(256);
            let producers: Arc<Mutex<HashMap<ProducerId, ProducerInfo>>> =
                Arc::new(Mutex::new(HashMap::new()));

            {
                let events_tx = events.clone();
                let producers = producers.clone();
                audio_observer
                    .on_volumes(move |volumes| {
                        let map = producers.lock().unwrap();
                        let dids: Vec<String> = volumes
                            .iter()
                            .filter_map(|v| map.get(&v.producer.id()).map(|i| i.did.clone()))
                            .collect();
                        let _ = events_tx.send(RoomEvent::ActiveSpeakers { dids });
                    })
                    .detach();

                let events_tx = events.clone();
                audio_observer
                    .on_silence(move || {
                        let _ = events_tx.send(RoomEvent::Silence);
                    })
                    .detach();
            }

            let channel = Arc::new(ChannelSfu {
                router,
                audio_observer,
                events,
                producers,
                moderation: Arc::new(Mutex::new(HashMap::new())),
            });
            channels.insert(uri.to_string(), channel.clone());
            log::info!("Voice channel router created: {uri}");
            Ok(channel)
        }

        pub async fn get_channel(&self, uri: &str) -> Option<Arc<ChannelSfu>> {
            self.channels.read().await.get(uri).cloned()
        }

        pub async fn apply_voice_control(&self, cmd: VoiceControlCommand) {
            let Some(channel) = self.get_channel(&cmd.channel_uri).await else {
                return;
            };
            match cmd.action {
                VoiceAction::ServerMute(muted) => {
                    channel.set_moderation(&cmd.target_did, Some(muted), None);
                    channel.emit(RoomEvent::ForceMute {
                        did: cmd.target_did,
                        muted,
                    });
                }
                VoiceAction::ServerDeafen(deafened) => {
                    channel.set_moderation(&cmd.target_did, None, Some(deafened));
                    channel.emit(RoomEvent::ForceDeafen {
                        did: cmd.target_did,
                        deafened,
                    });
                }
                VoiceAction::Disconnect => {
                    channel.emit(RoomEvent::ForceDisconnect {
                        did: cmd.target_did,
                    });
                }
            }
        }

        pub async fn cleanup_channel_if_empty(&self, uri: &str) {
            let mut channels = self.channels.write().await;
            if let Some(channel) = channels.get(uri)
                && channel.events.receiver_count() == 0
            {
                channels.remove(uri);
                log::info!("Voice channel router closed (empty): {uri}");
            }
        }
    }
}

#[cfg(windows)]
mod backend {
    pub fn status() -> String {
        "unavailable on native Windows (build/run voice via WSL2/Docker)".to_string()
    }
}

pub use backend::status;

#[cfg(not(windows))]
pub use backend::{ChannelSfu, RoomEvent, Sfu};
