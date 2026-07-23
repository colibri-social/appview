use mediasoup::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportOptions {
    pub id: TransportId,
    pub dtls_parameters: DtlsParameters,
    pub ice_candidates: Vec<IceCandidate>,
    pub ice_parameters: IceParameters,
}

pub fn transport_options(transport: &WebRtcTransport) -> TransportOptions {
    TransportOptions {
        id: transport.id(),
        dtls_parameters: transport.dtls_parameters(),
        ice_candidates: transport.ice_candidates().clone(),
        ice_parameters: transport.ice_parameters().clone(),
    }
}

#[derive(Serialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum ServerMessage {
    #[serde(rename_all = "camelCase")]
    Init {
        router_rtp_capabilities: RtpCapabilitiesFinalized,
        producer_transport_options: TransportOptions,
        consumer_transport_options: TransportOptions,
        ice_servers: serde_json::Value,
    },
    ConnectedProducerTransport,
    #[serde(rename_all = "camelCase")]
    Produced {
        id: ProducerId,
    },
    ConnectedConsumerTransport,
    #[serde(rename_all = "camelCase")]
    Consumed {
        id: ConsumerId,
        producer_id: ProducerId,
        kind: MediaKind,
        rtp_parameters: RtpParameters,
    },
    #[serde(rename_all = "camelCase")]
    ProducerAdded {
        did: String,
        producer_id: ProducerId,
        kind: MediaKind,
        source: String,
    },
    #[serde(rename_all = "camelCase")]
    ProducerRemoved {
        did: String,
        producer_id: ProducerId,
    },
    #[serde(rename_all = "camelCase")]
    ActiveSpeakers {
        dids: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    ServerMuted {
        muted: bool,
    },
    #[serde(rename_all = "camelCase")]
    ServerDeafened {
        deafened: bool,
    },
    Kicked,
    Superseded,
    #[serde(rename_all = "camelCase")]
    Error {
        message: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum ClientMessage {
    #[serde(rename_all = "camelCase")]
    Init { rtp_capabilities: RtpCapabilities },
    #[serde(rename_all = "camelCase")]
    ConnectProducerTransport { dtls_parameters: DtlsParameters },
    #[serde(rename_all = "camelCase")]
    Produce {
        kind: MediaKind,
        rtp_parameters: RtpParameters,
        #[serde(default)]
        source: String,
    },
    #[serde(rename_all = "camelCase")]
    ConnectConsumerTransport { dtls_parameters: DtlsParameters },
    #[serde(rename_all = "camelCase")]
    Consume { producer_id: ProducerId },
    #[serde(rename_all = "camelCase")]
    ConsumerResume { id: ConsumerId },
    #[serde(rename_all = "camelCase")]
    CloseProducer { producer_id: ProducerId },
}
