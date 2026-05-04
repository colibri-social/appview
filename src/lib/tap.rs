use crate::models::record_data::{self, ActiveModel as RecordDataModel, Entity as RecordData};
use base64::Engine;
use rocket::tokio::{net::TcpStream, sync::mpsc::Sender};
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, sea_query};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::io::Error;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::client::IntoClientRequest,
};

use crate::xrpc::social::colibri::sync::subscribe_events_handler::DidStruct;

pub type TapStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize, Debug, Clone)]
pub struct TapMessageRecord {
    pub live: bool,
    pub did: String,
    #[allow(dead_code)]
    pub rev: String,
    pub collection: String,
    pub rkey: String,
    #[allow(dead_code)]
    pub action: String,
    pub record: Option<Value>,
    #[allow(dead_code)]
    pub cid: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TapMessage {
    pub id: Number,
    #[serde(rename = "type")]
    pub message_type: String,
    pub record: Option<TapMessageRecord>,
}

#[derive(Serialize)]
pub struct TapAck {
    #[serde(rename = "type")]
    pub tap_type: String,
    pub id: Number,
}

/// Opens a new connection to the tap instance.
pub async fn connect_to_tap() -> Result<TapStream, Error> {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let credentials =
        base64::engine::general_purpose::STANDARD.encode(format!("admin:{tap_password}"));

    let mut request = format!("ws://{tap_hostname}/channel")
        .into_client_request()
        .expect("Failed to build request");

    request.headers_mut().insert(
        "Authorization",
        format!("Basic {credentials}").parse().unwrap(),
    );

    let (ws_stream, _response) = connect_async(request).await.expect("Failed to connect");

    log::info!("Connected to Tap!");

    Ok(ws_stream)
}

/// Sends DIDs to Tap to register for backfilling.
pub async fn register_dids(dids: Vec<String>) {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let client = reqwest::Client::new();

    let did_struct = DidStruct { dids };

    let res = client
        .post(format!("http://{tap_hostname}/repos/add"))
        .basic_auth(String::from("admin"), Some(tap_password))
        .json(&did_struct)
        .send()
        .await;

    if let Err(res_err) = res {
        log::error!("Unable to add DIDs: {}", res_err);
    } else {
        log::info!("Now tracking DIDs: {}", &did_struct.dids.join(", "));
    }
}

/// Acknowledges a message from Tap and saves the data in the database.
pub async fn ack_tap_msg(db: &DatabaseConnection, to_tap: &mut Sender<String>, text: String) {
    let msg_data: TapMessage = serde_json::from_str(text.as_str()).unwrap();

    if let Some(safe_record) = msg_data.record {
        let json_data: Value = serde_json::to_value(&safe_record.record).unwrap();

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

        if let Err(res) = insert_result {
            log::error!("Unable to save record in database: {}", res);

            return;
        }
    }

    let ack = TapAck {
        tap_type: String::from("ack"),
        id: msg_data.id.clone(),
    };

    let serialized_ack = serde_json::to_string(&ack).unwrap();

    let ack_res = to_tap.send(serialized_ack).await;

    if let Err(res_err) = ack_res {
        log::error!(
            "Unable to acknowledge event with ID {}: {}",
            msg_data.id,
            res_err
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use rocket::tokio::sync::mpsc;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn sends_ack_for_non_record_messages() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let (mut tx, mut rx) = mpsc::channel::<String>(1);

        ack_tap_msg(
            &db,
            &mut tx,
            String::from(r#"{"id":1,"type":"heartbeat","record":null}"#),
        )
        .await;

        let ack = rx.recv().await.unwrap();
        let ack_json: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack_json["type"], "ack");
        assert_eq!(ack_json["id"], 1);
    }
}
