use crate::lib::at_uri::AtUri;
use crate::lib::colibri::{ColibriMembership, ColibriMessage};
use crate::lib::moderation::{self, MEMBER_NSID};
use crate::lib::notifications::{IndexedNotification, index_message_notifications};
use crate::models::record_data::{self, ActiveModel as RecordDataModel, Entity as RecordData};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use rocket::tokio::sync::{broadcast, mpsc};
use rocket::tokio::{net::TcpStream, sync::mpsc::Sender};
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::io::Error;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::client::IntoClientRequest,
};

const MEMBERSHIP_NSID: &str = "social.colibri.membership";

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
///
/// For `delete` events on `social.colibri.member` and `social.colibri.membership`
/// the local `record_data` row is actually removed, so subsequent authz / read
/// queries reflect that the user is gone. The generic delete path still
/// upserts an empty row (tracked bug — separate follow-up); these two NSIDs
/// are special-cased here because they drive the role-revocation flow.
pub async fn ack_tap_msg(db: &DatabaseConnection, to_tap: &mut Sender<String>, text: String) {
    let msg_data: TapMessage = serde_json::from_str(text.as_str()).unwrap();

    if let Some(safe_record) = msg_data.record {
        let is_membership_or_member_delete = safe_record.action == "delete"
            && matches!(
                safe_record.collection.as_str(),
                MEMBER_NSID | MEMBERSHIP_NSID
            );

        if is_membership_or_member_delete {
            let delete_result = RecordData::delete_many()
                .filter(record_data::Column::Did.eq(&safe_record.did))
                .filter(record_data::Column::Nsid.eq(&safe_record.collection))
                .filter(record_data::Column::Rkey.eq(&safe_record.rkey))
                .exec(db)
                .await;

            if let Err(res) = delete_result {
                log::error!("Unable to delete record from database: {}", res);
                return;
            }
        } else {
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

/// Bundles the cross-task channels the AppView uses for tap-derived events.
/// Stored in Rocket state so handlers can subscribe to the broadcasts.
pub struct CommsBridge {
    /// Outbound channel for sending messages back upstream to tap (e.g. acks).
    /// Kept around for symmetry; not all handlers consume it.
    #[allow(dead_code)]
    pub channel: mpsc::Sender<String>,
    /// Fan-out of every record received from tap. Each WS subscriber holds
    /// its own `Receiver` and decides what to do per-record.
    pub broadcast: broadcast::Sender<TapMessageRecord>,
    /// Fan-out of notifications indexed inside the tap pipeline. Each WS
    /// subscriber filters by recipient DID.
    pub notifications: broadcast::Sender<IndexedNotification>,
}

/// Runs the tap-connection event loop:
///
/// 1. Forwards outbound text from `rx_outbound` upstream over the WS socket.
/// 2. Reads inbound records from the WS socket; for `social.colibri.message`
///    records that aren't deletes, indexes notifications centrally (so each
///    record produces notification rows exactly once regardless of how many
///    subscribers are connected).
/// 3. Broadcasts every inbound record on `tx_inbound` for the
///    `subscribeEvents` WS handler to consume.
/// 4. Acknowledges the message back to tap.
///
/// This is the only place the AppView consumes tap data; subscriber WS
/// connections read from `tx_inbound` and `notifications_tx` exclusively.
pub async fn run_connection(
    db: DatabaseConnection,
    mut socket: TapStream,
    mut rx_outbound: mpsc::Receiver<String>,
    tx_inbound: broadcast::Sender<TapMessageRecord>,
    mut to_tap: mpsc::Sender<String>,
    notifications_tx: broadcast::Sender<IndexedNotification>,
) {
    loop {
        rocket::tokio::select! {
            // Message from an S2C client -> send to Remote Server
            Some(msg) = rx_outbound.recv() => {
                let _ = socket.send(TungMessage::Text(msg.clone().into())).await;
            }
            // Message from Remote Server -> broadcast to all S2C clients
            msg = socket.next() => {
                if let Some(Ok(TungMessage::Text(text))) = msg {
                    let Ok(_) = serde_json::from_str::<Value>(&text) else {
                        // Invalid json, ignore
                        return;
                    };

                    let tap_msg = serde_json::from_str::<TapMessage>(&text).unwrap();

                    if tap_msg.message_type == "record"
                        && tap_msg.record.is_some()
                        && !tap_msg.record.as_ref().unwrap().live
                    {
                        // Event isn't live, skip but stay connected
                        return;
                    }

                    let record = tap_msg.record.unwrap();

                    // Centralized notification indexing for newly authored
                    // Colibri messages. Persists rows + broadcasts to live
                    // WS subscribers, so each notification is written
                    // exactly once regardless of subscriber count.
                    if record.collection == "social.colibri.message"
                        && record.action != "delete"
                        && let Some(payload) = record.record.clone()
                        && let Ok(message) = serde_json::from_value::<ColibriMessage>(payload)
                    {
                        let message_uri = format!(
                            "at://{}/{}/{}",
                            record.did, record.collection, record.rkey
                        );
                        match index_message_notifications(&db, &record.did, &message_uri, &message)
                            .await
                        {
                            Ok(rows) => {
                                for row in rows {
                                    let _ = notifications_tx.send(row);
                                }
                            }
                            Err(e) => {
                                log::error!("notification indexing failed for {message_uri}: {e}");
                            }
                        }
                    }

                    // Self-leave detection: when a user deletes their own
                    // `social.colibri.membership`, drop their community-side
                    // `social.colibri.member` record. Runs before `ack_tap_msg`
                    // so the cached membership row (which holds the community
                    // URI) is still present to read.
                    if record.collection == MEMBERSHIP_NSID
                        && record.action == "delete"
                        && let Err(e) = revoke_member_for_leave(&db, &record).await
                    {
                        log::error!(
                            "member revoke on leave failed for {}/{}: {}",
                            record.did,
                            record.rkey,
                            e
                        );
                    }

                    let _ = tx_inbound.send(record);
                    ack_tap_msg(&db, &mut to_tap, text.to_string()).await;
                }
            }
        }
    }
}

/// Looks up the cached `social.colibri.membership` row for the deleted record,
/// resolves the community DID from its payload, and revokes the community-side
/// `social.colibri.member` record. Returns `Ok(())` when the cached record is
/// missing or malformed (best-effort — nothing to revoke).
async fn revoke_member_for_leave(
    db: &DatabaseConnection,
    record: &TapMessageRecord,
) -> Result<(), DbErr> {
    let old = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&record.did))
        .filter(record_data::Column::Nsid.eq(MEMBERSHIP_NSID))
        .filter(record_data::Column::Rkey.eq(&record.rkey))
        .one(db)
        .await?;
    let Some(row) = old else {
        return Ok(());
    };
    let Ok(membership) = serde_json::from_value::<ColibriMembership>(row.data) else {
        return Ok(());
    };
    let Some(community) = AtUri::parse(&membership.community) else {
        return Ok(());
    };

    moderation::revoke_community_member(db, &community.authority, &record.did).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;
    use rocket::tokio::sync::mpsc;

    #[tokio::test]
    async fn sends_ack_for_non_record_messages() {
        let db = mock_db();
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
