use crate::lib::at_uri::AtUri;
use crate::lib::channel_authz;
use crate::lib::colibri::{
    ColibriChannel, ColibriMembership, ColibriMessage, ColibriModeration,
    ColibriModerationSubject,
};
use crate::lib::community_authz;
use crate::lib::community_record::fetch_community_record;
use crate::lib::community_write;
use crate::lib::event_scope::{CommunityResolver, ScopedEvent, SharedScopedEvent};
use crate::lib::events::{
    CommunityCreationProgressEvent, MuteEvent, MuteEventData, SeenEvent, SeenEventData,
};
use crate::lib::map_tap_event::map_tap_event;
use crate::lib::moderation::{self, ACTION_BLOCKED_JOIN, MODERATION_NSID};
use crate::lib::notifications::{IndexedNotification, index_message_notifications};
use crate::lib::time::current_iso8601_utc;
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
/// For `delete` events the local `record_data` row is removed for any NSID,
/// keeping the cache consistent with the source repo.
pub async fn ack_tap_msg(
    db: &DatabaseConnection,
    to_tap: &mut Sender<String>,
    text: String,
    save_in_db: bool,
) {
    let msg_data: TapMessage = serde_json::from_str(text.as_str()).unwrap();

    if let Some(safe_record) = msg_data.record
        && save_in_db
    {
        if safe_record.action == "delete" {
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
                indexed_at: ActiveValue::Set(current_iso8601_utc()),
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
    } else {
        log::debug!("Acknowledged tap event with ID {}", msg_data.id);
    }
}

/// Bundles the cross-task channels the AppView uses for tap-derived events.
/// Stored in Rocket state so handlers can subscribe to the broadcasts.
pub struct CommsBridge {
    /// Outbound channel for sending messages back upstream to tap (e.g. acks).
    /// Kept around for symmetry; not all handlers consume it.
    #[allow(dead_code)]
    pub channel: mpsc::Sender<String>,
    /// Fan-out of pre-mapped, scope-tagged server events derived from tap
    /// records. Each record is mapped and enriched exactly once in
    /// `run_connection`; every WS subscriber holds its own `Receiver` and
    /// forwards only the events whose [`EventScope`] matches its user.
    ///
    /// [`EventScope`]: crate::lib::event_scope::EventScope
    pub broadcast: broadcast::Sender<SharedScopedEvent>,
    /// Fan-out of notifications indexed inside the tap pipeline. Each WS
    /// subscriber filters by recipient DID.
    pub notifications: broadcast::Sender<IndexedNotification>,
    /// Fan-out of `application_event`s emitted directly by REST handlers
    /// (`approveMembership`, `kickUser`/`kick`, `dismissApplication`,
    /// `undismissApplication`) rather than derived from a single tap record.
    /// Bypasses `map_tap_event` entirely — the handler already has full
    /// context (and, for `approveMembership`, has just performed the write),
    /// so there's no need to round-trip through the firehose to know what
    /// happened. Every WS subscriber forwards these as-is; clients scope by
    /// `community`.
    pub applications: broadcast::Sender<crate::lib::events::ColibriServerEvent>,
    /// Fan-out of `seen_event`s — per-user read-state changes pushed to the
    /// originating user's other connected clients (each subscriber filters by
    /// `recipient_did`). `channel_read` events are emitted from the tap loop
    /// when a `social.colibri.channel.read` record is indexed; `message_seen`
    /// events are emitted by the `updateSeenForMessage` handler.
    pub seen: broadcast::Sender<SeenEvent>,
    /// Fan-out of `mute_event`s — per-user mute/unmute changes pushed to the
    /// originating user's other connected clients (each subscriber filters by
    /// `recipient_did`). Emitted from the tap loop when a
    /// `social.colibri.actor.mute` record is created or deleted.
    pub mute: broadcast::Sender<MuteEvent>,
    /// Fan-out of `community_creation_progress` events — per-user progress
    /// hints pushed to the creating user's own connections (each subscriber
    /// filters by `recipient_did`). Emitted by `community.create` while
    /// bootstrapping a BYO community on a (possibly slow) external PDS.
    pub progress: broadcast::Sender<CommunityCreationProgressEvent>,
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
    tx_inbound: broadcast::Sender<SharedScopedEvent>,
    mut to_tap: mpsc::Sender<String>,
    notifications_tx: broadcast::Sender<IndexedNotification>,
    seen_tx: broadcast::Sender<SeenEvent>,
    mute_tx: broadcast::Sender<MuteEvent>,
) {
    // Caches channel/message -> community DID across the connection's lifetime
    // so per-event community resolution rarely touches the database.
    let resolver = CommunityResolver::new();

    loop {
        rocket::tokio::select! {
            // Message from an S2C client -> send to Remote Server
            Some(msg) = rx_outbound.recv() => {
                let _ = socket.send(TungMessage::Text(msg.clone().into())).await;
            }
            // Message from Remote Server -> broadcast to all S2C clients
            msg = socket.next() => {
                if let Some(Ok(TungMessage::Text(text))) = msg {
                    let tap_msg = match serde_json::from_str::<TapMessage>(&text) {
                        Ok(m) => m,
                        Err(e) => {
                            log::warn!("Received unparseable event from tap: {e}; payload={text}");
                            continue;
                        }
                    };

                    // if tap_msg.message_type == "record"
                    //     && tap_msg.record.is_some()
                    //     && !tap_msg.record.as_ref().unwrap().live
                    // {
                    //     log::warn!("Received old event from tap ({})", tap_msg.id);
                    //     // Event isn't live, skip but stay connected
                    //     ack_tap_msg(&db, &mut to_tap, text.to_string(), false).await;
                    //     continue;
                    // }

                    if tap_msg.record.is_none() {
                        // Event does not carry a record
                        ack_tap_msg(&db, &mut to_tap, text.to_string(), false).await;
                        continue;
                    }

                    let record = tap_msg.record.unwrap();

                    log::debug!("Processing Tap event, ID {}", tap_msg.id);

                    // Channel post restrictions: reject (don't index, don't
                    // notify, don't broadcast) messages from authors not
                    // permitted to post in the target channel. Fails open on
                    // any inconclusive lookup (channel not yet cached, DB
                    // error, malformed JSON) — only a successfully loaded
                    // channel + authz pair that conclusively disallows the
                    // author causes a reject.
                    if record.collection == "social.colibri.message"
                        && record.action != "delete"
                        && let Some(payload) = record.record.as_ref()
                        && let Ok(message) = serde_json::from_value::<ColibriMessage>(payload.clone())
                        && let Some(community_did) =
                            resolver.community_for_channel(&db, &message.channel).await
                        && let Ok(Some(chan_json)) = community_write::read_cached(
                            &db,
                            &community_did,
                            "social.colibri.channel",
                            &message.channel,
                        )
                        .await
                        && let Ok(channel) = serde_json::from_value::<ColibriChannel>(chan_json)
                    {
                        let community_uri =
                            format!("at://{community_did}/social.colibri.community/self");
                        match community_authz::load_actor_authz(
                            &db,
                            &community_uri,
                            &record.did,
                        )
                        .await
                        {
                            Ok(authz) if !channel_authz::can_post(&channel, &authz, &record.did) => {
                                log::info!(
                                    "rejecting message {}/{} into restricted channel {}: author not permitted",
                                    record.did,
                                    record.rkey,
                                    message.channel
                                );
                                ack_tap_msg(&db, &mut to_tap, text.to_string(), false).await;
                                continue;
                            }
                            Ok(_) => {}
                            Err(e) => log::warn!(
                                "authz lookup failed for {}: {e}; allowing message through",
                                record.did
                            ),
                        }
                    }

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
                                    // Background Web Push (closed-app delivery)
                                    // runs off-thread so its network I/O never
                                    // stalls the tap loop. It applies its own
                                    // DND + mute filtering. The live WS
                                    // broadcast below is the foreground path.
                                    let push_db = db.clone();
                                    let push_row = row.clone();
                                    rocket::tokio::spawn(async move {
                                        crate::lib::push_send::deliver(push_db, push_row).await;
                                    });

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

                    // Auto-join on membership-create. For open communities the
                    // AppView writes the matching `social.colibri.member` record
                    // on the community's PDS. Closed communities sit pending
                    // until a moderator hits `approveMembership`. Banned users
                    // get a `blockedJoin` moderation entry for the audit trail
                    // and no member record. Legacy communities (rkey != "self"
                    // on the community record) are not auto-joinable — we have
                    // no community-side credentials for them.
                    if record.collection == MEMBERSHIP_NSID
                        && record.action != "delete"
                        && let Err(e) = process_membership_create(&db, &record).await
                    {
                        log::error!(
                            "membership-create processing failed for {}/{}: {}",
                            record.did,
                            record.rkey,
                            e
                        );
                    }

                    // Cross-device read-state sync: when a user's read cursor
                    // advances (a `social.colibri.channel.read` record is
                    // indexed), tell their other connected clients to clear that
                    // channel's white "unread messages" dot.
                    if record.collection == "social.colibri.channel.read"
                        && record.action != "delete"
                        && let Some(payload) = record.record.as_ref()
                        && let Some(channel) =
                            payload.get("channel").and_then(|c| c.as_str())
                    {
                        let _ = seen_tx.send(SeenEvent {
                            recipient_did: record.did.clone(),
                            data: SeenEventData {
                                event: String::from("channel_read"),
                                channel_uri: channel.to_string(),
                                message_uri: None,
                                cleared: None,
                            },
                        });
                    }

                    // Cross-device mute sync: when a user mutes or unmutes a
                    // channel/community (a `social.colibri.actor.mute` record is
                    // created or deleted), tell their other connected clients so
                    // their mute set stays current without a reload. On delete
                    // the record body is gone, so read the `subject` from the
                    // cached row before `ack_tap_msg` removes it.
                    if record.collection == "social.colibri.actor.mute" {
                        let subject = match record.action.as_str() {
                            "delete" => mute_subject_from_cache(&db, &record).await,
                            _ => record
                                .record
                                .as_ref()
                                .and_then(|p| p.get("subject"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        };

                        if let Some(subject) = subject {
                            let event = if record.action == "delete" {
                                "unmuted"
                            } else {
                                "muted"
                            };
                            let _ = mute_tx.send(MuteEvent {
                                recipient_did: record.did.clone(),
                                data: MuteEventData {
                                    event: String::from(event),
                                    subject,
                                },
                            });
                        }
                    }

                    // Map the record into scope-tagged events and fan them out
                    // exactly once — every connected subscriber then only
                    // filters by scope rather than re-mapping/enriching. This
                    // runs BEFORE `ack_tap_msg`, which deletes the cached row
                    // for delete actions that the mapper still needs to read
                    // (member/reaction deletes, message community resolution).
                    match map_tap_event(&record, &db, &resolver).await {
                        Ok(events) => {
                            for (event, scope) in events {
                                let scoped = std::sync::Arc::new(ScopedEvent {
                                    scope,
                                    payload: event.serialize(),
                                });
                                let _ = tx_inbound.send(scoped);
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            // `Facet` is an expected, intentionally-unhandled
                            // collection; don't spam the log for it.
                            if msg != "Facet" {
                                log::error!("Unable to map tap record {:?}: {}", record, msg);
                            }
                        }
                    }

                    ack_tap_msg(&db, &mut to_tap, text.to_string(), true).await;
                }
            }
        }
    }
}

/// Processes a `social.colibri.membership` create event. Parses the payload,
/// resolves the community record (cache → PDS fallback), refuses legacy
/// communities, records a `blockedJoin` audit entry for banned users, and
/// auto-writes a `social.colibri.member` record on the community's PDS for
/// open communities. Closed communities are a no-op — moderators advance them
/// via the `approveMembership` endpoint.
async fn process_membership_create(
    db: &DatabaseConnection,
    record: &TapMessageRecord,
) -> Result<(), DbErr> {
    let payload = match record.record.as_ref() {
        Some(v) => v,
        None => {
            log::warn!(
                "membership-create event from {}/{} carried no payload; skipping",
                record.did,
                record.rkey
            );
            return Ok(());
        }
    };
    let membership = match serde_json::from_value::<ColibriMembership>(payload.clone()) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "membership-create payload from {}/{} failed to parse: {e}",
                record.did,
                record.rkey
            );
            return Ok(());
        }
    };

    let Some(community_uri) = AtUri::parse(&membership.community) else {
        log::warn!(
            "membership from {}/{} has malformed community URI {:?}; skipping",
            record.did,
            record.rkey,
            membership.community
        );
        return Ok(());
    };
    let community_did = community_uri.authority.clone();
    let community_rkey = community_uri.rkey.clone();

    // Legacy communities live as one of many records on a user repo with a
    // TID rkey; AppView holds no credentials for those PDSes, so it can't
    // write the community-side member record. By design — these communities
    // are read-only as of the Variant A cutover.
    if community_rkey != "self" {
        log::info!(
            "ignoring membership from {} to legacy community {}/{} (rkey != \"self\")",
            record.did,
            community_did,
            community_rkey
        );
        return Ok(());
    }

    let community = match fetch_community_record(db, &community_did, &community_rkey).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            log::warn!(
                "membership from {} references unknown community {}/{}; skipping",
                record.did,
                community_did,
                community_rkey
            );
            return Ok(());
        }
        Err(e) => {
            log::error!(
                "community fetch failed for membership {}/{}: {e}",
                record.did,
                record.rkey
            );
            return Ok(());
        }
    };

    if moderation::is_user_banned(db, &community_did, &record.did).await? {
        log::info!(
            "refusing membership-create for banned user {} in {}; writing blockedJoin audit",
            record.did,
            community_did
        );
        let audit = ColibriModeration {
            record_type: Some(MODERATION_NSID.to_string()),
            action: ACTION_BLOCKED_JOIN.to_string(),
            subject: ColibriModerationSubject {
                did: Some(record.did.clone()),
                uri: None,
            },
            reason: None,
            created_by: community_did.clone(),
            created_at: current_iso8601_utc(),
        };
        if let Err(e) = moderation::write_moderation_record(db, &community_uri, &audit).await {
            log::error!(
                "blockedJoin audit write failed for {} in {}: {e}",
                record.did,
                community_did
            );
        }
        return Ok(());
    }

    if community.requires_approval_to_join {
        log::debug!(
            "membership from {} is pending approval for closed community {}",
            record.did,
            community_did
        );
        return Ok(());
    }

    let membership_uri = format!("at://{}/{}/{}", record.did, record.collection, record.rkey);
    match moderation::write_member_record(
        db,
        &community_did,
        &record.did,
        vec![],
        Some(membership_uri),
    )
    .await
    {
        Ok(Some(_)) => {
            log::info!(
                "auto-admitted {} to open community {}",
                record.did,
                community_did
            );
        }
        Ok(None) => {
            log::debug!(
                "membership-create from {} for {} hit existing member record (idempotent skip)",
                record.did,
                community_did
            );
        }
        Err(e) => {
            log::error!(
                "auto-admit write_member_record failed for {} in {}: {e}",
                record.did,
                community_did
            );
        }
    }

    Ok(())
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

/// Reads the `subject` of a just-deleted `social.colibri.actor.mute` record from
/// the local cache. The delete event carries no body, so the cached row (still
/// present until `ack_tap_msg` removes it) is the only source of the subject.
/// Returns `None` when the row is missing or malformed.
async fn mute_subject_from_cache(
    db: &DatabaseConnection,
    record: &TapMessageRecord,
) -> Option<String> {
    let row = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(&record.did))
        .filter(record_data::Column::Nsid.eq("social.colibri.actor.mute"))
        .filter(record_data::Column::Rkey.eq(&record.rkey))
        .one(db)
        .await
        .ok()??;
    row.data
        .get("subject")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
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
            true,
        )
        .await;

        let ack = rx.recv().await.unwrap();
        let ack_json: serde_json::Value = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack_json["type"], "ack");
        assert_eq!(ack_json["id"], 1);
    }
}
