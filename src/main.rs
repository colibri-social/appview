#![allow(special_module_name)]

extern crate pretty_env_logger;

use crate::lib::colibri::ColibriMessage;
use crate::lib::db::init_db;
use crate::lib::notifications::{IndexedNotification, index_message_notifications};
use crate::lib::sentry::init_sentry;
use crate::lib::tap::{self, TapMessage, TapMessageRecord, TapStream, ack_tap_msg};
use futures::{SinkExt, StreamExt};
use migration::{Migrator, MigratorTrait};
use rocket::fairing::AdHoc;
use rocket::http::Method;
use rocket::tokio::sync::{broadcast, mpsc};
use rocket::{get, launch, routes, tokio};
use rocket_cors::{AllowedOrigins, CorsOptions};
use sea_orm::DatabaseConnection;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as TungMessage;

mod lib;
#[allow(dead_code)]
mod models;
mod well_known;
mod xrpc;

fn init_seaorm(db: DatabaseConnection) -> AdHoc {
    AdHoc::try_on_ignite("Manage SeaORM", |rocket| async { Ok(rocket.manage(db)) })
}

struct CommsBridge {
    #[allow(dead_code)]
    channel: mpsc::Sender<String>,
    broadcast: broadcast::Sender<TapMessageRecord>,
    notifications: broadcast::Sender<IndexedNotification>,
}

#[derive(Clone)]
pub struct EventNotification {
    pub event_type: String,
    pub data: Vec<String>,
}

async fn handle_tap_connection(
    db: DatabaseConnection,
    mut socket: TapStream,
    mut rx_outbound: mpsc::Receiver<String>,
    tx_inbound: broadcast::Sender<TapMessageRecord>,
    mut to_tap: mpsc::Sender<String>,
    notifications_tx: broadcast::Sender<IndexedNotification>,
) {
    loop {
        tokio::select! {
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
                    // Colibri messages. Persists rows + broadcasts to live WS
                    // subscribers, so each notification is written exactly
                    // once regardless of how many clients are connected.
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

                    let _ = tx_inbound.send(record);
                    ack_tap_msg(&db, &mut to_tap, text.to_string()).await;
                }
            }
        }
    }
}

#[get("/")]
fn landing_ascii() -> String {
    String::from(
        r#"            _ _ _          _                  _       _
           | (_) |        (_)                (_)     | |
   ___ ___ | |_| |__  _ __ _   ___  ___   ___ _  __ _| |
  / __/ _ \| | | '_ \| '__| | / __|/ _ \ / __| |/ _` | |
 | (_| (_) | | | |_) | |  | |_\__ \ (_) | (__| | (_| | |
  \___\___/|_|_|_.__/|_|  |_(_)___/\___/ \___|_|\__,_|_|


This is an AT Protocol Application View (AppView) for the "colibri.social" application.

Most API routes are under /xrpc/

        Docs: https://colibri.social/docs
        Code: https://github.com/colibri-social/appview
    Protocol: https://atproto.com
"#,
    )
}

#[launch]
async fn rocket() -> _ {
    pretty_env_logger::init();
    let _ = dotenvy::dotenv();
    let _potential_guard = init_sentry().ok();

    let cors = CorsOptions::default()
        .allowed_origins(AllowedOrigins::all())
        .allowed_methods(
            vec![Method::Get, Method::Post, Method::Options]
                .into_iter()
                .map(From::from)
                .collect(),
        )
        .allow_credentials(true);

    let safe_cors = cors.to_cors().unwrap();

    let (to_tap, from_tap_channel) = mpsc::channel::<String>(128);
    let (from_tap_broadcast, _) = broadcast::channel::<TapMessageRecord>(128);

    let c2c_broadcast_channel = broadcast::channel::<EventNotification>(128);

    let (notification_broadcast, _) = broadcast::channel::<IndexedNotification>(128);

    let tap_bridge = CommsBridge {
        channel: to_tap.clone(),
        broadcast: from_tap_broadcast.clone(),
        notifications: notification_broadcast.clone(),
    };

    let db = match init_db().await {
        Ok(db) => db,
        Err(error) => {
            log::error!("failed to initialize SeaORM: {error}");
            panic!();
        }
    };

    Migrator::up(&db, None)
        .await
        .expect("Unable to apply SeaORM migrations.");

    let tap_stream: TapStream = tap::connect_to_tap()
        .await
        .expect("Failed to connect to tap");

    tokio::spawn(handle_tap_connection(
        db.clone(),
        tap_stream,
        from_tap_channel,
        from_tap_broadcast,
        to_tap,
        notification_broadcast,
    ));

    rocket::build()
        .mount(
            "/",
            routes![
                landing_ascii,
                well_known::did_json,
                xrpc::com::atproto::identity::resolve_did,
                xrpc::com::atproto::identity::resolve_handle,
                xrpc::com::atproto::identity::resolve_identity,
                xrpc::com::atproto::sync::get_record,
                xrpc::com::atproto::sync::list_records,
                xrpc::social::colibri::actor::get_data,
                xrpc::social::colibri::actor::list_communities,
                xrpc::social::colibri::actor::set_state,
                xrpc::social::colibri::channel::get_read_cursor,
                xrpc::social::colibri::channel::list_messages,
                xrpc::social::colibri::channel::list_reactions,
                xrpc::social::colibri::community::block_message,
                xrpc::social::colibri::community::block_user,
                xrpc::social::colibri::community::create_invitation,
                xrpc::social::colibri::community::delete_invitation,
                xrpc::social::colibri::community::get_invitation,
                xrpc::social::colibri::community::list_blocked_users,
                xrpc::social::colibri::community::list_categories,
                xrpc::social::colibri::community::list_channels,
                xrpc::social::colibri::community::list_invitations,
                xrpc::social::colibri::community::list_members,
                xrpc::social::colibri::community::unblock_user,
                xrpc::social::colibri::notification::get_unread_count,
                xrpc::social::colibri::notification::list_notifications,
                xrpc::social::colibri::notification::update_seen,
                xrpc::social::colibri::sync::subscribe_events,
            ],
        )
        .mount("/", rocket_cors::catch_all_options_routes())
        .attach(safe_cors.clone())
        .attach(init_seaorm(db))
        .manage(tap_bridge)
        .manage(c2c_broadcast_channel)
        .manage(safe_cors)
}
