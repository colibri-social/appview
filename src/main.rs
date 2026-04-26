#![allow(special_module_name)]

extern crate pretty_env_logger;

use crate::lib::db::init_db;
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

struct TapBridge {
    #[allow(dead_code)]
    to_tap: mpsc::Sender<String>,
    from_tap: broadcast::Sender<TapMessageRecord>,
}

async fn handle_tap_connection(
    db: DatabaseConnection,
    mut socket: TapStream,
    mut rx_outbound: mpsc::Receiver<String>,
    tx_inbound: broadcast::Sender<TapMessageRecord>,
    mut to_tap: mpsc::Sender<String>,
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
                        && tap_msg.record.as_ref().unwrap().live == false
                    {
                        // Event isn't live, skip but stay connected
                        return;
                    }

                    let _ = tx_inbound.send(tap_msg.record.unwrap());
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

    let (to_tap, rx_outbound) = mpsc::channel::<String>(128);
    let (tx_inbound, _) = broadcast::channel::<TapMessageRecord>(128);

    let bridge = TapBridge {
        to_tap: to_tap.clone(),
        from_tap: tx_inbound.clone(),
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
        rx_outbound,
        tx_inbound,
        to_tap,
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
                xrpc::social::colibri::sync::subscribe_events,
            ],
        )
        .mount("/", rocket_cors::catch_all_options_routes())
        .attach(safe_cors.clone())
        .attach(init_seaorm(db))
        .manage(bridge)
        .manage(safe_cors)
}
