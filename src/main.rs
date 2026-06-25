#![allow(special_module_name)]

extern crate pretty_env_logger;

use crate::lib::crypto;
use crate::lib::db::init_db;
use crate::lib::notifications::IndexedNotification;
use crate::lib::sentry::init_sentry;
use crate::lib::tap::{self, CommsBridge, TapMessageRecord, TapStream, run_connection};
use migration::{Migrator, MigratorTrait};
use rocket::fairing::AdHoc;
use rocket::http::Method;
use rocket::tokio::sync::{broadcast, mpsc};
use rocket::{get, launch, routes, tokio};
use rocket_cors::{AllowedOrigins, CorsOptions};
use sea_orm::DatabaseConnection;

mod lib;
#[allow(dead_code)]
mod models;
mod well_known;
mod xrpc;

fn init_seaorm(db: DatabaseConnection) -> AdHoc {
    AdHoc::try_on_ignite("Manage SeaORM", |rocket| async { Ok(rocket.manage(db)) })
}

/// Boot-time guard: panics with a descriptive message if `name` is unset or
/// empty. Used to surface missing configuration at startup rather than on the
/// first request that needs it.
fn require_env_var(name: &str) {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => {}
        _ => panic!("{name} must be set (cannot start without it)"),
    }
}

/// Client-to-client broadcast payload — used by the WS subscriber pipeline
/// for events the AppView generates internally (e.g. typing presence) and
/// distributes between connected clients.
#[derive(Clone)]
pub struct EventNotification {
    pub event_type: String,
    pub data: Vec<String>,
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

    // Boot-time invariant checks. Anything the AppView cannot operate
    // without should fail-fast here with a clear message rather than at
    // first-call. Keep this list in sync with the env vars that
    // `community.create` (and downstream PDS-write helpers) consume.
    require_env_var("PDS_LOC");
    require_env_var("APPVIEW_HANDLE_DOMAIN");

    // Load the at-rest credential encryption key before any handler that
    // touches `community_credentials` can run. Fails fast at boot if the env
    // var is missing or malformed.
    let master_key = crypto::load_master_key_from_env()
        .expect("CREDENTIAL_ENCRYPTION_KEY must be a base64-encoded 32-byte key");
    crypto::install_master_key(master_key).expect("master key already installed");

    // Web Push is optional. Surface at boot whether it's configured so a
    // missing keypair (→ no background push) is obvious in the logs rather
    // than a silent no-op per notification.
    if lib::vapid_config::is_configured() {
        log::info!("Web Push enabled (VAPID keypair configured).");
    } else {
        log::warn!(
            "Web Push disabled: set VAPID_PUBLIC_KEY/VAPID_PRIVATE_KEY to enable background push."
        );
    }

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

    let (application_broadcast, _) =
        broadcast::channel::<crate::lib::events::ColibriServerEvent>(128);

    let (seen_broadcast, _) = broadcast::channel::<crate::lib::events::SeenEvent>(128);

    let (mute_broadcast, _) = broadcast::channel::<crate::lib::events::MuteEvent>(128);

    let tap_bridge = CommsBridge {
        channel: to_tap.clone(),
        broadcast: from_tap_broadcast.clone(),
        notifications: notification_broadcast.clone(),
        applications: application_broadcast,
        seen: seen_broadcast.clone(),
        mute: mute_broadcast.clone(),
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
        .expect("Unable to apply SeaORM migrations");

    let tap_stream: TapStream = tap::connect_to_tap()
        .await
        .expect("Failed to connect to tap");

    tokio::spawn(run_connection(
        db.clone(),
        tap_stream,
        from_tap_channel,
        from_tap_broadcast,
        to_tap,
        notification_broadcast,
        seen_broadcast,
        mute_broadcast,
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
                xrpc::com::atproto::sync::get_blob,
                xrpc::com::atproto::sync::get_record,
                xrpc::com::atproto::sync::list_records,
                xrpc::social::colibri::actor::get_data,
                xrpc::social::colibri::actor::list_communities,
                xrpc::social::colibri::actor::list_mutes,
                xrpc::social::colibri::actor::set_state,
                xrpc::social::colibri::channel::create_channel,
                xrpc::social::colibri::channel::delete_channel,
                xrpc::social::colibri::channel::get_read_cursor,
                xrpc::social::colibri::channel::list_messages,
                xrpc::social::colibri::channel::list_reactions,
                xrpc::social::colibri::channel::list_unread_status,
                xrpc::social::colibri::channel::update_channel,
                xrpc::social::colibri::embed::get_metadata,
                xrpc::social::colibri::embed::get_image,
                xrpc::social::colibri::community::approve_membership,
                xrpc::social::colibri::community::ban_user,
                xrpc::social::colibri::community::block_message,
                xrpc::social::colibri::community::create,
                xrpc::social::colibri::community::create_category,
                xrpc::social::colibri::community::create_invitation,
                xrpc::social::colibri::community::delete_category,
                xrpc::social::colibri::community::delete_community,
                xrpc::social::colibri::community::delete_invitation,
                xrpc::social::colibri::community::dismiss_application,
                xrpc::social::colibri::community::get_data,
                xrpc::social::colibri::community::get_invitation,
                xrpc::social::colibri::community::kick,
                xrpc::social::colibri::community::kick_user,
                xrpc::social::colibri::community::list_applications,
                xrpc::social::colibri::community::list_banned_users,
                xrpc::social::colibri::community::list_categories,
                xrpc::social::colibri::community::list_channels,
                xrpc::social::colibri::community::list_invitations,
                xrpc::social::colibri::community::list_members,
                xrpc::social::colibri::community::list_roles,
                xrpc::social::colibri::community::create_role,
                xrpc::social::colibri::community::delete_role,
                xrpc::social::colibri::community::register_credentials,
                xrpc::social::colibri::community::reorder_categories,
                xrpc::social::colibri::community::reorder_channels,
                xrpc::social::colibri::community::set_member_roles,
                xrpc::social::colibri::community::unban_user,
                xrpc::social::colibri::community::undismiss_application,
                xrpc::social::colibri::community::update_category,
                xrpc::social::colibri::community::update_community,
                xrpc::social::colibri::community::update_role,
                xrpc::social::colibri::notification::get_unread_count,
                xrpc::social::colibri::notification::get_unseen,
                xrpc::social::colibri::notification::list_notifications,
                xrpc::social::colibri::notification::register_push,
                xrpc::social::colibri::notification::unregister_push,
                xrpc::social::colibri::notification::update_seen,
                xrpc::social::colibri::notification::update_seen_for_message,
                xrpc::social::colibri::sync::subscribe_events,
            ],
        )
        .mount("/", rocket_cors::catch_all_options_routes())
        .attach(safe_cors.clone())
        .attach(init_seaorm(db))
        .manage(tap_bridge)
        .manage(c2c_broadcast_channel)
        .manage(crate::lib::embed_cache::EmbedCache::default())
        .manage(safe_cors)
}
