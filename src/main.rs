#![allow(special_module_name)]

extern crate pretty_env_logger;

use crate::lib::db::init_db;
use crate::lib::sentry::init_sentry;
use rocket::fairing::AdHoc;
use rocket::http::Method;
use rocket::{get, launch, routes};
use rocket_cors::{AllowedOrigins, CorsOptions};

mod lib;
#[allow(dead_code)]
mod models;
mod well_known;
mod xrpc;

fn init_seaorm() -> AdHoc {
    AdHoc::try_on_ignite("Initialize SeaORM", |rocket| async {
        match init_db().await {
            Ok(db) => Ok(rocket.manage(db)),
            Err(error) => {
                log::error!("failed to initialize SeaORM: {error}");
                Err(rocket)
            }
        }
    })
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
fn rocket() -> _ {
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
                xrpc::social::colibri::actor::get_data,
                xrpc::social::colibri::sync::subscribe_events,
            ],
        )
        .attach(cors.to_cors().unwrap())
        .attach(init_seaorm())
}
