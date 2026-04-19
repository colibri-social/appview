#![allow(special_module_name)]

extern crate pretty_env_logger;

use crate::lib::db::init_db;
use crate::lib::sentry::init_sentry;
use futures_util::StreamExt;
use rocket::fairing::{AdHoc, Fairing, Info, Kind};
use rocket::http::Header;
use rocket::{Request, Response, get, launch, routes};

mod did_document;
mod lib;
#[allow(dead_code)]
mod models;
mod xrpc;

pub struct CORS;

// Source - https://stackoverflow.com/a/64904947
#[rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> Info {
        Info {
            name: "Adds CORS headers to responses",
            kind: Kind::Response,
        }
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        response.set_header(Header::new("Access-Control-Allow-Origin", "*"));
        response.set_header(Header::new(
            "Access-Control-Allow-Methods",
            "POST, GET, PATCH, OPTIONS",
        ));
        response.set_header(Header::new("Access-Control-Allow-Headers", "*"));
    }
}

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

    rocket::build()
        .mount(
            "/",
            routes![
                landing_ascii,
                did_document::did_document,
                xrpc::com::atproto::identity::resolve_did,
                xrpc::com::atproto::identity::resolve_handle,
                xrpc::com::atproto::identity::resolve_identity,
                xrpc::com::atproto::repo::get_record,
                xrpc::social::colibri::sync::subscribe_events
            ],
        )
        .attach(CORS)
        .attach(init_seaorm())
}
