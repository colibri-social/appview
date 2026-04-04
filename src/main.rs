#![allow(special_module_name)]

extern crate pretty_env_logger;

use crate::lib::sentry::init_sentry;
use crate::lib::db::init_db;
use rocket::fairing::{AdHoc, Fairing, Info, Kind};
use rocket::http::Header;
use rocket::{Request, Response, get, launch, routes};

mod lib;
#[allow(dead_code)]
mod models;

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

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
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

#[launch]
fn rocket() -> _ {
    pretty_env_logger::init();
    let _ = dotenvy::dotenv();
    let _potential_guard = init_sentry().ok();

    rocket::build()
        .mount("/", routes![index])
        .attach(CORS)
        .attach(init_seaorm())
}
