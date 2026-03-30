use crate::lib::sentry::init_sentry;

#[allow(special_module_name)]
mod lib;

extern crate pretty_env_logger;

#[macro_use]
extern crate rocket;
extern crate log;

use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::Header;
use rocket::{Request, Response};

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

#[launch]
fn rocket() -> _ {
    pretty_env_logger::init();
    let _ = dotenvy::dotenv();
    let _potential_guard = init_sentry().ok();

    rocket::build().mount("/", routes![index]).attach(CORS)
}
