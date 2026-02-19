#[macro_use]
extern crate rocket;

mod db;
mod events;
mod jetstream;
mod models;
mod webrtc;
mod ws_handler;

use chrono::{DateTime, Utc};
use rocket::{fairing::AdHoc, http::Status, serde::json::Json, State};
use sqlx::postgres::PgPoolOptions;
use tracing::error;

use models::message::Message;
use webrtc::RoomState;

// Source - https://stackoverflow.com/a/64904947
// Posted by Ibraheem Ahmed, modified by community. See post 'Timeline' for change history
// Retrieved 2026-02-19, License - CC BY-SA 4.0

use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::Header;
use rocket::{Request, Response};

pub struct CORS;

#[rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> Info {
        Info {
            name: "Add CORS headers to responses",
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
        response.set_header(Header::new("Access-Control-Allow-Credentials", "true"));
    }
}

// ── REST endpoints ────────────────────────────────────────────────────────────

/// Retrieve messages for a channel, newest first.
///
/// Query params:
/// - `channel` (required)
/// - `limit`   (optional, default 50, max 100)
/// - `before`  (optional ISO 8601 timestamp for cursor-based pagination)
#[get("/api/messages?<channel>&<limit>&<before>")]
async fn get_messages(
    channel: &str,
    limit: Option<i64>,
    before: Option<&str>,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Vec<Message>>, Status> {
    let limit = limit.unwrap_or(50).clamp(1, 100);
    let before: Option<DateTime<Utc>> = before
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    db::get_messages(pool, channel, limit, before)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_messages error: {e}");
            Status::InternalServerError
        })
}

// ── Rocket launch ─────────────────────────────────────────────────────────────

#[launch]
fn rocket() -> _ {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "colibri_appview=info,rocket=info".into()),
        )
        .init();

    rocket::build()
        // ── Database pool ────────────────────────────────────────────────────
        .attach(AdHoc::try_on_ignite("PostgreSQL Pool", |rocket| async {
            let url = match std::env::var("DATABASE_URL") {
                Ok(u) => u,
                Err(_) => {
                    error!("DATABASE_URL is not set");
                    return Err(rocket);
                }
            };

            match PgPoolOptions::new().max_connections(10).connect(&url).await {
                Ok(pool) => {
                    if let Err(e) = sqlx::migrate!("./migrations").run(&pool).await {
                        error!("Failed to run migrations: {e}");
                        return Err(rocket);
                    }
                    Ok(rocket.manage(pool))
                }
                Err(e) => {
                    error!("Failed to connect to PostgreSQL: {e}");
                    Err(rocket)
                }
            }
        }))
        // ── Shared application state ─────────────────────────────────────────
        .manage(events::create_event_bus(4096))
        .manage(RoomState::new())
        // ── Routes ───────────────────────────────────────────────────────────
        .mount(
            "/",
            routes![get_messages, ws_handler::subscribe, webrtc::signal,],
        )
        // ── Background: Jetstream consumer ───────────────────────────────────
        .attach(AdHoc::on_liftoff("Jetstream Consumer", |rocket| {
            Box::pin(async move {
                let pool = rocket.state::<sqlx::PgPool>().unwrap().clone();
                let bus = rocket.state::<events::EventBus>().unwrap().clone();
                rocket::tokio::spawn(jetstream::run(pool, bus));
            })
        }))
       	.attach(CORS)
}
