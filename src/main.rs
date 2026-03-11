#[macro_use]
extern crate rocket;

mod atproto;
mod backfill;
mod db;
mod emoji;
mod events;
mod jetstream;
mod models;
mod webrtc;
mod ws_handler;

use chrono::{DateTime, Utc};
use rocket::{
    fairing::AdHoc,
    http::Status,
    request::{self, FromRequest},
    response::{self, Responder, Response as RocketResponse},
    serde::json::Json,
    State,
};
use sqlx::postgres::PgPoolOptions;
use tracing::error;

use models::{
    author::AuthorProfile,
    community::{
        Channel, CommunitiesResponse, Community, CommunityMember, CreateInviteRequest, InviteCodeInfo,
    },
    message::MessageResponse,
    reaction::ReactionSummary,
};
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
    }
}

// ── API key guard ─────────────────────────────────────────────────────────────

/// Request guard that validates the `Authorization: Bearer <key>` header
/// against the `INVITE_API_KEY` environment variable.
pub struct ApiKey;

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for ApiKey {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        let expected = match std::env::var("INVITE_API_KEY") {
            Ok(k) if !k.is_empty() => k,
            _ => {
                error!("INVITE_API_KEY env var is not set; rejecting request");
                return rocket::request::Outcome::Error((Status::InternalServerError, ()));
            }
        };
        let provided = req
            .headers()
            .get_one("Authorization")
            .and_then(|v| v.strip_prefix("Bearer "));
        match provided {
            Some(key) if key == expected => rocket::request::Outcome::Success(ApiKey),
            _ => rocket::request::Outcome::Error((Status::Unauthorized, ())),
        }
    }
}

// ── REST endpoints ────────────────────────────────────────────────────────────

/// Catch-all handler for CORS preflight requests.
#[options("/<_..>")]
fn preflight() -> Status {
    Status::NoContent
}

/// Retrieve messages for a channel, newest first, with author profile included.
///
/// Query params:
/// - `channel` (required)
/// - `limit`   (optional, default 50, max 100)
/// - `before`  (optional ISO 8601 timestamp for cursor-based pagination)
#[get("/api/messages?<channel>&<limit>&<before>&<all>")]
async fn get_messages(
    channel: &str,
    limit: Option<i64>,
    before: Option<&str>,
    all: Option<&str>,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Vec<MessageResponse>>, Status> {
    let limit = limit.unwrap_or(50).clamp(1, 100);
    let before: Option<DateTime<Utc>> = before
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    db::get_messages(pool, channel, limit, before, all)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_messages error: {e}");
            Status::InternalServerError
        })
}

/// Retrieve the cached profile for a specific author DID.///
/// Query param: `did` (required)
#[get("/api/authors?<did>")]
async fn get_author(
    did: &str,
    pool: &State<sqlx::PgPool>,
    http: &State<reqwest::Client>,
) -> Result<Json<AuthorProfile>, Status> {
    // Serve from cache; fetch from ATProto if not yet cached.
    match db::get_author_profile(pool, did).await {
        Ok(Some(profile)) => return Ok(Json(profile)),
        Ok(None) => {}
        Err(e) => {
            error!("get_author DB error: {e}");
            return Err(Status::InternalServerError);
        }
    }

    match jetstream::ensure_profile_cached(pool, http, did).await {
        Some(profile) => Ok(Json(profile)),
        None => Err(Status::NotFound),
    }
}

/// Retrieve a single message by author DID and record key, fully enriched.
///
/// Query params: `author` (required), `rkey` (required)
#[get("/api/message?<author>&<rkey>")]
async fn get_message(
    author: &str,
    rkey: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<MessageResponse>, Status> {
    match db::get_message_by_author_and_rkey(pool, author, rkey).await {
        Ok(Some(msg)) => Ok(Json(msg)),
        Ok(None) => Err(Status::NotFound),
        Err(e) => {
            error!("get_message error: {e}");
            Err(Status::InternalServerError)
        }
    }
}

/// Retrieve reactions for a specific message, grouped by emoji.
///
/// Query param: `message` (required) — the record key of the target message
#[get("/api/reactions?<message>")]
async fn get_reactions_for_message(
    message: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Vec<ReactionSummary>>, Status> {
    db::get_reactions_for_message(pool, message)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_reactions_for_message error: {e}");
            Status::InternalServerError
        })
}

/// Retrieve all reactions in a channel, keyed by target message rkey.
///
/// Query param: `channel` (required)
#[get("/api/reactions/channel?<channel>")]
async fn get_reactions_for_channel(
    channel: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<std::collections::HashMap<String, Vec<ReactionSummary>>>, Status> {
    db::get_reactions_for_channel(pool, channel)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_reactions_for_channel error: {e}");
            Status::InternalServerError
        })
}

/// Retrieve all channels for a community.
///
/// Query param: `community` (required) — the full AT-URI of the community
#[get("/api/channels?<community>")]
async fn get_channels(
    community: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Vec<Channel>>, Status> {
    db::get_channels_for_community(pool, community)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_channels error: {e}");
            Status::InternalServerError
        })
}

/// Retrieve channels and categories combined into a sidebar-ready structure.
///
/// Query param: `community` (required) — the full AT-URI of the community
///
/// Response shape:
/// ```json
/// {
///   "categories": [{ "rkey", "name", "emoji", "parent_rkey", "channels": [...] }],
///   "uncategorized": [...]
/// }
/// ```
#[get("/api/sidebar?<community>")]
async fn get_sidebar(
    community: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<crate::models::community::SidebarResponse>, Status> {
    db::get_sidebar_for_community(pool, community)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_sidebar error: {e}");
            Status::InternalServerError
        })
}

/// Look up a community by AT-URI or rkey.
///
/// Query param: `community` (required) — AT-URI (`at://...`) or bare rkey
#[get("/api/community?<community>")]
async fn get_community(
    community: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Community>, Status> {
    match db::get_community(pool, community).await {
        Ok(Some(c)) => Ok(Json(c)),
        Ok(None) => Err(Status::NotFound),
        Err(e) => {
            error!("get_community error: {e}");
            Err(Status::InternalServerError)
        }
    }
}

/// Retrieve all members of a community, enriched with cached profile data.
///
/// Query param: `community` (required) — the full AT-URI of the community
#[get("/api/members?<community>")]
async fn get_members(
    community: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Vec<CommunityMember>>, Status> {
    db::get_members_for_community(pool, community)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_members error: {e}");
            Status::InternalServerError
        })
}

/// Retrieve all communities for a user (both owned and joined) in one roundtrip.
///
/// Query param: `did` (required)
#[get("/api/communities?<did>")]
async fn get_communities(
    did: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<CommunitiesResponse>, Status> {
    db::get_communities_for_user(pool, did)
        .await
        .map(Json)
        .map_err(|e| {
            error!("get_communities error: {e}");
            Status::InternalServerError
        })
}

/// Look up an invite code and return community info.
///
/// Path param: `code`
#[get("/api/invite/<code>")]
async fn get_invite(
    code: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<serde_json::Value>, Status> {
    match db::get_invite_code(pool, code).await {
        Ok(Some((invite, community))) => Ok(Json(serde_json::json!({
            "code": invite.code,
            "community_uri": invite.community_uri,
            "created_by_did": invite.created_by_did,
            "max_uses": invite.max_uses,
            "use_count": invite.use_count,
            "active": invite.active,
            "community": community,
        }))),
        Ok(None) => Err(Status::NotFound),
        Err(e) => {
            error!("get_invite error: {e}");
            Err(Status::InternalServerError)
        }
    }
}

/// Create a new invite code for a community.
///
/// Body: `{ "community_uri": "...", "owner_did": "...", "max_uses": null }`
#[post("/api/invite", data = "<body>")]
async fn create_invite(
    _key: ApiKey,
    body: Json<CreateInviteRequest>,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<serde_json::Value>, Status> {
    match db::create_invite_code(pool, &body.community_uri, &body.owner_did, body.max_uses).await {
        Ok(Some(code)) => Ok(Json(serde_json::json!({ "code": code }))),
        Ok(None) => Err(Status::Forbidden),
        Err(e) => {
            error!("create_invite error: {e}");
            Err(Status::InternalServerError)
        }
    }
}

/// Revoke (deactivate) an invite code.
///
/// Path param: `code`, query param: `owner_did` (required)
#[delete("/api/invite/<code>?<owner_did>")]
async fn revoke_invite(
    _key: ApiKey,
    code: &str,
    owner_did: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Status, Status> {
    match db::revoke_invite_code(pool, code, owner_did).await {
        Ok(true) => Ok(Status::NoContent),
        Ok(false) => Err(Status::Forbidden),
        Err(e) => {
            error!("revoke_invite error: {e}");
            Err(Status::InternalServerError)
        }
    }
}

/// Block a message by author DID and rkey. The message is hidden from all
/// future requests and all connected clients are notified via a MessageDeleted
/// event, matching the behaviour of a normal message deletion.
///
/// Query params: `author_did`, `rkey`
#[post("/api/message/block?<author_did>&<rkey>")]
async fn block_message(
    _key: ApiKey,
    author_did: &str,
    rkey: &str,
    pool: &State<sqlx::PgPool>,
    bus: &State<events::EventBus>,
) -> Status {
    match db::block_message(pool, author_did, rkey).await {
        Ok(Some(msg)) => {
            let _ = bus.send(events::AppEvent::MessageDeleted {
                id: msg.id,
                rkey: msg.rkey,
                author_did: msg.author_did,
                channel: msg.channel,
            });
            Status::NoContent
        }
        Ok(None) => Status::NotFound,
        Err(e) => {
            error!("block_message error: {e}");
            Status::InternalServerError
        }
    }
}


struct RangeHeader(Option<String>);

/// Set the presence state for a user.
/// Valid states: `online`, `away`, `dnd`.
/// This updates both the current state and the preferred state (persisted across reconnects).
///
/// Query params: `did`, `state`
#[post("/api/user/state?<did>&<state>")]
async fn set_user_state(
    _key: ApiKey,
    did: &str,
    state: &str,
    pool: &State<sqlx::PgPool>,
    bus: &State<events::EventBus>,
) -> Status {
    match state {
        "online" | "away" | "dnd" => {}
        _ => return Status::UnprocessableEntity,
    }
    match db::set_user_state(pool, did, state, true).await {
        Ok(updated) => {
            let _ = bus.send(events::AppEvent::UserStatusChanged {
                did: did.to_string(),
                status: updated.status.unwrap_or_default(),
                emoji: updated.emoji,
                state: updated.state,
                display_name: updated.display_name,
                avatar_url: updated.avatar_url,
            });
            Status::NoContent
        }
        Err(e) => {
            error!("set_user_state error: {e}");
            Status::InternalServerError
        }
    }
}



#[rocket::async_trait]
impl<'r> FromRequest<'r> for RangeHeader {
    type Error = ();
    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let val = req.headers().get_one("Range").map(|s| s.to_string());
        request::Outcome::Success(RangeHeader(val))
    }
}

/// Resolve and proxy a blob by DID and CID from the author's PDS.
/// Forwards the `Range` header and relays `Content-Type`, `Content-Range`,
/// `Content-Length`, and `Accept-Ranges` back to the client.
///
/// Query params: `did` (required), `cid` (required — the `$link` value from the blob ref)
#[get("/api/blob?<did>&<cid>")]
async fn get_blob<'r>(
    did: &'r str,
    cid: &'r str,
    http: &'r State<reqwest::Client>,
    range: RangeHeader,
) -> Result<BlobResponse, Status> {
    let pds_url = match atproto::resolve_pds(http, did).await {
        Ok(u) => u,
        Err(e) => {
            error!(did, cid, "Failed to resolve PDS for blob: {e}");
            return Err(Status::NotFound);
        }
    };

    let url = format!(
        "{}/xrpc/com.atproto.sync.getBlob?did={}&cid={}",
        pds_url.trim_end_matches('/'),
        did,
        cid
    );

    let mut req = http.get(&url);
    if let Some(range_val) = range.0 {
        req = req.header("Range", range_val);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            error!(did, cid, "Failed to fetch blob from PDS: {e}");
            return Err(Status::new(502));
        }
    };

    let status = Status::from_code(resp.status().as_u16()).unwrap_or(Status::InternalServerError);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let content_range = resp
        .headers()
        .get("content-range")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let content_length = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let accept_ranges = resp
        .headers()
        .get("accept-ranges")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let body = match resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            error!(did, cid, "Failed to read blob body from PDS: {e}");
            return Err(Status::new(502));
        }
    };

    Ok(BlobResponse {
        status,
        content_type,
        content_range,
        content_length,
        accept_ranges,
        body,
    })
}

struct BlobResponse {
    status: Status,
    content_type: String,
    content_range: Option<String>,
    content_length: Option<String>,
    accept_ranges: Option<String>,
    body: Vec<u8>,
}

impl<'r> Responder<'r, 'static> for BlobResponse {
    fn respond_to(self, _req: &'r Request<'_>) -> response::Result<'static> {
        let mut builder = RocketResponse::build();
        builder.status(self.status);
        builder.raw_header("Content-Type", self.content_type);
        if let Some(cr) = self.content_range {
            builder.raw_header("Content-Range", cr);
        }
        if let Some(cl) = self.content_length {
            builder.raw_header("Content-Length", cl);
        }
        builder.raw_header(
            "Accept-Ranges",
            self.accept_ranges.unwrap_or_else(|| "bytes".to_string()),
        );
        let len = self.body.len();
        builder.sized_body(len, std::io::Cursor::new(self.body));
        builder.ok()
    }
}

/// Mark an invite code as used (increments use_count, enforces max_uses).
/// Returns 204 on success, 404 if the code doesn't exist, 410 if the code is
/// inactive or exhausted.
#[post("/api/invite/<code>/use")]
async fn use_invite(
    _key: ApiKey,
    code: &str,
    pool: &State<sqlx::PgPool>,
) -> Status {
    match db::use_invite_code(pool, code).await {
        Ok(true) => Status::NoContent,
        Ok(false) => Status::Gone,
        Err(e) => {
            error!("use_invite error: {e}");
            Status::InternalServerError
        }
    }
}


///
/// Query param: `community` (required) — the full AT-URI of the community
#[get("/api/invites?<community>")]
async fn list_invites(
    _key: ApiKey,
    community: &str,
    pool: &State<sqlx::PgPool>,
) -> Result<Json<Vec<InviteCodeInfo>>, Status> {
    db::get_invite_codes_for_community(pool, community)
        .await
        .map(Json)
        .map_err(|e| {
            error!("list_invites error: {e}");
            Status::InternalServerError
        })
}

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
        .manage(reqwest::Client::new())
        // ── Routes ───────────────────────────────────────────────────────────
        .mount(
            "/",
            routes![
                get_messages,
                get_message,
                get_author,
                get_reactions_for_message,
                get_reactions_for_channel,
                get_communities,
                get_community,
                get_channels,
                get_sidebar,
                get_members,
                get_invite,
                create_invite,
                revoke_invite,
                use_invite,
                block_message,
                set_user_state,
                list_invites,
                get_blob,
                ws_handler::subscribe,
                webrtc::signal,
                preflight
            ],
        )
        // ── Background: Jetstream consumer ───────────────────────────────────
        .attach(AdHoc::on_liftoff("Jetstream Consumer", |rocket| {
            Box::pin(async move {
                let pool = rocket.state::<sqlx::PgPool>().unwrap().clone();
                let http = rocket.state::<reqwest::Client>().unwrap().clone();
                let bus = rocket.state::<events::EventBus>().unwrap().clone();
                rocket::tokio::spawn(jetstream::run(pool, http, bus));
            })
        }))
        // ── Background: Backfill task ─────────────────────────────────────────
        .attach(AdHoc::on_liftoff("Backfill", |rocket| {
            Box::pin(async move {
                let pool = rocket.state::<sqlx::PgPool>().unwrap().clone();
                let http = rocket.state::<reqwest::Client>().unwrap().clone();
                rocket::tokio::spawn(backfill::run(pool, http));
            })
        }))
        .attach(CORS)
}
