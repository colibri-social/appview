use std::io::Cursor;
use std::net::IpAddr;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use rocket::http::{ContentType, Status};
use rocket::request::{FromRequest, Outcome, Request};
use rocket::response::{Responder, Response};
use rocket::serde::json::Json;
use rocket::{get, response};

use crate::lib::embed_fetch::{FetchError, validate_and_fetch};
use crate::lib::hum_guard::RateLimiter;
use crate::lib::responses::{ErrorBody, ErrorResponse};

/// Preview images can be larger than HTML; cap at 8 MiB.
const MAX_IMAGE_BYTES: usize = 8 * 1024 * 1024;

/// Raster types this proxy will forward. Deliberately excludes
/// `image/svg+xml` — SVG can carry `<script>`, which would execute in the
/// AppView's origin on direct navigation to this endpoint.
const ALLOWED_IMAGE_TYPES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/gif",
    "image/webp",
    "image/avif",
    "image/bmp",
    "image/x-icon",
    "image/vnd.microsoft.icon",
];

fn is_allowed_raster(content_type: &str) -> bool {
    let base = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    ALLOWED_IMAGE_TYPES.contains(&base.as_str())
}

const DEFAULT_RATE_PER_MIN: u32 = 120;

fn rate_per_min() -> u32 {
    std::env::var("EMBED_IMAGE_RATE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RATE_PER_MIN)
}

static RATE: LazyLock<Mutex<RateLimiter>> =
    LazyLock::new(|| Mutex::new(RateLimiter::new(rate_per_min(), Duration::from_secs(60))));

/// Process-global per-client-IP rate check for the unauthenticated image
/// proxy, since it has no caller identity to key on otherwise.
fn rate_ok(key: &str) -> bool {
    RATE.lock().unwrap().check_at(key, Instant::now())
}

/// The requester's IP, best-effort (falls back to the TCP peer address when
/// no `ip_header` is configured/present).
pub struct ClientIp(pub Option<IpAddr>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ClientIp {
    type Error = std::convert::Infallible;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        Outcome::Success(ClientIp(req.client_ip()))
    }
}

pub struct ImageResponse {
    bytes: Vec<u8>,
    content_type: String,
}

impl<'r> Responder<'r, 'static> for ImageResponse {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let ct = self
            .content_type
            .parse::<ContentType>()
            .unwrap_or(ContentType::Binary);
        Response::build()
            .header(ct)
            .raw_header("X-Content-Type-Options", "nosniff")
            .sized_body(self.bytes.len(), Cursor::new(self.bytes))
            .ok()
    }
}

pub enum GetImageResponse {
    Image(ImageResponse),
    NotImage,
    Upstream(ErrorResponse),
}

impl<'r> Responder<'r, 'static> for GetImageResponse {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        match self {
            GetImageResponse::Image(b) => b.respond_to(req),
            GetImageResponse::NotImage => {
                let body = r#"{"error":"NotAnImage"}"#;
                Response::build()
                    .status(Status::UnsupportedMediaType)
                    .header(ContentType::JSON)
                    .sized_body(body.len(), Cursor::new(body))
                    .ok()
            }
            GetImageResponse::Upstream(e) => e.respond_to(req),
        }
    }
}

type RateOkFn = dyn Fn(&str) -> bool + Send + Sync;

fn rate_limited() -> GetImageResponse {
    GetImageResponse::Upstream(ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("RateLimited"),
            message: String::from("Too many image requests; try again shortly."),
        }),
    })
}

async fn get_image_inner(url: &str, rate_key: &str, rate_ok_fn: &RateOkFn) -> GetImageResponse {
    if !rate_ok_fn(rate_key) {
        return rate_limited();
    }

    let resource = match validate_and_fetch(url, MAX_IMAGE_BYTES).await {
        Ok(r) => r,
        Err(err) => {
            let code = match err {
                FetchError::InvalidUrl(_) | FetchError::Blocked(_) => "InvalidRequest",
                _ => "UpstreamError",
            };
            return GetImageResponse::Upstream(ErrorResponse {
                body: Json(ErrorBody {
                    error: String::from(code),
                    message: err.to_string(),
                }),
            });
        }
    };

    // Only ever stream back a known-safe raster image type — never SVG
    // (script-capable) or arbitrary bytes.
    if !is_allowed_raster(&resource.content_type) {
        return GetImageResponse::NotImage;
    }

    GetImageResponse::Image(ImageResponse {
        bytes: resource.bytes,
        content_type: resource.content_type,
    })
}

#[get("/xrpc/social.colibri.embed.getImage?<url>")]
/// Proxies a remote embed preview image through the AppView so the client's IP
/// is never exposed to the image host. Unauthenticated (like
/// `com.atproto.sync.getBlob`) because it's loaded directly via an `<img src>`,
/// but constrained by the SSRF guard, a raster-only content-type allowlist, and
/// a per-client-IP rate limit (`EMBED_IMAGE_RATE_LIMIT`, default 120/min) since
/// it has no other caller identity to bound abuse by.
pub async fn get_image(url: &str, client_ip: ClientIp) -> GetImageResponse {
    let key = client_ip
        .0
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| String::from("unknown"));
    get_image_inner(url, &key, &rate_ok).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    fn always_ok(_: &str) -> bool {
        true
    }

    #[tokio::test]
    async fn rejects_loopback_url() {
        let resp = get_image_inner("http://127.0.0.1/x.png", "test-ip", &always_ok).await;
        match resp {
            GetImageResponse::Upstream(e) => {
                assert_eq!(e.body.into_inner().error, "InvalidRequest");
            }
            _ => panic!("expected upstream error"),
        }
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        let resp = get_image_inner("ftp://example.com/x.png", "test-ip", &always_ok).await;
        assert!(matches!(resp, GetImageResponse::Upstream(_)));
    }

    #[tokio::test]
    async fn rejects_when_rate_limited() {
        let resp = get_image_inner("http://127.0.0.1/x.png", "test-ip", &|_| false).await;
        match resp {
            GetImageResponse::Upstream(e) => {
                assert_eq!(e.body.into_inner().error, "RateLimited");
            }
            _ => panic!("expected rate-limited error"),
        }
    }

    #[test]
    fn allows_known_raster_types() {
        assert!(is_allowed_raster("image/png"));
        assert!(is_allowed_raster("image/jpeg; charset=binary"));
    }

    #[test]
    fn rejects_svg_and_unknown_types() {
        assert!(!is_allowed_raster("image/svg+xml"));
        assert!(!is_allowed_raster("text/html"));
    }
}
