use std::io::Cursor;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::{get, response};

use crate::lib::embed_fetch::{playable_video_type, validate_and_fetch};
use crate::lib::hum_guard::RateLimiter;
use crate::lib::range::{RangeResult, parse_range};
use crate::lib::responses::{ErrorCode, ErrorResponse};

use super::get_image_handler::ClientIp;

const MAX_VIDEO_BYTES: usize = 8 * 1024 * 1024;

const CACHE_CONTROL: &str = "public, max-age=3600";

const DEFAULT_RATE_PER_MIN: u32 = 120;

fn rate_per_min() -> u32 {
    std::env::var("EMBED_VIDEO_RATE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RATE_PER_MIN)
}

static RATE: LazyLock<Mutex<RateLimiter>> =
    LazyLock::new(|| Mutex::new(RateLimiter::new(rate_per_min(), Duration::from_secs(60))));

fn rate_ok(key: &str) -> bool {
    RATE.lock().unwrap().check_at(key, Instant::now())
}

pub struct VideoResponse {
    bytes: Bytes,
    content_type: String,
}

impl<'r> Responder<'r, 'static> for VideoResponse {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        let ct = self
            .content_type
            .parse::<ContentType>()
            .unwrap_or(ContentType::Binary);
        let total = self.bytes.len() as u64;

        match parse_range(req.headers().get_one("Range"), total) {
            RangeResult::Full => Response::build()
                .header(ct)
                .raw_header("Accept-Ranges", "bytes")
                .raw_header("Cache-Control", CACHE_CONTROL)
                .raw_header("X-Content-Type-Options", "nosniff")
                .sized_body(self.bytes.len(), Cursor::new(self.bytes))
                .ok(),
            RangeResult::Partial { start, end } => {
                let part = self.bytes.slice(start as usize..=end as usize);
                Response::build()
                    .status(Status::PartialContent)
                    .header(ct)
                    .raw_header("Accept-Ranges", "bytes")
                    .raw_header("Content-Range", format!("bytes {start}-{end}/{total}"))
                    .raw_header("Cache-Control", CACHE_CONTROL)
                    .raw_header("X-Content-Type-Options", "nosniff")
                    .sized_body(part.len(), Cursor::new(part))
                    .ok()
            }
            RangeResult::Unsatisfiable => Response::build()
                .status(Status::RangeNotSatisfiable)
                .raw_header("Content-Range", format!("bytes */{total}"))
                .ok(),
        }
    }
}

pub enum GetVideoResponse {
    Video(VideoResponse),
    NotVideo,
    Upstream(ErrorResponse),
}

impl<'r> Responder<'r, 'static> for GetVideoResponse {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        match self {
            GetVideoResponse::Video(v) => v.respond_to(req),
            GetVideoResponse::NotVideo => ErrorCode::NotAVideo
                .with("The linked resource is not a video type we serve.")
                .respond_to(req),
            GetVideoResponse::Upstream(e) => e.respond_to(req),
        }
    }
}

type RateOkFn = dyn Fn(&str) -> bool + Send + Sync;

fn rate_limited() -> GetVideoResponse {
    GetVideoResponse::Upstream(
        ErrorCode::RateLimited.with("Too many video requests; try again shortly."),
    )
}

async fn get_video_inner(url: &str, rate_key: &str, rate_ok_fn: &RateOkFn) -> GetVideoResponse {
    if !rate_ok_fn(rate_key) {
        return rate_limited();
    }

    let resource = match validate_and_fetch(url, MAX_VIDEO_BYTES).await {
        Ok(r) => r,
        Err(err) => return GetVideoResponse::Upstream(ErrorResponse::from(err)),
    };

    if playable_video_type(&resource.content_type).is_none() {
        return GetVideoResponse::NotVideo;
    }

    if resource.bytes.len() >= MAX_VIDEO_BYTES {
        return GetVideoResponse::NotVideo;
    }

    GetVideoResponse::Video(VideoResponse {
        bytes: Bytes::from(resource.bytes),
        content_type: resource.content_type,
    })
}

#[get("/xrpc/social.colibri.embed.getVideo?<url>")]
pub async fn get_video(url: &str, client_ip: ClientIp) -> GetVideoResponse {
    let key = client_ip
        .0
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| String::from("unknown"));
    get_video_inner(url, &key, &rate_ok).await
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
        let resp = get_video_inner("http://127.0.0.1/clip.mp4", "test-ip", &always_ok).await;
        match resp {
            GetVideoResponse::Upstream(e) => {
                assert_eq!(e.body.into_inner().error, "InvalidRequest");
            }
            _ => panic!("expected upstream error"),
        }
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        let resp = get_video_inner("ftp://example.com/clip.mp4", "test-ip", &always_ok).await;
        assert!(matches!(resp, GetVideoResponse::Upstream(_)));
    }

    #[tokio::test]
    async fn rejects_when_rate_limited() {
        let resp = get_video_inner("http://127.0.0.1/clip.mp4", "test-ip", &|_| false).await;
        match resp {
            GetVideoResponse::Upstream(e) => {
                assert_eq!(e.body.into_inner().error, "RateLimited");
            }
            _ => panic!("expected rate-limited error"),
        }
    }

    #[test]
    fn allows_only_playable_video_types() {
        assert!(playable_video_type("video/mp4").is_some());
        assert!(playable_video_type("video/webm; codecs=vp9").is_some());
        assert!(playable_video_type("video/quicktime").is_none());
        assert!(playable_video_type("text/html").is_none());
        assert!(playable_video_type("image/gif").is_none());
    }
}
