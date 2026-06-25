use std::io::Cursor;

use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Responder, Response};
use rocket::serde::json::Json;
use rocket::{get, response};

use crate::lib::embed_fetch::{FetchError, validate_and_fetch};
use crate::lib::responses::{ErrorBody, ErrorResponse};

/// Preview images can be larger than HTML; cap at 8 MiB.
const MAX_IMAGE_BYTES: usize = 8 * 1024 * 1024;

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

async fn get_image_inner(url: &str) -> GetImageResponse {
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

    // Only ever stream back image content — never arbitrary bytes.
    if !resource.content_type.starts_with("image/") {
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
/// but constrained by the SSRF guard and an `image/*` content-type requirement.
pub async fn get_image(url: &str) -> GetImageResponse {
    get_image_inner(url).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    #[tokio::test]
    async fn rejects_loopback_url() {
        let resp = get_image_inner("http://127.0.0.1/x.png").await;
        match resp {
            GetImageResponse::Upstream(e) => {
                assert_eq!(e.body.into_inner().error, "InvalidRequest");
            }
            _ => panic!("expected upstream error"),
        }
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        let resp = get_image_inner("ftp://example.com/x.png").await;
        assert!(matches!(resp, GetImageResponse::Upstream(_)));
    }
}
