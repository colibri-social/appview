use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};

use crate::lib::embed_cache::EmbedCache;
use crate::lib::embed_fetch::{EmbedMetadata, FetchError, FetchedResource, extract_metadata};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use reqwest::Url;

const LXM: &str = "social.colibri.embed.getMetadata";
/// HTML bodies are parsed for `<head>` meta only; 1 MiB is plenty.
const MAX_HTML_BYTES: usize = 1024 * 1024;

fn auth_error(err: service_auth::ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

fn upstream_error(err: FetchError) -> ErrorResponse {
    let code = match err {
        FetchError::InvalidUrl(_) | FetchError::Blocked(_) => "InvalidRequest",
        _ => "UpstreamError",
    };
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from(code),
            message: err.to_string(),
        }),
    }
}

/// Core logic, parameterized over the auth + fetch dependencies so it can be
/// unit-tested without the network. Mirrors the `_with` pattern used by
/// `set_state_handler`.
async fn get_metadata_with<VA, FE>(
    uri: String,
    auth: String,
    cache: &EmbedCache,
    verify_auth_fn: VA,
    fetch_fn: FE,
) -> Result<Json<EmbedMetadata>, ErrorResponse>
where
    VA: Fn(String, String) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>>,
    FE: Fn(String) -> BoxFuture<'static, Result<FetchedResource, FetchError>>,
{
    verify_auth_fn(auth, String::from(LXM))
        .await
        .map_err(auth_error)?;

    if let Some(cached) = cache.get(&uri).await {
        return Ok(Json(cached));
    }

    let resource = fetch_fn(uri.clone()).await.map_err(upstream_error)?;

    let html = String::from_utf8_lossy(&resource.bytes);
    let metadata = extract_metadata(&html, &resource.final_url);

    // Cache even "empty" results to avoid hammering pages with no OG tags.
    cache.insert(uri, metadata.clone()).await;

    Ok(Json(metadata))
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn fetch_boxed(uri: String) -> BoxFuture<'static, Result<FetchedResource, FetchError>> {
    Box::pin(async move {
        crate::lib::embed_fetch::validate_and_fetch(&uri, MAX_HTML_BYTES).await
    })
}

#[get("/xrpc/social.colibri.embed.getMetadata?<uri>&<auth>")]
/// Fetches a URL server-side and returns its OpenGraph/Twitter-card metadata,
/// so the client's IP is never exposed to the target site. Service-authed to
/// keep the AppView from becoming a public open URL-fetch proxy.
pub async fn get_metadata(
    uri: &str,
    auth: &str,
    cache: &State<EmbedCache>,
) -> Result<Json<EmbedMetadata>, ErrorResponse> {
    // Guard against obviously bad input before doing any work.
    if Url::parse(uri).is_err() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Malformed URL"),
            }),
        });
    }

    get_metadata_with(
        uri.to_string(),
        auth.to_string(),
        cache.inner(),
        verify_auth_boxed,
        fetch_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::embed_fetch::EmbedImage;
    use rocket::tokio;

    fn ok_auth() -> impl Fn(
        String,
        String,
    ) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>> {
        |_, _| Box::pin(async { Ok(String::from("did:plc:abc")) })
    }

    fn html_resource(html: &'static str) -> impl Fn(
        String,
    ) -> BoxFuture<'static, Result<FetchedResource, FetchError>> {
        move |_| {
            Box::pin(async move {
                Ok(FetchedResource {
                    final_url: Url::parse("https://example.com/article").unwrap(),
                    content_type: String::from("text/html"),
                    bytes: html.as_bytes().to_vec(),
                })
            })
        }
    }

    #[tokio::test]
    async fn returns_auth_error_on_bad_token() {
        let cache = EmbedCache::default();
        let result = get_metadata_with(
            String::from("https://example.com"),
            String::from("bad"),
            &cache,
            |_, _| {
                Box::pin(async { Err(service_auth::ServiceAuthError::InvalidSignature) })
            },
            html_resource("<html></html>"),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }

    #[tokio::test]
    async fn parses_and_caches_metadata() {
        let cache = EmbedCache::default();
        let html = r#"<html><head>
            <meta property="og:title" content="Hello" />
            <meta property="og:image" content="/cover.png" />
        </head></html>"#;

        let result = get_metadata_with(
            String::from("https://example.com/article"),
            String::from("token"),
            &cache,
            ok_auth(),
            html_resource(html),
        )
        .await
        .unwrap();

        assert_eq!(result.title.as_deref(), Some("Hello"));
        assert_eq!(
            result.image.as_ref().unwrap()[0],
            EmbedImage {
                url: String::from("https://example.com/cover.png"),
                alt: None,
            }
        );

        // Second call must hit the cache, not the fetcher (which would panic).
        let cached = get_metadata_with(
            String::from("https://example.com/article"),
            String::from("token"),
            &cache,
            ok_auth(),
            |_| Box::pin(async { panic!("should have hit cache") }),
        )
        .await
        .unwrap();
        assert_eq!(cached.title.as_deref(), Some("Hello"));
    }
}
