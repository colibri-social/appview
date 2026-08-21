use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};

use crate::lib::embed_cache::EmbedCache;
use crate::lib::embed_fetch::{
    EmbedImage, EmbedMetadata, EmbedVideo, FetchError, FetchedResource, VideoProbe,
    extract_metadata, playable_video_type,
};
use crate::lib::klipy::{self, KlipyMedia};
use crate::lib::responses::{ErrorCode, ErrorResponse};
use crate::lib::service_auth;
use reqwest::Url;

const LXM: &str = "social.colibri.embed.getMetadata";
/// HTML bodies are parsed for `<head>` meta only; 1 MiB is plenty.
const MAX_HTML_BYTES: usize = 1024 * 1024;
const MAX_VIDEO_BYTES: u64 = 8 * 1024 * 1024;

fn auth_error(err: service_auth::ServiceAuthError) -> ErrorResponse {
    ErrorCode::AuthRequired.with(err.to_string())
}

fn upstream_error(err: FetchError) -> ErrorResponse {
    ErrorResponse::from(err)
}

/// Core logic, parameterized over the auth + fetch dependencies so it can be
/// unit-tested without the network. Mirrors the `_with` pattern used by
/// `set_state_handler`.
async fn get_metadata_with<VA, FE, PV, KL>(
    uri: String,
    auth: String,
    cache: &EmbedCache,
    verify_auth_fn: VA,
    fetch_fn: FE,
    probe_video_fn: PV,
    klipy_fn: KL,
) -> Result<Json<EmbedMetadata>, ErrorResponse>
where
    VA: Fn(String, String) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>>,
    FE: Fn(String) -> BoxFuture<'static, Result<FetchedResource, FetchError>>,
    PV: Fn(String) -> BoxFuture<'static, Result<VideoProbe, FetchError>>,
    KL: Fn(String, String) -> BoxFuture<'static, Result<KlipyMedia, ErrorResponse>>,
{
    verify_auth_fn(auth, String::from(LXM))
        .await
        .map_err(auth_error)?;

    if let Some(cached) = cache.get(&uri).await {
        return Ok(Json(cached));
    }

    if let Some((klipy_resource, slug)) = klipy::parse_media_page(&uri)
        && let Ok(media) = klipy_fn(klipy_resource.to_string(), slug).await
    {
        let mut metadata = metadata_from_klipy(media);
        metadata.video = confirm_playable(metadata.video.take(), &probe_video_fn).await;
        cache.insert(uri, metadata.clone()).await;
        return Ok(Json(metadata));
    }

    let resource = fetch_fn(uri.clone()).await.map_err(upstream_error)?;

    let html = String::from_utf8_lossy(&resource.bytes);
    let mut metadata = extract_metadata(&html, &resource.final_url);
    metadata.video = confirm_playable(metadata.video.take(), &probe_video_fn).await;

    // Cache even "empty" results to avoid hammering pages with no OG tags.
    cache.insert(uri, metadata.clone()).await;

    Ok(Json(metadata))
}

fn metadata_from_klipy(media: KlipyMedia) -> EmbedMetadata {
    let pixels = |value: Option<u64>| value.and_then(|found| u32::try_from(found).ok());

    EmbedMetadata {
        title: media.title,
        description: None,
        site_name: Some(String::from("Klipy")),
        theme_color: None,
        image: Some(vec![EmbedImage {
            url: media.image.url,
            alt: None,
            width: pixels(media.image.width),
            height: pixels(media.image.height),
        }]),
        video: media.video.map(|(variant, mime)| {
            vec![EmbedVideo {
                url: variant.url,
                mime_type: Some(String::from(mime)),
                width: pixels(variant.width),
                height: pixels(variant.height),
            }]
        }),
        large_image: Some(true),
    }
}

async fn confirm_playable<PV>(
    candidates: Option<Vec<EmbedVideo>>,
    probe_video_fn: &PV,
) -> Option<Vec<EmbedVideo>>
where
    PV: Fn(String) -> BoxFuture<'static, Result<VideoProbe, FetchError>>,
{
    let mut confirmed = Vec::new();

    for candidate in candidates? {
        let Ok(probe) = probe_video_fn(candidate.url.clone()).await else {
            continue;
        };
        let Some(mime) = playable_video_type(&probe.content_type) else {
            continue;
        };
        if probe.length.is_none_or(|length| length > MAX_VIDEO_BYTES) {
            continue;
        }
        confirmed.push(EmbedVideo {
            mime_type: Some(String::from(mime)),
            ..candidate
        });
    }

    (!confirmed.is_empty()).then_some(confirmed)
}

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn fetch_boxed(uri: String) -> BoxFuture<'static, Result<FetchedResource, FetchError>> {
    Box::pin(async move { crate::lib::embed_fetch::validate_and_fetch(&uri, MAX_HTML_BYTES).await })
}

fn probe_video_boxed(url: String) -> BoxFuture<'static, Result<VideoProbe, FetchError>> {
    Box::pin(async move { crate::lib::embed_fetch::probe_video(&url).await })
}

fn klipy_boxed(
    resource: String,
    slug: String,
) -> BoxFuture<'static, Result<KlipyMedia, ErrorResponse>> {
    Box::pin(async move { klipy::media_by_slug(&resource, &slug).await })
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
        return Err(ErrorCode::InvalidRequest.with("Malformed URL"));
    }

    get_metadata_with(
        uri.to_string(),
        auth.to_string(),
        cache.inner(),
        verify_auth_boxed,
        fetch_boxed,
        probe_video_boxed,
        klipy_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::embed_fetch::EmbedImage;
    use rocket::tokio;

    fn ok_auth()
    -> impl Fn(String, String) -> BoxFuture<'static, Result<String, service_auth::ServiceAuthError>>
    {
        |_, _| Box::pin(async { Ok(String::from("did:plc:abc")) })
    }

    fn no_klipy() -> impl Fn(String, String) -> BoxFuture<'static, Result<KlipyMedia, ErrorResponse>>
    {
        |_, _| Box::pin(async { Err(ErrorCode::UpstreamFailure.with("no klipy")) })
    }

    fn no_probe() -> impl Fn(String) -> BoxFuture<'static, Result<VideoProbe, FetchError>> {
        |_| Box::pin(async { Err(FetchError::Upstream(String::from("no probe"))) })
    }

    fn probe_as(
        content_type: &'static str,
        length: Option<u64>,
    ) -> impl Fn(String) -> BoxFuture<'static, Result<VideoProbe, FetchError>> {
        move |_| {
            Box::pin(async move {
                Ok(VideoProbe {
                    content_type: String::from(content_type),
                    length,
                })
            })
        }
    }

    fn html_resource(
        html: &'static str,
    ) -> impl Fn(String) -> BoxFuture<'static, Result<FetchedResource, FetchError>> {
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
            |_, _| Box::pin(async { Err(service_auth::ServiceAuthError::InvalidSignature) }),
            html_resource("<html></html>"),
            no_probe(),
            no_klipy(),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "AuthRequired"
        );
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
            no_probe(),
            no_klipy(),
        )
        .await
        .unwrap();

        assert_eq!(result.title.as_deref(), Some("Hello"));
        assert_eq!(
            result.image.as_ref().unwrap()[0],
            EmbedImage {
                url: String::from("https://example.com/cover.png"),
                alt: None,
                width: None,
                height: None,
            }
        );

        // Second call must hit the cache, not the fetcher (which would panic).
        let cached = get_metadata_with(
            String::from("https://example.com/article"),
            String::from("token"),
            &cache,
            ok_auth(),
            |_| Box::pin(async { panic!("should have hit cache") }),
            no_probe(),
            no_klipy(),
        )
        .await
        .unwrap();
        assert_eq!(cached.title.as_deref(), Some("Hello"));
    }

    const GIFBOX_HTML: &str = r#"<html><head>
        <meta property="og:title" content="Anime Spray Face" />
        <meta name="twitter:card" content="player" />
        <meta property="og:image" content="https://rpc.gifbox.me/media/post/abc/poster" />
        <meta property="og:video" content="https://rpc.gifbox.me/media/post/abc/mp4" />
        <meta property="og:video:type" content="video/mp4" />
        <meta property="og:video:width" content="520" />
        <meta property="og:video:height" content="292" />
    </head></html>"#;

    async fn metadata_for(
        html: &'static str,
        probe: impl Fn(String) -> BoxFuture<'static, Result<VideoProbe, FetchError>>,
    ) -> EmbedMetadata {
        let cache = EmbedCache::default();
        get_metadata_with(
            String::from("https://example.com/article"),
            String::from("token"),
            &cache,
            ok_auth(),
            html_resource(html),
            probe,
            no_klipy(),
        )
        .await
        .unwrap()
        .into_inner()
    }

    #[tokio::test]
    async fn resolves_a_klipy_page_through_the_api_instead_of_scraping() {
        let cache = EmbedCache::default();
        let metadata = get_metadata_with(
            String::from("https://klipy.com/gifs/nix-nixos"),
            String::from("token"),
            &cache,
            ok_auth(),
            |_| Box::pin(async { panic!("must not scrape a Klipy page") }),
            probe_as("video/mp4", Some(231_164)),
            |resource, slug| {
                Box::pin(async move {
                    assert_eq!(resource, "gifs");
                    assert_eq!(slug, "nix-nixos");
                    Ok(KlipyMedia {
                        title: Some(String::from("NixOS Rebuild Switch Heart Locket")),
                        image: klipy::KlipyVariant {
                            url: String::from("https://static.klipy.com/md.gif"),
                            width: Some(400),
                            height: Some(300),
                        },
                        video: Some((
                            klipy::KlipyVariant {
                                url: String::from("https://static.klipy.com/md.mp4"),
                                width: Some(400),
                                height: Some(300),
                            },
                            "video/mp4",
                        )),
                    })
                })
            },
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(metadata.site_name.as_deref(), Some("Klipy"));
        assert_eq!(
            metadata.title.as_deref(),
            Some("NixOS Rebuild Switch Heart Locket")
        );
        assert_eq!(metadata.large_image, Some(true));
        assert_eq!(
            metadata.image.as_ref().unwrap()[0].url,
            "https://static.klipy.com/md.gif"
        );
        let video = metadata.video.as_ref().expect("playable video");
        assert_eq!(video[0].url, "https://static.klipy.com/md.mp4");
        assert_eq!(video[0].mime_type.as_deref(), Some("video/mp4"));
    }

    #[tokio::test]
    async fn falls_back_to_scraping_when_the_klipy_api_fails() {
        let cache = EmbedCache::default();
        let metadata = get_metadata_with(
            String::from("https://klipy.com/gifs/nix-nixos"),
            String::from("token"),
            &cache,
            ok_auth(),
            html_resource(r#"<html><head><title>Just a moment...</title></head></html>"#),
            no_probe(),
            no_klipy(),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(metadata.title.as_deref(), Some("Just a moment..."));
        assert!(metadata.video.is_none());
    }

    #[tokio::test]
    async fn leaves_a_non_klipy_url_on_the_scrape_path() {
        let cache = EmbedCache::default();
        let metadata = get_metadata_with(
            String::from("https://example.com/article"),
            String::from("token"),
            &cache,
            ok_auth(),
            html_resource(r#"<html><head><meta property="og:title" content="Hi" /></head></html>"#),
            no_probe(),
            |_, _| Box::pin(async { panic!("must not consult Klipy") }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(metadata.title.as_deref(), Some("Hi"));
    }

    #[tokio::test]
    async fn keeps_a_confirmed_extensionless_video() {
        let metadata = metadata_for(GIFBOX_HTML, probe_as("video/mp4", Some(62_196))).await;

        let video = metadata.video.as_ref().expect("video kept");
        assert_eq!(video[0].url, "https://rpc.gifbox.me/media/post/abc/mp4");
        assert_eq!(video[0].mime_type.as_deref(), Some("video/mp4"));
        assert_eq!(video[0].width, Some(520));
        assert_eq!(video[0].height, Some(292));
    }

    #[tokio::test]
    async fn drops_a_video_the_host_serves_as_html() {
        let metadata = metadata_for(GIFBOX_HTML, probe_as("text/html", Some(1024))).await;
        assert!(metadata.video.is_none());
        assert!(metadata.image.is_some());
    }

    #[tokio::test]
    async fn drops_a_video_larger_than_the_proxy_cap() {
        let metadata =
            metadata_for(GIFBOX_HTML, probe_as("video/mp4", Some(50 * 1024 * 1024))).await;
        assert!(metadata.video.is_none());
    }

    #[tokio::test]
    async fn drops_a_video_of_unknown_length() {
        let metadata = metadata_for(GIFBOX_HTML, probe_as("video/mp4", None)).await;
        assert!(metadata.video.is_none());
    }

    #[tokio::test]
    async fn drops_a_video_whose_probe_fails() {
        let metadata = metadata_for(GIFBOX_HTML, no_probe()).await;
        assert!(metadata.video.is_none());
    }
}
