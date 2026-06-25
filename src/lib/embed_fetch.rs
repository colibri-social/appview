//! SSRF-safe outbound fetching and OpenGraph/Twitter-card parsing for link
//! embeds. Both the `getMetadata` and `getImage` embed handlers route their
//! outbound requests through [`validate_and_fetch`] so the client's IP is never
//! exposed to the target site and the AppView cannot be coerced into fetching
//! internal/private addresses on a caller's behalf.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use reqwest::{Client, Url, redirect::Policy};
use scraper::{Html, Selector};
use serde::Serialize;

/// How long an outbound fetch may take before it's abandoned.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
/// Maximum number of redirects we'll follow (each one is re-validated).
const MAX_REDIRECTS: usize = 5;
/// A browser-ish UA — many sites only emit OG tags for "real" user agents.
const USER_AGENT: &str = "Mozilla/5.0 (compatible; ColibriBot/1.0; +https://colibri.social)";

/// The embed metadata returned to the client. Field names are serialized to the
/// camelCase shape the client expects.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EmbedMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "siteName", skip_serializing_if = "Option::is_none")]
    pub site_name: Option<String>,
    #[serde(rename = "themeColor", skip_serializing_if = "Option::is_none")]
    pub theme_color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<Vec<EmbedImage>>,
    /// Whether the preview image should render large (Discord's
    /// `summary_large_image` style) vs a small thumbnail beside the text
    /// (`summary`). Only present when there's an image.
    #[serde(rename = "largeImage", skip_serializing_if = "Option::is_none")]
    pub large_image: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EmbedImage {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alt: Option<String>,
}

impl EmbedMetadata {
    /// True when there's nothing worth rendering a card for.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.title.is_none() && self.description.is_none() && self.image.is_none()
    }
}

#[derive(Debug)]
pub enum FetchError {
    /// URL didn't parse or used a disallowed scheme.
    InvalidUrl(String),
    /// Host resolved to a private/loopback/link-local address (SSRF guard).
    Blocked(String),
    /// Redirect chain exceeded [`MAX_REDIRECTS`].
    TooManyRedirects,
    /// Network/transport failure talking to the upstream.
    Upstream(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::InvalidUrl(m) => write!(f, "invalid url: {m}"),
            FetchError::Blocked(m) => write!(f, "blocked address: {m}"),
            FetchError::TooManyRedirects => write!(f, "too many redirects"),
            FetchError::Upstream(m) => write!(f, "upstream error: {m}"),
        }
    }
}

/// A fully-read upstream response, body capped at the caller's limit.
pub struct FetchedResource {
    /// The final URL after following redirects — used to resolve relative URLs.
    pub final_url: Url,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// Returns true for any IP we must never connect to on a caller's behalf:
/// loopback, private, link-local, unspecified, CGNAT, and ULA/IPv6 equivalents.
pub fn is_blocked_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                // 0.0.0.0/8 "this host on this network"
                || v4.octets()[0] == 0
                // 100.64.0.0/10 CGNAT shared address space
                || (v4.octets()[0] == 100 && (64..=127).contains(&v4.octets()[1]))
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_ip(&IpAddr::V4(mapped));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                // fc00::/7 unique local addresses
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // fe80::/10 link-local
                || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

/// Resolves `url`'s host and rejects it if every/any resolved address is a
/// blocked (private/loopback/etc.) IP. Rejecting when *any* resolved address is
/// blocked is the conservative choice and frustrates DNS-rebinding tricks.
async fn assert_host_allowed(url: &Url) -> Result<(), FetchError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FetchError::InvalidUrl(format!(
            "unsupported scheme '{}'",
            url.scheme()
        )));
    }

    let host = url
        .host_str()
        .ok_or_else(|| FetchError::InvalidUrl("missing host".into()))?;
    let port = url.port_or_known_default().unwrap_or(80);

    // A bare IP literal never hits DNS — check it directly.
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_blocked_ip(&ip) {
            return Err(FetchError::Blocked(host.to_string()));
        }
        return Ok(());
    }

    let mut addrs = rocket::tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| FetchError::Upstream(format!("dns lookup failed: {e}")))?
        .peekable();

    if addrs.peek().is_none() {
        return Err(FetchError::Upstream(format!("no addresses for {host}")));
    }

    for addr in addrs {
        if is_blocked_ip(&addr.ip()) {
            return Err(FetchError::Blocked(format!("{host} -> {}", addr.ip())));
        }
    }

    Ok(())
}

/// SSRF-safe GET: validates the host (and every redirect target) against the
/// private-address blocklist, follows up to [`MAX_REDIRECTS`] redirects
/// manually, and reads at most `max_bytes` of the body.
pub async fn validate_and_fetch(
    raw_url: &str,
    max_bytes: usize,
) -> Result<FetchedResource, FetchError> {
    let mut current = Url::parse(raw_url).map_err(|e| FetchError::InvalidUrl(e.to_string()))?;

    // Redirects are handled by hand so each hop can be re-validated.
    let client = Client::builder()
        .timeout(FETCH_TIMEOUT)
        .redirect(Policy::none())
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| FetchError::Upstream(e.to_string()))?;

    for _ in 0..=MAX_REDIRECTS {
        assert_host_allowed(&current).await?;

        let resp = client
            .get(current.clone())
            .send()
            .await
            .map_err(|e| FetchError::Upstream(e.to_string()))?;

        let status = resp.status();
        if status.is_redirection() {
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| FetchError::Upstream("redirect without location".into()))?;
            current = current
                .join(location)
                .map_err(|e| FetchError::InvalidUrl(e.to_string()))?;
            continue;
        }

        if !status.is_success() {
            return Err(FetchError::Upstream(format!("upstream returned {status}")));
        }

        let final_url = resp.url().clone();
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        // Read in chunks so an oversized body is abandoned early.
        let mut resp = resp;
        let mut bytes: Vec<u8> = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| FetchError::Upstream(e.to_string()))?
        {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > max_bytes {
                bytes.truncate(max_bytes);
                break;
            }
        }

        return Ok(FetchedResource {
            final_url,
            content_type,
            bytes,
        });
    }

    Err(FetchError::TooManyRedirects)
}

/// Parses OpenGraph / Twitter-card / standard meta tags out of an HTML document.
/// `base` is the (post-redirect) page URL, used to absolutize relative images.
pub fn extract_metadata(html: &str, base: &Url) -> EmbedMetadata {
    let doc = Html::parse_document(html);

    // Collect every <meta property|name=...> into a lowercased key -> content map,
    // keeping the first occurrence of each key (matches typical OG precedence).
    let mut metas: HashMap<String, String> = HashMap::new();
    if let Ok(sel) = Selector::parse("meta") {
        for el in doc.select(&sel) {
            let key = el
                .value()
                .attr("property")
                .or_else(|| el.value().attr("name"));
            if let (Some(k), Some(content)) = (key, el.value().attr("content")) {
                let content = content.trim();
                if !content.is_empty() {
                    metas
                        .entry(k.to_ascii_lowercase())
                        .or_insert_with(|| content.to_string());
                }
            }
        }
    }

    let get = |k: &str| metas.get(k).cloned();

    let doc_title = Selector::parse("title").ok().and_then(|sel| {
        doc.select(&sel)
            .next()
            .map(|e| e.text().collect::<String>().trim().to_string())
            .filter(|t| !t.is_empty())
    });

    let title = get("og:title")
        .or_else(|| get("twitter:title"))
        .or(doc_title);
    let description = get("og:description")
        .or_else(|| get("twitter:description"))
        .or_else(|| get("description"));
    let site_name = get("og:site_name").or_else(|| base.host_str().map(|h| h.to_string()));
    let theme_color = get("theme-color");

    // The page's canonical URL — some sites (mis)set og:image to it, which is
    // never a real image. Resolve candidates in preference order and skip any
    // that collapse to the page URL so we fall through to a usable one
    // (e.g. og:image == page URL → use twitter:image instead).
    let page_url = get("og:url")
        .and_then(|u| base.join(&u).ok())
        .unwrap_or_else(|| base.clone());

    let image_url = [
        "og:image",
        "og:image:url",
        "og:image:secure_url",
        "twitter:image",
        "twitter:image:src",
    ]
    .iter()
    .filter_map(|k| get(k))
    .filter_map(|raw| base.join(&raw).ok())
    .find(|abs| abs.as_str() != page_url.as_str() && abs.as_str() != base.as_str());

    let image = image_url.map(|abs| {
        vec![EmbedImage {
            url: abs.to_string(),
            alt: get("og:image:alt").or_else(|| get("twitter:image:alt")),
        }]
    });

    // Discord-style sizing: large unless the card is explicitly a `summary`.
    let large_image = image
        .as_ref()
        .map(|_| get("twitter:card").as_deref() != Some("summary"));

    EmbedMetadata {
        title,
        description,
        site_name,
        theme_color,
        image,
        large_image,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    #[test]
    fn blocks_loopback_and_private_ips() {
        assert!(is_blocked_ip(&"127.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip(&"10.0.0.5".parse().unwrap()));
        assert!(is_blocked_ip(&"192.168.1.1".parse().unwrap()));
        assert!(is_blocked_ip(&"169.254.1.1".parse().unwrap()));
        assert!(is_blocked_ip(&"100.64.0.1".parse().unwrap()));
        assert!(is_blocked_ip(&"0.0.0.0".parse().unwrap()));
        assert!(is_blocked_ip(&"::1".parse().unwrap()));
        assert!(is_blocked_ip(&"fc00::1".parse().unwrap()));
        assert!(is_blocked_ip(&"fe80::1".parse().unwrap()));
    }

    #[test]
    fn allows_public_ips() {
        assert!(!is_blocked_ip(&"1.1.1.1".parse().unwrap()));
        assert!(!is_blocked_ip(&"8.8.8.8".parse().unwrap()));
        assert!(!is_blocked_ip(&"2606:4700:4700::1111".parse().unwrap()));
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        let err = assert_host_allowed(&Url::parse("ftp://example.com").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, FetchError::InvalidUrl(_)));
    }

    #[tokio::test]
    async fn rejects_loopback_literal_host() {
        let err = assert_host_allowed(&Url::parse("http://127.0.0.1/foo").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, FetchError::Blocked(_)));
    }

    #[test]
    fn extracts_og_with_precedence() {
        let base = Url::parse("https://example.com/article").unwrap();
        let html = r##"
            <html><head>
                <title>Fallback Title</title>
                <meta property="og:title" content="OG Title" />
                <meta name="twitter:title" content="TW Title" />
                <meta property="og:description" content="A description." />
                <meta property="og:site_name" content="Example" />
                <meta name="theme-color" content="#ff8800" />
                <meta property="og:image" content="/img/cover.png" />
                <meta property="og:image:alt" content="Cover" />
            </head><body></body></html>
        "##;

        let meta = extract_metadata(html, &base);
        assert_eq!(meta.title.as_deref(), Some("OG Title"));
        assert_eq!(meta.description.as_deref(), Some("A description."));
        assert_eq!(meta.site_name.as_deref(), Some("Example"));
        assert_eq!(meta.theme_color.as_deref(), Some("#ff8800"));
        let img = meta.image.unwrap();
        assert_eq!(img[0].url, "https://example.com/img/cover.png");
        assert_eq!(img[0].alt.as_deref(), Some("Cover"));
    }

    #[test]
    fn skips_og_image_equal_to_page_url() {
        // Mirrors a real misconfiguration (lou.gg): og:image points at the page
        // itself, so we must fall through to twitter:image.
        let base = Url::parse("https://lou.gg/").unwrap();
        let html = r#"
            <html><head>
                <meta property="og:image" content="https://lou.gg/" />
                <meta property="og:url" content="https://lou.gg/" />
                <meta name="twitter:image" content="/img/og.png" />
                <meta name="twitter:card" content="summary_large_image" />
                <meta property="og:title" content="Louis" />
            </head></html>
        "#;
        let meta = extract_metadata(html, &base);
        let img = meta.image.expect("should fall through to twitter:image");
        assert_eq!(img[0].url, "https://lou.gg/img/og.png");
        assert_eq!(meta.large_image, Some(true));
    }

    #[test]
    fn summary_card_marks_thumbnail() {
        let base = Url::parse("https://example.com/p").unwrap();
        let html = r#"
            <html><head>
                <meta property="og:image" content="/thumb.png" />
                <meta name="twitter:card" content="summary" />
            </head></html>
        "#;
        let meta = extract_metadata(html, &base);
        assert!(meta.image.is_some());
        assert_eq!(meta.large_image, Some(false));
    }

    #[test]
    fn falls_back_to_title_tag_and_host() {
        let base = Url::parse("https://news.example.org/x").unwrap();
        let html = "<html><head><title>  Just A Title  </title></head><body></body></html>";
        let meta = extract_metadata(html, &base);
        assert_eq!(meta.title.as_deref(), Some("Just A Title"));
        assert_eq!(meta.site_name.as_deref(), Some("news.example.org"));
        assert!(meta.description.is_none());
        assert!(meta.image.is_none());
    }

    #[test]
    fn empty_when_no_usable_metadata() {
        let base = Url::parse("https://example.com").unwrap();
        let meta = extract_metadata("<html><head></head><body>hi</body></html>", &base);
        assert!(meta.is_empty());
    }
}
