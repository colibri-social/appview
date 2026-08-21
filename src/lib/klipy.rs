use std::time::Duration;

use reqwest::Client;
use serde::Serialize;
use serde_json::Value;

use crate::lib::responses::{ErrorCode, ErrorResponse};

const KLIPY_BASE: &str = "https://api.klipy.com/api/v1";
const FETCH_TIMEOUT: Duration = Duration::from_secs(6);
pub const DEFAULT_PER_PAGE: u32 = 24;

const MEDIA_SIZES: [&str; 4] = ["md", "hd", "sm", "xs"];
const PREVIEW_SIZES: [&str; 4] = ["sm", "xs", "md", "hd"];

const GIFS_RESOURCE: &str = "gifs";

const MEDIA_RESOURCES: [&str; 6] = [
    "gifs",
    "stickers",
    "clips",
    "static-memes",
    "ai-gifs",
    "emojis",
];

const IMAGE_FORMATS: [&str; 3] = ["gif", "webp", "jpg"];
const VIDEO_FORMATS: [(&str, &str); 2] = [("mp4", "video/mp4"), ("webm", "video/webm")];

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GifItem {
    pub id: String,
    #[serde(rename = "mediaUrl")]
    pub media_url: String,
    #[serde(rename = "previewUrl")]
    pub preview_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GifPage {
    pub items: Vec<GifItem>,
    pub page: u32,
    #[serde(rename = "hasNext")]
    pub has_next: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GifCategory {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(rename = "previewUrl", skip_serializing_if = "Option::is_none")]
    pub preview_url: Option<String>,
}

fn upstream(msg: impl Into<String>) -> ErrorResponse {
    ErrorCode::UpstreamFailure.with(msg.into())
}

fn api_key() -> Result<String, ErrorResponse> {
    match std::env::var("KLIPY_API_KEY") {
        Ok(k) if !k.trim().is_empty() => Ok(k),
        _ => Err(upstream("KLIPY_API_KEY is not configured")),
    }
}

async fn get_json(
    resource: &str,
    path: &str,
    query: &[(&str, String)],
) -> Result<Value, ErrorResponse> {
    let key = api_key()?;
    let base = format!("{KLIPY_BASE}/{key}/{resource}/{path}");
    let mut url = reqwest::Url::parse(&base).map_err(|e| upstream(e.to_string()))?;
    {
        let mut pairs = url.query_pairs_mut();
        for (name, value) in query {
            pairs.append_pair(name, value);
        }
    }

    let client = Client::builder().timeout(FETCH_TIMEOUT).build()?;
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        return Err(upstream(format!("Klipy returned {}", resp.status())));
    }
    Ok(resp.json::<Value>().await?)
}

pub async fn search(q: &str, page: u32) -> Result<GifPage, ErrorResponse> {
    let body = get_json(
        GIFS_RESOURCE,
        "search",
        &[
            ("q", q.to_string()),
            ("page", page.to_string()),
            ("per_page", DEFAULT_PER_PAGE.to_string()),
        ],
    )
    .await?;
    Ok(normalize_page(&body, page))
}

pub async fn trending(page: u32) -> Result<GifPage, ErrorResponse> {
    let body = get_json(
        GIFS_RESOURCE,
        "trending",
        &[
            ("page", page.to_string()),
            ("per_page", DEFAULT_PER_PAGE.to_string()),
        ],
    )
    .await?;
    Ok(normalize_page(&body, page))
}

pub async fn categories() -> Result<Vec<GifCategory>, ErrorResponse> {
    let body = get_json(GIFS_RESOURCE, "categories", &[]).await?;
    Ok(normalize_categories(&body))
}

#[derive(Debug, Clone, PartialEq)]
pub struct KlipyVariant {
    pub url: String,
    pub width: Option<u64>,
    pub height: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KlipyMedia {
    pub title: Option<String>,
    pub image: KlipyVariant,
    pub video: Option<(KlipyVariant, &'static str)>,
}

pub fn parse_media_page(raw_url: &str) -> Option<(&'static str, String)> {
    let url = reqwest::Url::parse(raw_url).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }

    let host = url.host_str()?.to_ascii_lowercase();
    if host != "klipy.com" && host != "www.klipy.com" {
        return None;
    }

    let mut segments = url.path_segments()?.filter(|part| !part.is_empty());
    let requested = segments.next()?;
    let resource = MEDIA_RESOURCES
        .iter()
        .copied()
        .find(|known| *known == requested)?;
    let slug = segments.next()?.to_string();
    if segments.next().is_some() {
        return None;
    }

    let usable = !slug.is_empty()
        && slug
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    usable.then_some((resource, slug))
}

pub async fn media_by_slug(resource: &str, slug: &str) -> Result<KlipyMedia, ErrorResponse> {
    if !MEDIA_RESOURCES.contains(&resource) {
        return Err(upstream("Unsupported Klipy resource"));
    }
    let body = get_json(resource, slug, &[]).await?;
    normalize_media(&body["data"]).ok_or_else(|| upstream("Klipy returned no usable media"))
}

fn media_variant(item: &Value, format: &str) -> Option<KlipyVariant> {
    let file = &item["file"];

    if let Some(node) = pick(file, &MEDIA_SIZES, format) {
        return Some(KlipyVariant {
            url: node["url"].as_str()?.to_string(),
            width: node["width"].as_u64(),
            height: node["height"].as_u64(),
        });
    }

    let url = file[format].as_str()?.to_string();
    let meta = &item["file_meta"][format];
    Some(KlipyVariant {
        url,
        width: meta["width"].as_u64(),
        height: meta["height"].as_u64(),
    })
}

fn normalize_media(item: &Value) -> Option<KlipyMedia> {
    let image = IMAGE_FORMATS
        .iter()
        .find_map(|format| media_variant(item, format))?;

    let video = VIDEO_FORMATS
        .iter()
        .find_map(|(format, mime)| media_variant(item, format).map(|found| (found, *mime)));

    Some(KlipyMedia {
        title: item["title"]
            .as_str()
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(str::to_string),
        image,
        video,
    })
}

fn items_array(body: &Value) -> &[Value] {
    let data = &body["data"];
    if let Some(arr) = data["data"].as_array() {
        arr
    } else if let Some(arr) = data.as_array() {
        arr
    } else {
        &[]
    }
}

fn normalize_page(body: &Value, requested_page: u32) -> GifPage {
    let items = items_array(body)
        .iter()
        .filter_map(normalize_item)
        .collect();

    let data = &body["data"];
    let page = data["current_page"]
        .as_u64()
        .map(|n| n as u32)
        .unwrap_or(requested_page);
    let has_next = data["has_next"].as_bool().unwrap_or(false);

    GifPage {
        items,
        page,
        has_next,
    }
}

fn pick<'a>(file: &'a Value, sizes: &[&str], format: &str) -> Option<&'a Value> {
    sizes.iter().find_map(|size| {
        let node = &file[*size][format];
        node["url"].as_str().map(|_| node)
    })
}

fn normalize_item(item: &Value) -> Option<GifItem> {
    let id = item["slug"]
        .as_str()
        .map(str::to_string)
        .or_else(|| item["id"].as_str().map(str::to_string))
        .or_else(|| item["id"].as_u64().map(|n| n.to_string()))?;

    let file = &item["file"];

    let media = pick(file, &MEDIA_SIZES, "gif").or_else(|| pick(file, &MEDIA_SIZES, "webp"));
    let media_url = media
        .and_then(|n| n["url"].as_str())
        .or_else(|| item["url"].as_str())?
        .to_string();

    let preview = pick(file, &PREVIEW_SIZES, "gif")
        .or_else(|| pick(file, &PREVIEW_SIZES, "webp"))
        .or_else(|| pick(file, &PREVIEW_SIZES, "jpg"));
    let preview_url = preview
        .and_then(|n| n["url"].as_str())
        .unwrap_or(&media_url)
        .to_string();

    let (width, height) = media
        .map(|n| (n["width"].as_u64(), n["height"].as_u64()))
        .unwrap_or((None, None));

    Some(GifItem {
        id,
        media_url,
        preview_url,
        width,
        height,
    })
}

fn normalize_categories(body: &Value) -> Vec<GifCategory> {
    let Some(arr) = body["data"]["categories"].as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|c| {
            let name = c["category"]
                .as_str()
                .or_else(|| c["name"].as_str())
                .or_else(|| c["title"].as_str())
                .map(str::to_string)?;
            let query = c["query"].as_str().map(str::to_string);
            let preview_url = c["preview_url"]
                .as_str()
                .or_else(|| c["image"].as_str())
                .map(str::to_string);
            Some(GifCategory {
                name,
                query,
                preview_url,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use serde_json::json;

    #[tokio::test]
    #[ignore = "hits the live network and needs KLIPY_API_KEY; run explicitly with --ignored"]
    async fn resolves_a_live_klipy_gif_page() {
        let (resource, slug) = parse_media_page("https://klipy.com/gifs/nix-nixos").unwrap();
        let media = media_by_slug(resource, &slug).await.unwrap();

        assert!(media.image.url.starts_with("https://static.klipy.com/"));
        let (video, mime) = media.video.expect("klipy publishes an mp4");
        assert!(video.url.starts_with("https://static.klipy.com/"));
        assert_eq!(mime, "video/mp4");
    }

    #[test]
    fn parses_every_media_page_path() {
        for (raw, resource, slug) in [
            ("https://klipy.com/gifs/nix-nixos", "gifs", "nix-nixos"),
            (
                "https://www.klipy.com/stickers/happy-4",
                "stickers",
                "happy-4",
            ),
            (
                "https://klipy.com/clips/give-it-to-me",
                "clips",
                "give-it-to-me",
            ),
            ("https://klipy.com/static-memes/a_b", "static-memes", "a_b"),
            ("https://klipy.com/ai-gifs/x1", "ai-gifs", "x1"),
            ("https://klipy.com/emojis/y2/", "emojis", "y2"),
            (
                "https://klipy.com/gifs/nix-nixos?utm=1",
                "gifs",
                "nix-nixos",
            ),
        ] {
            assert_eq!(
                parse_media_page(raw),
                Some((resource, String::from(slug))),
                "{raw}"
            );
        }
    }

    #[test]
    fn rejects_urls_that_are_not_media_pages() {
        for raw in [
            "https://klipy.com/",
            "https://klipy.com/gifs",
            "https://klipy.com/explore/trending",
            "https://klipy.com/gifs/a/b",
            "https://klipy.com/gifs/..%2F..%2Fsecret",
            "https://klipy.com/gifs/bad!slug",
            "https://notklipy.com/gifs/nix-nixos",
            "https://klipy.com.evil.test/gifs/nix-nixos",
            "ftp://klipy.com/gifs/nix-nixos",
        ] {
            assert_eq!(parse_media_page(raw), None, "{raw}");
        }
    }

    #[test]
    fn normalizes_a_nested_media_item() {
        let item = json!({
            "slug": "nix-nixos",
            "title": "NixOS Rebuild Switch Heart Locket",
            "file": {
                "md": {
                    "gif": { "url": "https://static/md.gif", "width": 400, "height": 300 },
                    "mp4": { "url": "https://static/md.mp4", "width": 400, "height": 300 },
                    "webm": { "url": "https://static/md.webm" }
                }
            }
        });

        let media = normalize_media(&item).expect("usable media");
        assert_eq!(
            media.title.as_deref(),
            Some("NixOS Rebuild Switch Heart Locket")
        );
        assert_eq!(media.image.url, "https://static/md.gif");
        assert_eq!(media.image.width, Some(400));

        let (video, mime) = media.video.expect("video variant");
        assert_eq!(video.url, "https://static/md.mp4");
        assert_eq!(mime, "video/mp4");
    }

    #[test]
    fn normalizes_a_flat_clip_item() {
        let item = json!({
            "url": "https://klipy.com/clips/give-it-to-me",
            "slug": "give-it-to-me",
            "title": "Give It To Me",
            "file": {
                "mp4": "https://static/clip.mp4",
                "gif": "https://static/clip.gif",
                "webp": "https://static/clip.webp"
            },
            "file_meta": {
                "mp4": { "width": 480, "height": 360, "size": 231164 },
                "gif": { "width": 320, "height": 240, "size": 1427722 }
            }
        });

        let media = normalize_media(&item).expect("usable media");
        assert_eq!(media.image.url, "https://static/clip.gif");
        assert_eq!(media.image.width, Some(320));

        let (video, mime) = media.video.expect("video variant");
        assert_eq!(video.url, "https://static/clip.mp4");
        assert_eq!(video.width, Some(480));
        assert_eq!(mime, "video/mp4");
    }

    #[test]
    fn never_falls_back_to_the_page_url_as_media() {
        let item = json!({
            "url": "https://klipy.com/clips/give-it-to-me",
            "slug": "give-it-to-me",
            "file": {}
        });
        assert_eq!(normalize_media(&item), None);
    }

    #[test]
    fn prefers_an_animated_image_over_a_still_one() {
        let item = json!({
            "slug": "s",
            "file": { "md": { "jpg": { "url": "https://static/md.jpg" } } }
        });
        let media = normalize_media(&item).expect("usable media");
        assert_eq!(media.image.url, "https://static/md.jpg");
        assert!(media.video.is_none());

        let animated = json!({
            "slug": "s",
            "file": {
                "md": {
                    "jpg": { "url": "https://static/md.jpg" },
                    "gif": { "url": "https://static/md.gif" }
                }
            }
        });
        let media = normalize_media(&animated).expect("usable media");
        assert_eq!(media.image.url, "https://static/md.gif");
    }

    #[test]
    fn normalizes_a_paginated_search_page() {
        let body = json!({
            "result": true,
            "data": {
                "data": [{
                    "id": 123,
                    "slug": "happy-dance-abc",
                    "title": "happy dance",
                    "file": {
                        "hd": { "gif": { "url": "https://cdn/hd.gif", "width": 480, "height": 360 } },
                        "md": { "gif": { "url": "https://cdn/md.gif", "width": 320, "height": 240 } },
                        "sm": { "gif": { "url": "https://cdn/sm.gif", "width": 160, "height": 120 } },
                        "xs": { "jpg": { "url": "https://cdn/xs.jpg" } }
                    }
                }],
                "current_page": 2,
                "per_page": 24,
                "has_next": true
            }
        });

        let page = normalize_page(&body, 2);
        assert_eq!(page.page, 2);
        assert!(page.has_next);
        assert_eq!(page.items.len(), 1);

        let item = &page.items[0];
        assert_eq!(item.id, "happy-dance-abc");

        assert_eq!(item.media_url, "https://cdn/md.gif");
        assert_eq!(item.preview_url, "https://cdn/sm.gif");
        assert_eq!(item.width, Some(320));
        assert_eq!(item.height, Some(240));
    }

    #[test]
    fn falls_back_through_sizes_and_formats() {
        let body = json!({
            "data": { "data": [{
                "id": "only-id",
                "url": "https://cdn/fallback.gif",
                "file": { "xs": { "jpg": { "url": "https://cdn/xs.jpg" } } }
            }]}
        });
        let page = normalize_page(&body, 1);
        let item = &page.items[0];
        assert_eq!(item.id, "only-id");
        assert_eq!(item.media_url, "https://cdn/fallback.gif");
        assert_eq!(item.preview_url, "https://cdn/xs.jpg");
        assert!(!page.has_next);
        assert_eq!(page.page, 1);
    }

    #[test]
    fn skips_items_without_any_usable_media() {
        let body = json!({ "data": { "data": [{ "id": 1, "file": {} }] } });
        let page = normalize_page(&body, 1);
        assert!(page.items.is_empty());
    }

    #[test]
    fn normalizes_categories_from_data_categories() {
        let body = json!({
            "result": true,
            "data": {
                "locale": "en_US",
                "categories": [
                    { "category": "hello", "query": "hello", "preview_url": "https://cdn/hello.gif" },
                    { "category": "happy birthday", "query": "happy birthday" }
                ]
            }
        });
        let cats = normalize_categories(&body);
        assert_eq!(cats.len(), 2);
        assert_eq!(cats[0].name, "hello");
        assert_eq!(cats[0].query.as_deref(), Some("hello"));
        assert_eq!(
            cats[0].preview_url.as_deref(),
            Some("https://cdn/hello.gif")
        );
        assert_eq!(cats[1].name, "happy birthday");
        assert!(cats[1].preview_url.is_none());
    }

    #[test]
    fn empty_or_unexpected_body_yields_empty_results() {
        assert!(normalize_page(&json!({}), 1).items.is_empty());
        assert!(normalize_categories(&json!({ "data": "nope" })).is_empty());
    }
}
