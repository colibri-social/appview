use std::time::Duration;

use reqwest::Client;
use rocket::serde::json::Json;
use serde::Serialize;
use serde_json::Value;

use crate::lib::responses::{ErrorBody, ErrorResponse};

const KLIPY_BASE: &str = "https://api.klipy.com/api/v1";
const FETCH_TIMEOUT: Duration = Duration::from_secs(6);
pub const DEFAULT_PER_PAGE: u32 = 24;

const MEDIA_SIZES: [&str; 4] = ["md", "hd", "sm", "xs"];
const PREVIEW_SIZES: [&str; 4] = ["sm", "xs", "md", "hd"];

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
    ErrorResponse {
        body: Json(ErrorBody {
            error: "UpstreamError".into(),
            message: msg.into(),
        }),
    }
}

fn api_key() -> Result<String, ErrorResponse> {
    match std::env::var("KLIPY_API_KEY") {
        Ok(k) if !k.trim().is_empty() => Ok(k),
        _ => Err(upstream("KLIPY_API_KEY is not configured")),
    }
}

async fn get_json(path: &str, query: &[(&str, String)]) -> Result<Value, ErrorResponse> {
    let key = api_key()?;
    let base = format!("{KLIPY_BASE}/{key}/gifs/{path}");
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
    let body = get_json("categories", &[]).await?;
    Ok(normalize_categories(&body))
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
    use serde_json::json;

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
