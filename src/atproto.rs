use anyhow::{anyhow, Result};
use serde::Deserialize;
use tracing::warn;

// ── Public data types ─────────────────────────────────────────────────────────

pub struct ProfileData {
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

pub struct RawRecord {
    pub rkey: String,
    pub text: String,
    pub created_at: String,
    pub channel: String,
    pub parent: Option<String>,
}

pub struct RawReaction {
    pub rkey: String,
    pub emoji: String,
    pub target_rkey: String,
}

// ── Wire types ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BskyProfileResponse {
    display_name: Option<String>,
    avatar: Option<String>,
}

#[derive(Deserialize)]
struct ListRecordsResponse {
    records: Vec<RecordEntry>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct RecordEntry {
    uri: String,
    value: Option<ColibriRecord>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColibriRecord {
    // social.colibri.message fields
    text: Option<String>,
    created_at: Option<String>,
    channel: Option<String>,
    #[serde(default)]
    parent: Option<String>,
    // social.colibri.reaction fields
    emoji: Option<String>,
    target_message: Option<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Fetch a Bluesky actor profile from the public AppView API.
/// Returns `None` if the DID is not found or the request fails.
pub async fn fetch_profile(
    client: &reqwest::Client,
    did: &str,
) -> Result<Option<ProfileData>> {
    let url = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor={}",
        did
    );

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        warn!("fetch_profile: HTTP {} for {}", resp.status(), did);
        return Ok(None);
    }

    let body: BskyProfileResponse = resp.json().await?;
    Ok(Some(ProfileData {
        display_name: body.display_name,
        avatar_url: body.avatar,
    }))
}

/// Resolve a DID to its ATProto PDS service endpoint URL.
/// Supports `did:plc:` (via plc.directory) and `did:web:`.
pub async fn resolve_pds(client: &reqwest::Client, did: &str) -> Result<String> {
    let doc_url = if did.starts_with("did:plc:") {
        format!("https://plc.directory/{}", did)
    } else if let Some(domain) = did.strip_prefix("did:web:") {
        format!("https://{}/.well-known/did.json", domain)
    } else {
        return Err(anyhow!("Unsupported DID method: {}", did));
    };

    let doc: serde_json::Value = client.get(&doc_url).send().await?.json().await?;

    let services = doc["service"]
        .as_array()
        .ok_or_else(|| anyhow!("No 'service' array in DID document for {}", did))?;

    for svc in services {
        let id = svc["id"].as_str().unwrap_or("");
        if id == "#atproto_pds" || id.ends_with("#atproto_pds") {
            if let Some(endpoint) = svc["serviceEndpoint"].as_str() {
                return Ok(endpoint.to_string());
            }
        }
    }

    Err(anyhow!("No AtprotoPersonalDataServer found for {}", did))
}

/// List `social.colibri.message` records from a PDS for the given DID.
/// Returns the records and the next cursor (`None` if no more pages).
pub async fn list_message_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<(Vec<RawRecord>, Option<String>)> {
    let mut url = format!(
        "{}/xrpc/com.atproto.repo.listRecords?repo={}&collection=social.colibri.message&limit=100",
        pds_url.trim_end_matches('/'),
        did
    );
    if let Some(c) = cursor {
        url.push_str("&cursor=");
        url.push_str(c);
    }

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "list_message_records: HTTP {} for {}",
            resp.status(),
            did
        ));
    }

    let body: ListRecordsResponse = resp.json().await?;

    let records = body
        .records
        .into_iter()
        .filter_map(|entry| {
            // URI format: at://did/collection/rkey
            let rkey = entry.uri.rsplit('/').next()?.to_string();
            let val = entry.value?;
            Some(RawRecord {
                rkey,
                text: val.text?,
                created_at: val.created_at?,
                channel: val.channel?,
                parent: val.parent,
            })
        })
        .collect();

    Ok((records, body.cursor))
}

/// List `social.colibri.reaction` records from a PDS for the given DID.
pub async fn list_reaction_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<(Vec<RawReaction>, Option<String>)> {
    let mut url = format!(
        "{}/xrpc/com.atproto.repo.listRecords?repo={}&collection=social.colibri.reaction&limit=100",
        pds_url.trim_end_matches('/'),
        did
    );
    if let Some(c) = cursor {
        url.push_str("&cursor=");
        url.push_str(c);
    }

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "list_reaction_records: HTTP {} for {}",
            resp.status(),
            did
        ));
    }

    let body: ListRecordsResponse = resp.json().await?;

    let records = body
        .records
        .into_iter()
        .filter_map(|entry| {
            let rkey = entry.uri.rsplit('/').next()?.to_string();
            let val = entry.value?;
            // Reactions use emoji + targetMessage fields
            let emoji = val.emoji?;
            let target_rkey = val.target_message?;
            Some(RawReaction { rkey, emoji, target_rkey })
        })
        .collect();

    Ok((records, body.cursor))
}
