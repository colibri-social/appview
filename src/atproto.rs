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
    pub facets: Option<serde_json::Value>,
}

pub struct RawReaction {
    pub rkey: String,
    pub emoji: String,
    pub target_rkey: String,
}

pub struct RawCommunity {
    pub rkey: String,
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
}

pub struct RawChannel {
    pub rkey: String,
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
    pub channel_type: String,
    pub category_rkey: Option<String>,
    /// rkey of the community on the same PDS.
    pub community_rkey: String,
}

pub struct RawMembership {
    pub rkey: String,
    pub uri: String,
    /// AT-URI of the community the member wants to join.
    pub community_uri: String,
}

pub struct RawApproval {
    pub rkey: String,
    pub uri: String,
    /// AT-URI of the member's social.colibri.membership record.
    pub membership_uri: String,
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
    #[serde(default)]
    facets: Option<serde_json::Value>,
    // social.colibri.reaction fields
    emoji: Option<String>,
    // social.colibri.community / social.colibri.channel
    name: Option<String>,
    description: Option<String>,
    #[serde(rename = "type")]
    channel_type: Option<String>,
    category: Option<String>,
    /// Used as community rkey in channel records, and as community AT-URI in membership records.
    community: Option<String>,
    // social.colibri.approval fields
    membership: Option<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Fetch a Bluesky actor profile from the public AppView API.
/// Returns `None` if the DID is not found or the request fails.
pub async fn fetch_profile(client: &reqwest::Client, did: &str) -> Result<Option<ProfileData>> {
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
                facets: val.facets,
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
            // Reactions use emoji + targetMessage fields.
            // targetMessage is an AT-URI (at://did/collection/rkey); extract just the rkey.
            let emoji = val.emoji?;
            let target_rkey = val
                .parent?
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_string();
            if target_rkey.is_empty() {
                return None;
            }
            Some(RawReaction {
                rkey,
                emoji,
                target_rkey,
            })
        })
        .collect();

    Ok((records, body.cursor))
}

/// List `social.colibri.community` records from a PDS for the given DID.
pub async fn list_community_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<(Vec<RawCommunity>, Option<String>)> {
    let body = fetch_records(client, pds_url, did, "social.colibri.community", cursor).await?;
    let records = body
        .records
        .into_iter()
        .filter_map(|entry| {
            let rkey = entry.uri.rsplit('/').next()?.to_string();
            let uri = entry.uri;
            let val = entry.value?;
            Some(RawCommunity {
                rkey,
                uri,
                name: val.name?,
                description: val.description,
            })
        })
        .collect();
    Ok((records, body.cursor))
}

/// List `social.colibri.channel` records from a PDS for the given DID.
pub async fn list_channel_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<(Vec<RawChannel>, Option<String>)> {
    let body = fetch_records(client, pds_url, did, "social.colibri.channel", cursor).await?;
    let records = body
        .records
        .into_iter()
        .filter_map(|entry| {
            let rkey = entry.uri.rsplit('/').next()?.to_string();
            let uri = entry.uri;
            let val = entry.value?;
            Some(RawChannel {
                rkey,
                uri,
                name: val.name?,
                description: val.description,
                channel_type: val.channel_type.unwrap_or_else(|| "text".to_string()),
                category_rkey: val.category,
                community_rkey: val.community?,
            })
        })
        .collect();
    Ok((records, body.cursor))
}

/// List `social.colibri.membership` records from a PDS for the given DID.
pub async fn list_membership_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<(Vec<RawMembership>, Option<String>)> {
    let body = fetch_records(client, pds_url, did, "social.colibri.membership", cursor).await?;
    let records = body
        .records
        .into_iter()
        .filter_map(|entry| {
            let rkey = entry.uri.rsplit('/').next()?.to_string();
            let uri = entry.uri;
            let val = entry.value?;
            Some(RawMembership {
                rkey,
                uri,
                community_uri: val.community?,
            })
        })
        .collect();
    Ok((records, body.cursor))
}

/// List `social.colibri.approval` records from a PDS for the given DID.
pub async fn list_approval_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<(Vec<RawApproval>, Option<String>)> {
    let body = fetch_records(client, pds_url, did, "social.colibri.approval", cursor).await?;
    let records = body
        .records
        .into_iter()
        .filter_map(|entry| {
            let rkey = entry.uri.rsplit('/').next()?.to_string();
            let uri = entry.uri;
            let val = entry.value?;
            Some(RawApproval {
                rkey,
                uri,
                membership_uri: val.membership?,
            })
        })
        .collect();
    Ok((records, body.cursor))
}

// ── Shared fetch helper ───────────────────────────────────────────────────────

async fn fetch_records(
    client: &reqwest::Client,
    pds_url: &str,
    did: &str,
    collection: &str,
    cursor: Option<&str>,
) -> Result<ListRecordsResponse> {
    let mut url = format!(
        "{}/xrpc/com.atproto.repo.listRecords?repo={}&collection={}&limit=100",
        pds_url.trim_end_matches('/'),
        did,
        collection
    );
    if let Some(c) = cursor {
        url.push_str("&cursor=");
        url.push_str(c);
    }
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "fetch_records({}): HTTP {} for {}",
            collection,
            resp.status(),
            did
        ));
    }
    Ok(resp.json().await?)
}
