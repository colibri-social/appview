//! Web Push delivery for mention/reply notifications.
//!
//! When the tap pipeline indexes a notification it hands it here; we fan a
//! VAPID Web Push out to every subscription the recipient has registered, so
//! the notification arrives even when the app is fully closed. Delivery is
//! filtered to match the in-app rules:
//!
//! - **Pings only** — only `mention`/`reply` notifications exist, so every row
//!   that reaches us is a ping.
//! - **Mutes** — skipped if the recipient muted the channel or its community.
//! - **Do Not Disturb** — skipped while the recipient's state is `dnd`.
//!
//! The `web-push` crate builds and encrypts the message (RFC 8291 aes128gcm)
//! and signs the VAPID JWT; we send the resulting request with the existing
//! `reqwest` client rather than pulling in a second HTTP/TLS stack.

use sea_orm::DatabaseConnection;
use serde::Serialize;
use web_push::{ContentEncoding, SubscriptionInfo, VapidSignatureBuilder, WebPushMessageBuilder};

use crate::lib::at_uri::AtUri;
use crate::lib::get_state::get_state;
use crate::lib::notifications::IndexedNotification;
use crate::lib::push_subscriptions;
use crate::lib::vapid_config::vapid_config;
use crate::models::record_data;
use crate::xrpc::social::colibri::actor::list_mutes_handler::fetch_mutes;
use crate::xrpc::social::colibri::actor::set_state_handler::UserState;

const COMMUNITY_NSID: &str = "social.colibri.community";
const FACET_SPOILER_TYPE: &str = "social.colibri.richtext.facet#spoiler";

/// Replaces every spoiler-covered range of `text` with a placeholder so hidden
/// content never appears in a push preview
fn redact_spoilers(text: &str, facets: &[serde_json::Value]) -> String {
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for facet in facets {
        let is_spoiler = facet
            .get("features")
            .and_then(|f| f.as_array())
            .is_some_and(|features| {
                features.iter().any(|feature| {
                    feature.get("$type").and_then(|t| t.as_str()) == Some(FACET_SPOILER_TYPE)
                })
            });
        if !is_spoiler {
            continue;
        }
        let index = facet.get("index");
        let start = index
            .and_then(|i| i.get("byteStart"))
            .and_then(serde_json::Value::as_u64);
        let end = index
            .and_then(|i| i.get("byteEnd"))
            .and_then(serde_json::Value::as_u64);
        let (Some(start), Some(end)) = (start, end) else {
            continue;
        };
        let (start, end) = (start as usize, end as usize);
        if start < end
            && end <= text.len()
            && text.is_char_boundary(start)
            && text.is_char_boundary(end)
        {
            ranges.push((start, end));
        }
    }
    if ranges.is_empty() {
        return text.to_string();
    }
    ranges.sort_by_key(|&(start, _)| start);

    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    for (start, end) in ranges {
        // Skip ranges that overlap one already redacted (nested/duplicate)
        if start < cursor {
            continue;
        }
        out.push_str(&text[cursor..start]);
        out.push_str("█████");
        cursor = end;
    }
    out.push_str(&text[cursor..]);
    out
}

/// Routing hints the Service Worker uses on click.
#[derive(Serialize)]
struct PushData {
    #[serde(rename = "channelUri")]
    channel_uri: String,
    #[serde(rename = "messageUri")]
    message_uri: String,
}

/// JSON push payload, shaped for `push-sw.js`.
#[derive(Serialize)]
struct PushPayload {
    title: String,
    body: String,
    tag: String,
    data: PushData,
}

/// Whether `channel_uri` is muted by the given mute records. Mirrors the
/// client's `Mutes` logic: a mute matches when its subject is the channel
/// itself, or is the community (same authority DID) that owns the channel —
/// a community mute suppresses all of its channels.
pub fn is_muted(mutes: &[record_data::Model], channel_uri: &str) -> bool {
    let channel_authority = AtUri::parse(channel_uri).map(|u| u.authority);
    for mute in mutes {
        let Some(subject) = mute.data.get("subject").and_then(|s| s.as_str()) else {
            continue;
        };
        if subject == channel_uri {
            return true;
        }
        if let Some(parsed) = AtUri::parse(subject)
            && parsed.collection == COMMUNITY_NSID
            && channel_authority.as_deref() == Some(parsed.authority.as_str())
        {
            return true;
        }
    }
    false
}

/// Applies the DND + mute filters and, if the notification passes, delivers a
/// Web Push to each of the recipient's subscriptions. Best-effort: every error
/// is logged and swallowed so a failed push never disrupts the tap pipeline.
/// Intended to be `tokio::spawn`ed from the tap loop.
pub async fn deliver(db: DatabaseConnection, notification: IndexedNotification) {
    // No VAPID keypair → Web Push is disabled; nothing to do.
    if vapid_config().is_none() {
        return;
    }

    let recipient_did = notification.row.recipient_did.clone();
    let channel_uri = notification.row.channel_uri.clone();

    // Do Not Disturb: the recipient asked not to be interrupted.
    match get_state(recipient_did.clone(), &db).await {
        Ok(UserState::Dnd) => return,
        Ok(_) => {}
        Err(e) => {
            log::warn!("push: failed to read state for {recipient_did}: {e}");
            // Fail open — a missing state shouldn't silently drop notifications.
        }
    }

    // Mutes: skip if the recipient muted the channel or its community.
    match fetch_mutes(&db, &recipient_did).await {
        Ok(mutes) if is_muted(&mutes, &channel_uri) => return,
        Ok(_) => {}
        Err(e) => {
            log::warn!("push: failed to read mutes for {recipient_did}: {e}");
        }
    }

    let title = match notification.row.kind.as_str() {
        "reply" => String::from("New reply"),
        _ => match notification.row.mention_role_name.as_deref() {
            Some(role) => format!("Mentioned via @{role}"),
            None => String::from("New mention"),
        },
    };
    let payload = PushPayload {
        title,
        body: redact_spoilers(&notification.message.text, &notification.message.facets),
        tag: notification.row.message_uri.clone(),
        data: PushData {
            channel_uri: channel_uri.clone(),
            message_uri: notification.row.message_uri.clone(),
        },
    };

    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            log::warn!("push: failed to serialize payload: {e}");
            return;
        }
    };

    let subscriptions = match push_subscriptions::list_for_actor(&db, &recipient_did).await {
        Ok(subs) => subs,
        Err(e) => {
            log::warn!("push: failed to list subscriptions for {recipient_did}: {e}");
            return;
        }
    };

    for sub in subscriptions {
        if let Err(e) = send_one(&db, &sub, &body).await {
            log::warn!("push: delivery to {} failed: {e}", sub.endpoint);
        }
    }
}

/// Builds the encrypted/signed request for a single subscription and sends it.
/// On a `404`/`410` from the push service the subscription is pruned. The
/// outbound request is validated and pinned the same way `embed_fetch` guards
/// other outbound fetches — the endpoint is caller-supplied at registration
/// time, so it's treated as untrusted here too.
async fn send_one(
    db: &DatabaseConnection,
    sub: &crate::models::push_subscriptions::Model,
    body: &[u8],
) -> Result<(), String> {
    let config = vapid_config().ok_or("vapid not configured")?;

    let subscription_info = SubscriptionInfo::new(&sub.endpoint, &sub.p256dh, &sub.auth);

    let mut sig_builder =
        VapidSignatureBuilder::from_base64(&config.private_key, &subscription_info)
            .map_err(|e| format!("vapid signature: {e}"))?;
    sig_builder.add_claim("sub", config.subject.clone());
    let signature = sig_builder
        .build()
        .map_err(|e| format!("vapid build: {e}"))?;

    let mut builder = WebPushMessageBuilder::new(&subscription_info);
    builder.set_payload(ContentEncoding::Aes128Gcm, body);
    builder.set_vapid_signature(signature);
    let message = builder.build().map_err(|e| format!("message build: {e}"))?;

    let payload = message.payload.ok_or("web push message had no payload")?;

    let content_encoding = match payload.content_encoding {
        ContentEncoding::Aes128Gcm => "aes128gcm",
        ContentEncoding::AesGcm => "aesgcm",
    };

    let (client, url) = crate::lib::embed_fetch::guarded_client_for(&message.endpoint.to_string())
        .await
        .map_err(|e| format!("endpoint validation: {e}"))?;

    let mut req = client
        .post(url)
        .header("TTL", message.ttl.to_string())
        .header("Content-Encoding", content_encoding);
    for (name, value) in &payload.crypto_headers {
        req = req.header(*name, value);
    }

    let res = req
        .body(payload.content)
        .send()
        .await
        .map_err(|e| format!("request: {e}"))?;

    let status = res.status();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::GONE {
        // The push service says this endpoint is dead — drop it so we stop
        // trying.
        let _ = push_subscriptions::delete_by_endpoint(db, &sub.endpoint).await;
    } else if !status.is_success() {
        return Err(format!("push service returned {status}"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mute(subject: &str) -> record_data::Model {
        record_data::Model {
            id: 1,
            did: String::from("did:plc:me"),
            nsid: String::from("social.colibri.actor.mute"),
            rkey: String::from("r"),
            data: json!({ "subject": subject }),
            indexed_at: String::from("2026-06-25T00:00:00.000Z"),
        }
    }

    const CHANNEL: &str = "at://did:plc:community/social.colibri.channel.text/general";

    #[test]
    fn channel_mute_matches_exact_channel() {
        assert!(is_muted(&[mute(CHANNEL)], CHANNEL));
    }

    #[test]
    fn community_mute_matches_any_channel_in_it() {
        let community = "at://did:plc:community/social.colibri.community/self";
        assert!(is_muted(&[mute(community)], CHANNEL));
    }

    #[test]
    fn unrelated_mute_does_not_match() {
        let other = "at://did:plc:other/social.colibri.community/self";
        assert!(!is_muted(&[mute(other)], CHANNEL));
        let other_channel = "at://did:plc:community/social.colibri.channel.text/random";
        assert!(!is_muted(&[mute(other_channel)], CHANNEL));
    }

    #[test]
    fn empty_mutes_never_match() {
        assert!(!is_muted(&[], CHANNEL));
    }

    fn spoiler(byte_start: u64, byte_end: u64) -> serde_json::Value {
        json!({
            "index": { "byteStart": byte_start, "byteEnd": byte_end },
            "features": [{ "$type": FACET_SPOILER_TYPE }],
        })
    }

    #[test]
    fn redacts_spoiler_range() {
        // "psst secret bye" — spoiler covers "secret" at bytes [5, 11).
        assert_eq!(
            redact_spoilers("psst secret bye", &[spoiler(5, 11)]),
            "psst █████ bye"
        );
    }

    #[test]
    fn leaves_text_without_spoilers_untouched() {
        let facets = vec![json!({
            "index": { "byteStart": 0, "byteEnd": 4 },
            "features": [{ "$type": "social.colibri.richtext.facet#bold" }],
        })];
        assert_eq!(redact_spoilers("bold text", &facets), "bold text");
        assert_eq!(redact_spoilers("plain", &[]), "plain");
    }

    #[test]
    fn redacts_multiple_and_respects_char_boundaries() {
        // "café x y" — "café " is 6 bytes (é is 2); spoiler over "x" [6,7) and
        // "y" [8,9).
        assert_eq!(
            redact_spoilers("café x y", &[spoiler(6, 7), spoiler(8, 9)]),
            "café █████ █████"
        );
    }

    #[test]
    fn ignores_out_of_bounds_or_misaligned_ranges() {
        // é spans bytes [3,5); a range ending at 4 is mid-char → left as-is.
        assert_eq!(redact_spoilers("café", &[spoiler(3, 4)]), "café");
        assert_eq!(redact_spoilers("hi", &[spoiler(0, 99)]), "hi");
    }
}
