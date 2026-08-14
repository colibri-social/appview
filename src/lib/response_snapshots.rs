//! Pins the serialised shape of the responses the AppView puts on the wire.
//!
//! Two things are asserted per response:
//!
//! 1. It still serialises byte-for-byte to `tests/fixtures/responses/<nsid>.json`,
//!    so an accidental serde attribute change (a dropped `rename`, a new
//!    `skip_serializing_if`) fails here rather than in a client six weeks later.
//! 2. Every property the lexicon marks `required` on that method's output is
//!    actually present. The vendored lexicons are the same ones
//!    `tests/lexicons.rs` reads; when a method has no vendored copy the check
//!    is skipped, matching the skip-on-absence rule used there.
//!
//! Regenerate the fixtures with `UPDATE_FIXTURES=1 cargo test`, then read the
//! diff before committing it. The required-key assertion runs either way, so a
//! regenerated fixture cannot launder away a missing field.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::lib::reactions::ReactionSummary;
use crate::xrpc::social::colibri::actor::get_data_handler::{Actor, ActorData, ActorStatus};
use crate::xrpc::social::colibri::channel::list_messages_handler::{
    Attachment, Message, MessageAuthor, MessageList, ParentMessage,
};
use crate::xrpc::social::colibri::community::reads::list_channels_handler::{Channel, ChannelList};

fn crate_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn fixture_path(nsid: &str) -> PathBuf {
    crate_root()
        .join("tests/fixtures/responses")
        .join(format!("{nsid}.json"))
}

fn updating_fixtures() -> bool {
    std::env::var("UPDATE_FIXTURES").is_ok_and(|value| !value.trim().is_empty())
}

/// Loads a vendored lexicon, or `None` when the AppView is running ahead of
/// the last sync and the method has no vendored copy yet.
fn vendored_lexicon(nsid: &str) -> Option<Value> {
    let path = crate_root().join("lexicons").join(format!("{nsid}.json"));
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Resolves an output schema that is itself a `ref` into the definition it
/// points at, so `{"type":"ref","ref":"lex:…#actorView"}` yields the object.
fn resolve_schema(nsid: &str, schema: &Value) -> Option<Value> {
    if schema.get("type").and_then(Value::as_str) != Some("ref") {
        return Some(schema.clone());
    }

    let target = schema.get("ref")?.as_str()?.trim_start_matches("lex:");
    let (doc_id, fragment) = match target.split_once('#') {
        Some(("", fragment)) => (nsid, fragment),
        Some((doc_id, fragment)) => (doc_id, fragment),
        None => (target, "main"),
    };

    let doc = vendored_lexicon(doc_id)?;
    doc.get("defs")?.get(fragment).cloned()
}

/// Asserts every property the lexicon marks `required` on this method's output
/// is present in the serialised response.
fn assert_required_output_keys(nsid: &str, value: &Value) {
    let Some(doc) = vendored_lexicon(nsid) else {
        return;
    };

    let Some(schema) = doc
        .get("defs")
        .and_then(|defs| defs.get("main"))
        .and_then(|main| main.get("output"))
        .and_then(|output| output.get("schema"))
    else {
        return;
    };

    let Some(resolved) = resolve_schema(nsid, schema) else {
        panic!("{nsid}: the output schema refs a definition that is not vendored");
    };

    if resolved.get("type").and_then(Value::as_str) != Some("object") {
        return;
    }

    let Some(required) = resolved.get("required").and_then(Value::as_array) else {
        return;
    };

    let missing: Vec<&str> = required
        .iter()
        .filter_map(Value::as_str)
        .filter(|key| value.get(key).is_none())
        .collect();

    assert!(
        missing.is_empty(),
        "{nsid}: the response omits properties the lexicon requires: {missing:?}"
    );
}

fn assert_response_snapshot<T: Serialize>(nsid: &str, response: &T) {
    let value = serde_json::to_value(response).expect("the response must serialise");
    assert_required_output_keys(nsid, &value);

    let rendered = format!(
        "{}\n",
        serde_json::to_string_pretty(&value).expect("the response must render")
    );
    let path = fixture_path(nsid);

    if updating_fixtures() {
        let parent = path.parent().expect("the fixture path must have a parent");
        std::fs::create_dir_all(parent).expect("the fixture directory must be creatable");
        std::fs::write(&path, &rendered).expect("the fixture must be writable");
        return;
    }

    let stored = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "{nsid} has no response fixture at {}. Run `UPDATE_FIXTURES=1 cargo test`, then review the diff before committing it.",
            path.display()
        )
    });

    assert_eq!(
        stored, rendered,
        "the serialised {nsid} response no longer matches its fixture. If the change is deliberate, run `UPDATE_FIXTURES=1 cargo test` and review the diff."
    );
}

fn blob(cid: &str, mime_type: &str, size: u64) -> Value {
    serde_json::json!({
        "$type": "blob",
        "ref": { "$link": cid },
        "mimeType": mime_type,
        "size": size,
    })
}

fn actor_data() -> ActorData {
    ActorData {
        display_name: String::from("Smoke Tester"),
        avatar: Some(blob(
            "bafkreiavatar000000000000000000000000000000000000000000000",
            "image/png",
            1024,
        )),
        banner: None,
        description: Some(String::from("A fixture actor.")),
        is_bot: false,
        online_state: String::from("online"),
        sync_bluesky: false,
        theme: None,
        status: ActorStatus {
            text: String::from("Pinning fixtures"),
            emoji: Some(String::from("📌")),
        },
        preferred_badge: None,
    }
}

fn message_author() -> MessageAuthor {
    MessageAuthor {
        did: String::from("did:plc:fixtureauthor00000000000"),
        handle: String::from("author.test"),
        data: actor_data(),
    }
}

fn bold_facet() -> Value {
    serde_json::json!({
        "index": { "byteStart": 0, "byteEnd": 5 },
        "features": [{ "$type": "social.colibri.richtext.facet#bold" }],
    })
}

#[test]
fn list_messages_response_matches_its_fixture() {
    let response = MessageList {
        cursor: Some(String::from("3lkfixturecursor")),
        messages: vec![Message {
            suppressed_embeds: vec![],
            mod_suppressed_embeds: vec![],
            uri: String::from(
                "at://did:plc:fixtureauthor00000000000/social.colibri.message/3lkfixture0001",
            ),
            text: String::from("Hello from a fixture"),
            facets: vec![bold_facet()],
            channel: String::from(
                "at://did:plc:fixturecommunity000000000/social.colibri.channel/general",
            ),
            community: String::from(
                "at://did:plc:fixturecommunity000000000/social.colibri.community/self",
            ),
            author: message_author(),
            parent: Some(ParentMessage {
                suppressed_embeds: vec![],
                mod_suppressed_embeds: vec![],
                uri: String::from(
                    "at://did:plc:fixtureauthor00000000000/social.colibri.message/3lkfixture0000",
                ),
                text: String::from("The message being replied to"),
                facets: vec![],
                channel: String::from(
                    "at://did:plc:fixturecommunity000000000/social.colibri.channel/general",
                ),
                community: String::from(
                    "at://did:plc:fixturecommunity000000000/social.colibri.community/self",
                ),
                author: message_author(),
                attachments: vec![],
                reactions: vec![],
                created_at: String::from("2026-01-01T00:00:00.000Z"),
                edited: false,
            }),
            attachments: vec![Attachment {
                blob: blob(
                    "bafkreiattachment0000000000000000000000000000000000000000",
                    "image/webp",
                    4096,
                ),
                name: Some(String::from("screenshot.webp")),
                width: Some(1280),
                height: Some(720),
            }],
            reactions: vec![ReactionSummary {
                emoji: String::from("🎉"),
                count: 2,
                reactor_dids: vec![
                    String::from("did:plc:fixturereactor0000000000"),
                    String::from("did:plc:fixtureauthor00000000000"),
                ],
            }],
            created_at: String::from("2026-01-01T00:01:00.000Z"),
            edited: true,
        }],
    };

    assert_response_snapshot("social.colibri.channel.listMessages", &response);
}

#[test]
fn get_actor_data_response_matches_its_fixture() {
    let response = Actor {
        did: String::from("did:plc:fixtureauthor00000000000"),
        handle: String::from("author.test"),
        data: actor_data(),
    };

    assert_response_snapshot("social.colibri.actor.getData", &response);
}

#[test]
fn list_channels_response_matches_its_fixture() {
    let response = ChannelList {
        channels: vec![
            Channel {
                link_embeds: None,
                uri: String::from(
                    "at://did:plc:fixturecommunity000000000/social.colibri.channel/general",
                ),
                name: String::from("general"),
                channel_type: String::from("social.colibri.channel.text"),
                category: String::from(
                    "at://did:plc:fixturecommunity000000000/social.colibri.category/main",
                ),
                description: Some(String::from("The default channel.")),
                owner_only: None,
                allowed_roles: vec![],
                allowed_members: vec![],
            },
            Channel {
                link_embeds: None,
                uri: String::from(
                    "at://did:plc:fixturecommunity000000000/social.colibri.channel/staff",
                ),
                name: String::from("staff"),
                channel_type: String::from("social.colibri.channel.text"),
                category: String::from(
                    "at://did:plc:fixturecommunity000000000/social.colibri.category/main",
                ),
                description: None,
                owner_only: Some(true),
                allowed_roles: vec![String::from("moderator")],
                allowed_members: vec![String::from("did:plc:fixtureauthor00000000000")],
            },
        ],
    };

    assert_response_snapshot("social.colibri.community.listChannels", &response);
}
