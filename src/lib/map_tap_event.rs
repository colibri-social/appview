use crate::lib::colibri::{
    ColibriCategory, ColibriChannel, ColibriCommunity, ColibriMessage, ColibriReaction,
};
use crate::lib::events::{
    CategoryEventData, ChannelEventData, ColibriServerEvent, ColibriServerEventData,
    CommunityEventData, MessageEventData, ReactionEventData,
};
use crate::lib::tap::TapMessageRecord;
use serde::de::Error;

pub fn map_tap_event(
    event_record: &TapMessageRecord,
) -> Result<ColibriServerEvent, serde_json::Error> {
    let uri = format!(
        "at://{}/social.colibri.community/{}",
        event_record.did, event_record.rkey
    );

    match event_record.collection.as_str() {
        "social.colibri.community" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriCommunity>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::CommunityEventData(
                        CommunityEventData {
                            event: String::from("upsert"),
                            uri,
                            category_order: Some(record_data.category_order),
                            description: Some(record_data.description),
                            name: Some(record_data.name),
                            picture: record_data.picture,
                        },
                    )),
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("community_event"),
                    data: Some(ColibriServerEventData::CommunityEventData(
                        CommunityEventData {
                            event: String::from("delete"),
                            uri,
                            category_order: None,
                            description: None,
                            name: None,
                            picture: None,
                        },
                    )),
                })
            }
        }
        "social.colibri.membership" | "social.colibri.approval" => {
            // TODO: Membership tracking
            Err(serde_json::error::Error::custom(String::from(
                "Not implemented",
            )))
        }
        "social.colibri.category" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriCategory>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                Ok(ColibriServerEvent {
                    event_type: String::from("category_event"),
                    data: Some(ColibriServerEventData::CategoryEventData(
                        CategoryEventData {
                            event: String::from("upsert"),
                            uri,
                            channel_order: Some(record_data.channel_order),
                            community: Some(record_data.community),
                            name: Some(record_data.name),
                        },
                    )),
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("category_event"),
                    data: Some(ColibriServerEventData::CategoryEventData(
                        CategoryEventData {
                            event: String::from("delete"),
                            uri,
                            channel_order: None,
                            community: None,
                            name: None,
                        },
                    )),
                })
            }
        }
        "social.colibri.channel" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriChannel>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                Ok(ColibriServerEvent {
                    event_type: String::from("channel_event"),
                    data: Some(ColibriServerEventData::ChannelEventData(ChannelEventData {
                        event: String::from("upsert"),
                        uri,
                        channel_type: Some(record_data.channel_type),
                        community: Some(record_data.community),
                        description: record_data.description,
                        name: Some(record_data.name),
                    })),
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("channel_event"),
                    data: Some(ColibriServerEventData::ChannelEventData(ChannelEventData {
                        event: String::from("upsert"),
                        uri,
                        channel_type: None,
                        community: None,
                        description: None,
                        name: None,
                    })),
                })
            }
        }
        "social.colibri.message" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriMessage>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                Ok(ColibriServerEvent {
                    event_type: String::from("message_event"),
                    data: Some(ColibriServerEventData::MessageEventData(MessageEventData {
                        event: String::from("upsert"),
                        uri,
                        attachments: record_data.attachments,
                        channel: Some(record_data.channel),
                        created_at: Some(record_data.created_at),
                        edited: record_data.edited,
                        facets: record_data.facets,
                        parent: record_data.parent,
                        text: Some(record_data.text),
                    })),
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("message_event"),
                    data: Some(ColibriServerEventData::MessageEventData(MessageEventData {
                        event: String::from("delete"),
                        uri,
                        attachments: None,
                        channel: None,
                        created_at: None,
                        edited: None,
                        facets: None,
                        parent: None,
                        text: None,
                    })),
                })
            }
        }
        "social.colibri.reaction" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriReaction>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::ReactionEventData(
                        ReactionEventData {
                            event: String::from("added"),
                            uri,
                            emoji: Some(record_data.emoji),
                            target: Some(record_data.target_message),
                        },
                    )),
                })
            } else {
                Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::ReactionEventData(
                        ReactionEventData {
                            event: String::from("removed"),
                            uri,
                            emoji: None,
                            target: None,
                        },
                    )),
                })
            }
        }
        "social.colibri.actor.data" | "app.bsky.actor.profile" => {
            // TODO: Fetch other half of profile, send full event
            Err(serde_json::error::Error::custom(String::from(
                "Not implemented",
            )))
        }
        "social.colibri.richtext.facet" => {
            Err(serde_json::error::Error::custom(String::from("Facet")))
        }
        _ => Err(serde_json::error::Error::custom(String::from(
            "Unknown collection",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(collection: &str, action: &str, value: serde_json::Value) -> TapMessageRecord {
        TapMessageRecord {
            live: true,
            did: String::from("did:plc:abc"),
            rev: String::from("1"),
            collection: collection.to_string(),
            rkey: String::from("r1"),
            action: action.to_string(),
            record: Some(value),
            cid: Some(String::from("cid")),
        }
    }

    #[test]
    fn maps_community_upsert_event() {
        let event = map_tap_event(&record(
            "social.colibri.community",
            "create",
            serde_json::json!({
                "$type": "social.colibri.community",
                "name": "General",
                "description": "desc",
                "categoryOrder": ["cat1"],
                "requiresApprovalToJoin": false
            }),
        ))
        .unwrap();

        assert_eq!(event.event_type, "community_event");
    }

    #[test]
    fn maps_message_delete_event() {
        let event = map_tap_event(&record(
            "social.colibri.message",
            "delete",
            serde_json::json!({
                "$type":"social.colibri.message",
                "text":"ignored",
                "createdAt":"2024-01-01T00:00:00Z",
                "channel":"c1"
            }),
        ))
        .unwrap();

        if let Some(crate::lib::events::ColibriServerEventData::MessageEventData(data)) = event.data
        {
            assert_eq!(data.event, "delete");
            assert_eq!(data.text, None);
        } else {
            panic!("expected message event data");
        }
    }

    #[test]
    fn returns_error_for_unknown_collection() {
        let err = map_tap_event(&TapMessageRecord {
            live: true,
            did: String::from("did:plc:abc"),
            rev: String::from("1"),
            collection: String::from("unknown.collection"),
            rkey: String::from("r1"),
            action: String::from("create"),
            record: Some(serde_json::json!({})),
            cid: Some(String::from("cid")),
        })
        .unwrap_err();

        assert_eq!(err.to_string(), "Unknown collection");
    }
}
