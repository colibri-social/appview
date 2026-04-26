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

                return Ok(ColibriServerEvent {
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
                });
            } else {
                return Ok(ColibriServerEvent {
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
                });
            }
        }
        "social.colibri.membership" | "social.colibri.approval" => {
            // TODO: Membership tracking
            return Err(serde_json::error::Error::custom(String::from(
                "Not implemented",
            )));
        }
        "social.colibri.category" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriCategory>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                return Ok(ColibriServerEvent {
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
                });
            } else {
                return Ok(ColibriServerEvent {
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
                });
            }
        }
        "social.colibri.channel" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriChannel>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                return Ok(ColibriServerEvent {
                    event_type: String::from("channel_event"),
                    data: Some(ColibriServerEventData::ChannelEventData(ChannelEventData {
                        event: String::from("upsert"),
                        uri,
                        channel_type: Some(record_data.channel_type),
                        community: Some(record_data.community),
                        description: record_data.description,
                        name: Some(record_data.name),
                    })),
                });
            } else {
                return Ok(ColibriServerEvent {
                    event_type: String::from("channel_event"),
                    data: Some(ColibriServerEventData::ChannelEventData(ChannelEventData {
                        event: String::from("upsert"),
                        uri,
                        channel_type: None,
                        community: None,
                        description: None,
                        name: None,
                    })),
                });
            }
        }
        "social.colibri.message" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriMessage>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                return Ok(ColibriServerEvent {
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
                });
            } else {
                return Ok(ColibriServerEvent {
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
                });
            }
        }
        "social.colibri.reaction" => {
            if event_record.action != "delete" {
                let record_data = serde_json::from_value::<ColibriReaction>(
                    event_record.record.as_ref().unwrap().to_owned(),
                )?;

                return Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::ReactionEventData(
                        ReactionEventData {
                            event: String::from("added"),
                            uri,
                            emoji: Some(record_data.emoji),
                            target: Some(record_data.target_message),
                        },
                    )),
                });
            } else {
                return Ok(ColibriServerEvent {
                    event_type: String::from("reaction_event"),
                    data: Some(ColibriServerEventData::ReactionEventData(
                        ReactionEventData {
                            event: String::from("removed"),
                            uri,
                            emoji: None,
                            target: None,
                        },
                    )),
                });
            }
        }
        "social.colibri.actor.data" | "app.bsky.actor.profile" => {
            // TODO: Fetch other half of profile, send full event
            return Err(serde_json::error::Error::custom(String::from(
                "Not implemented",
            )));
        }
        "social.colibri.richtext.facet" => {
            return Err(serde_json::error::Error::custom(String::from("Facet")));
        }
        _ => Err(serde_json::error::Error::custom(String::from(
            "Unknown collection",
        ))),
    }
}
