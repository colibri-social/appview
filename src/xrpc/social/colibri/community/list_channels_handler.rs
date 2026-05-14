use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::lib::at_uri::AtUri;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::models::record_data;

#[derive(Serialize, Deserialize, Debug)]
pub struct Channel {
    pub uri: String,
    pub name: String,
    #[serde(rename = "type")]
    pub channel_type: String,
    pub category: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChannelList {
    pub channels: Vec<Channel>,
}

#[derive(Deserialize)]
struct StoredChannel {
    name: String,
    #[serde(rename = "type")]
    channel_type: String,
    category: String,
}

pub async fn fetch_channel_records(
    db: &DatabaseConnection,
    authority: &str,
    community_rkey: &str,
) -> Result<Vec<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.channel"))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(community_rkey.to_string())],
        ))
        .all(db)
        .await
}

type FetchChannelsFn = fn(
    DatabaseConnection,
    String,
    String,
) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>>;

async fn list_channels_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_channels_fn: FetchChannelsFn,
) -> Result<Json<ChannelList>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    let records =
        fetch_channels_fn(db, community.authority.clone(), community.rkey.clone()).await?;

    let channels = records
        .into_iter()
        .filter_map(|r| {
            let stored = serde_json::from_value::<StoredChannel>(r.data).ok()?;
            Some(Channel {
                uri: format!("at://{}/{}/{}", r.did, r.nsid, r.rkey),
                name: stored.name,
                channel_type: stored.channel_type,
                category: format!(
                    "at://{}/social.colibri.category/{}",
                    community.authority, stored.category
                ),
            })
        })
        .collect();

    Ok(Json(ChannelList { channels }))
}

fn fetch_channels_boxed(
    db: DatabaseConnection,
    authority: String,
    community_rkey: String,
) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>> {
    Box::pin(async move { fetch_channel_records(&db, &authority, &community_rkey).await })
}

#[get("/xrpc/social.colibri.community.listChannels?<community>")]
/// Lists all channels defined under a Colibri community.
pub async fn list_channels(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ChannelList>, ErrorResponse> {
    list_channels_with(
        community.to_string(),
        db.inner().clone(),
        fetch_channels_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn maps_records_to_channel_response() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_channels_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            |_, _, _| {
                Box::pin(async {
                    Ok(vec![record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.channel"),
                        rkey: String::from("chan-a"),
                        data: serde_json::json!({
                            "name": "general",
                            "type": "social.colibri.channel.text",
                            "category": "cat1",
                            "community": "c1"
                        }),
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.channels.len(), 1);
        assert_eq!(
            result.channels[0].uri,
            "at://did:plc:owner/social.colibri.channel/chan-a"
        );
        assert_eq!(result.channels[0].name, "general");
        assert_eq!(result.channels[0].channel_type, "social.colibri.channel.text");
        assert_eq!(
            result.channels[0].category,
            "at://did:plc:owner/social.colibri.category/cat1"
        );
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_channels_with(
            String::from("not-a-uri"),
            db,
            |_, _, _| Box::pin(async { panic!("should not fetch when uri is invalid") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }
}
