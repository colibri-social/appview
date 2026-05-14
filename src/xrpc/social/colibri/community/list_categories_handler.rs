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
pub struct Category {
    pub uri: String,
    pub name: String,
    #[serde(rename = "channelOrder")]
    pub channel_order: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CategoryList {
    pub categories: Vec<Category>,
}

#[derive(Deserialize)]
struct StoredCategory {
    name: String,
    #[serde(rename = "channelOrder", default)]
    channel_order: Vec<String>,
}

pub async fn fetch_category_records(
    db: &DatabaseConnection,
    authority: &str,
    community_rkey: &str,
) -> Result<Vec<record_data::Model>, DbErr> {
    record_data::Entity::find()
        .filter(record_data::Column::Did.eq(authority))
        .filter(record_data::Column::Nsid.eq("social.colibri.category"))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'community' = $1"#,
            vec![sea_orm::Value::from(community_rkey.to_string())],
        ))
        .all(db)
        .await
}

type FetchCategoriesFn = fn(
    DatabaseConnection,
    String,
    String,
) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>>;

async fn list_categories_with(
    community_uri: String,
    db: DatabaseConnection,
    fetch_categories_fn: FetchCategoriesFn,
) -> Result<Json<CategoryList>, ErrorResponse> {
    let community = AtUri::parse(&community_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    })?;

    let records =
        fetch_categories_fn(db, community.authority.clone(), community.rkey.clone()).await?;

    let categories = records
        .into_iter()
        .filter_map(|r| {
            let stored = serde_json::from_value::<StoredCategory>(r.data).ok()?;
            let channel_order = stored
                .channel_order
                .into_iter()
                .map(|rkey| {
                    format!(
                        "at://{}/social.colibri.channel/{}",
                        community.authority, rkey
                    )
                })
                .collect();
            Some(Category {
                uri: format!("at://{}/{}/{}", r.did, r.nsid, r.rkey),
                name: stored.name,
                channel_order,
            })
        })
        .collect();

    Ok(Json(CategoryList { categories }))
}

fn fetch_categories_boxed(
    db: DatabaseConnection,
    authority: String,
    community_rkey: String,
) -> BoxFuture<'static, Result<Vec<record_data::Model>, DbErr>> {
    Box::pin(async move { fetch_category_records(&db, &authority, &community_rkey).await })
}

#[get("/xrpc/social.colibri.community.listCategories?<community>")]
/// Lists all categories defined under a Colibri community.
pub async fn list_categories(
    community: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CategoryList>, ErrorResponse> {
    list_categories_with(
        community.to_string(),
        db.inner().clone(),
        fetch_categories_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    #[tokio::test]
    async fn maps_records_to_category_response() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_categories_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            |_, _, _| {
                Box::pin(async {
                    Ok(vec![record_data::Model {
                        id: 1,
                        did: String::from("did:plc:owner"),
                        nsid: String::from("social.colibri.category"),
                        rkey: String::from("cat1"),
                        data: serde_json::json!({
                            "name": "General",
                            "channelOrder": ["chan-a", "chan-b"],
                            "community": "c1"
                        }),
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.categories.len(), 1);
        assert_eq!(
            result.categories[0].uri,
            "at://did:plc:owner/social.colibri.category/cat1"
        );
        assert_eq!(result.categories[0].name, "General");
        assert_eq!(
            result.categories[0].channel_order,
            vec![
                String::from("at://did:plc:owner/social.colibri.channel/chan-a"),
                String::from("at://did:plc:owner/social.colibri.channel/chan-b"),
            ]
        );
    }

    #[tokio::test]
    async fn rejects_invalid_community_uri() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_categories_with(
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

    #[tokio::test]
    async fn skips_records_with_invalid_payload() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_categories_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            db,
            |_, _, _| {
                Box::pin(async {
                    Ok(vec![
                        record_data::Model {
                            id: 1,
                            did: String::from("did:plc:owner"),
                            nsid: String::from("social.colibri.category"),
                            rkey: String::from("cat1"),
                            data: serde_json::json!({ "irrelevant": true }),
                        },
                        record_data::Model {
                            id: 2,
                            did: String::from("did:plc:owner"),
                            nsid: String::from("social.colibri.category"),
                            rkey: String::from("cat2"),
                            data: serde_json::json!({
                                "name": "Voice",
                                "channelOrder": [],
                                "community": "c1"
                            }),
                        },
                    ])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.categories.len(), 1);
        assert_eq!(result.categories[0].name, "Voice");
    }
}
