use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QuerySelect,
};

use crate::models::record_data::{self, Entity as RecordData, Model};

pub async fn list_atproto_records<T: for<'de> serde::Deserialize<'de>>(
    did: String,
    nsid: String,
    limit: Option<u64>,
    _cursor: Option<&str>, // TODO: Cursor support
    reverse: Option<bool>,
    db: &DatabaseConnection,
) -> Result<Vec<T>, DbErr> {
    let default_limit = 100;
    let records = RecordData::find()
        .filter(
            Condition::all()
                .add(record_data::Column::Did.eq(&did))
                .add(record_data::Column::Nsid.eq(&nsid)),
        )
        .limit(limit.unwrap_or(default_limit).to_owned());

    let result: Vec<Model>;

    if reverse.is_some_and(|v| v == true) {
        result = records.order_by_id_asc().all(db).await?;
    } else {
        result = records.order_by_id_desc().all(db).await?;
    }

    let mut json_records: Vec<T> = result
        .iter()
        .map(|r| serde_json::from_value::<T>(r.to_owned().data).unwrap())
        .collect();

    if reverse.is_some_and(|v| v == true) {
        json_records.reverse();
    }

    return Ok(json_records);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct SampleRecord {
        text: String,
    }

    #[tokio::test]
    async fn returns_records_in_default_order() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                crate::models::record_data::Model {
                    id: 2,
                    did: String::from("did:plc:abc"),
                    nsid: String::from("social.colibri.message"),
                    rkey: String::from("r2"),
                    data: serde_json::json!({ "text": "second" }),
                },
                crate::models::record_data::Model {
                    id: 1,
                    did: String::from("did:plc:abc"),
                    nsid: String::from("social.colibri.message"),
                    rkey: String::from("r1"),
                    data: serde_json::json!({ "text": "first" }),
                },
            ]])
            .into_connection();

        let records = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(10),
            None,
            None,
            &db,
        )
        .await
        .unwrap();

        assert_eq!(records[0].text, "second");
        assert_eq!(records[1].text, "first");
    }

    #[tokio::test]
    async fn reverses_records_when_reverse_flag_is_true() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![
                crate::models::record_data::Model {
                    id: 1,
                    did: String::from("did:plc:abc"),
                    nsid: String::from("social.colibri.message"),
                    rkey: String::from("r1"),
                    data: serde_json::json!({ "text": "first" }),
                },
                crate::models::record_data::Model {
                    id: 2,
                    did: String::from("did:plc:abc"),
                    nsid: String::from("social.colibri.message"),
                    rkey: String::from("r2"),
                    data: serde_json::json!({ "text": "second" }),
                },
            ]])
            .into_connection();

        let records = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(10),
            None,
            Some(true),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(records[0].text, "second");
        assert_eq!(records[1].text, "first");
    }
}
