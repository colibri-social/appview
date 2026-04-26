use sea_orm::{ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter};

use crate::models::record_data::{self, Entity as RecordData};

pub async fn get_atproto_record<T: for<'de> serde::Deserialize<'de>>(
    did: String,
    nsid: String,
    rkey: String,
    db: &DatabaseConnection,
) -> Result<T, DbErr> {
    let record = RecordData::find()
        .filter(
            Condition::all()
                .add(record_data::Column::Did.eq(&did))
                .add(record_data::Column::Nsid.eq(&nsid))
                .add(record_data::Column::Rkey.eq(&rkey)),
        )
        .one(db)
        .await?;

    if record.is_none() {
        return Err(DbErr::RecordNotFound(String::from(format!(
            "No record found for at://{did}/{nsid}/{rkey}"
        ))));
    }

    return serde_json::from_value::<T>(record.unwrap().data)
        .map_err(|e| DbErr::Custom(e.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct SampleRecord {
        text: String,
    }

    #[tokio::test]
    async fn returns_record_when_found() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![crate::models::record_data::Model {
                id: 1,
                did: String::from("did:plc:abc"),
                nsid: String::from("social.colibri.message"),
                rkey: String::from("r1"),
                data: serde_json::json!({ "text": "hello" }),
            }]])
            .into_connection();

        let record = get_atproto_record::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            String::from("r1"),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(record.text, "hello");
    }

    #[tokio::test]
    async fn returns_not_found_for_missing_record() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<crate::models::record_data::Model>::new()])
            .into_connection();

        let err = get_atproto_record::<SampleRecord>(
            String::from("did:plc:none"),
            String::from("social.colibri.message"),
            String::from("r1"),
            &db,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, DbErr::RecordNotFound(_)));
    }

    #[tokio::test]
    async fn returns_custom_error_for_invalid_payload() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![crate::models::record_data::Model {
                id: 1,
                did: String::from("did:plc:abc"),
                nsid: String::from("social.colibri.message"),
                rkey: String::from("r1"),
                data: serde_json::json!({ "missing_text": true }),
            }]])
            .into_connection();

        let err = get_atproto_record::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            String::from("r1"),
            &db,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, DbErr::Custom(_)));
    }
}
