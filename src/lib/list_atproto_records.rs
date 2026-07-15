use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect,
};

use crate::models::record_data::{self, Entity as RecordData, Model};

const DEFAULT_LIMIT: u64 = 100;
const MAX_LIMIT: u64 = 100;

pub struct PagedRecords<T> {
    pub records: Vec<T>,
    pub cursor: Option<String>,
}

/// Lists records for a given (did, nsid) with rkey-based cursor pagination.
///
/// The cursor is the rkey of the last record returned. Default order is
/// descending by rkey (newest TID-encoded records first); when `reverse` is
/// true, ascending order is used. The returned cursor is only set when the
/// page is full, matching `com.atproto.repo.listRecords` semantics.
pub async fn list_atproto_records<T: for<'de> serde::Deserialize<'de>>(
    did: String,
    nsid: String,
    limit: Option<u64>,
    cursor: Option<&str>,
    reverse: Option<bool>,
    db: &DatabaseConnection,
) -> Result<PagedRecords<T>, DbErr> {
    let effective_limit = limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let ascending = reverse.is_some_and(|v| v);

    let mut condition = Condition::all()
        .add(record_data::Column::Did.eq(&did))
        .add(record_data::Column::Nsid.eq(&nsid));

    if let Some(c) = cursor {
        condition = if ascending {
            condition.add(record_data::Column::Rkey.gt(c))
        } else {
            condition.add(record_data::Column::Rkey.lt(c))
        };
    }

    let query = RecordData::find().filter(condition).limit(effective_limit);

    let result: Vec<Model> = if ascending {
        query
            .order_by_asc(record_data::Column::Rkey)
            .all(db)
            .await?
    } else {
        query
            .order_by_desc(record_data::Column::Rkey)
            .all(db)
            .await?
    };

    let next_cursor = if (result.len() as u64) == effective_limit {
        result.last().map(|r| r.rkey.clone())
    } else {
        None
    };

    let records: Vec<T> = result
        .into_iter()
        .map(|r| serde_json::from_value::<T>(r.data).unwrap())
        .collect();

    Ok(PagedRecords {
        records,
        cursor: next_cursor,
    })
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

    fn model(id: i64, rkey: &str, text: &str) -> crate::models::record_data::Model {
        crate::models::record_data::Model {
            id,
            did: String::from("did:plc:abc"),
            nsid: String::from("social.colibri.message"),
            rkey: String::from(rkey),
            data: serde_json::json!({ "text": text }),
            indexed_at: String::from(""),
        }
    }

    #[tokio::test]
    async fn returns_records_in_descending_rkey_order_by_default() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![model(2, "r2", "second"), model(1, "r1", "first")]])
            .into_connection();

        let result = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(10),
            None,
            None,
            &db,
        )
        .await
        .unwrap();

        assert_eq!(result.records[0].text, "second");
        assert_eq!(result.records[1].text, "first");
    }

    #[tokio::test]
    async fn returns_records_in_ascending_rkey_order_when_reverse_is_true() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![model(1, "r1", "first"), model(2, "r2", "second")]])
            .into_connection();

        let result = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(10),
            None,
            Some(true),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(result.records[0].text, "first");
        assert_eq!(result.records[1].text, "second");
    }

    #[tokio::test]
    async fn returns_cursor_when_page_is_full() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![model(2, "r2", "second"), model(1, "r1", "first")]])
            .into_connection();

        let result = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(2),
            None,
            None,
            &db,
        )
        .await
        .unwrap();

        assert_eq!(result.cursor.as_deref(), Some("r1"));
    }

    #[tokio::test]
    async fn omits_cursor_when_page_is_not_full() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![model(1, "r1", "first")]])
            .into_connection();

        let result = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(10),
            None,
            None,
            &db,
        )
        .await
        .unwrap();

        assert!(result.cursor.is_none());
    }

    #[tokio::test]
    async fn clamps_caller_supplied_limit_to_max() {
        let rows: Vec<_> = (0..100)
            .map(|i| model(i, &format!("r{i:03}"), "x"))
            .collect();
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([rows])
            .into_connection();

        let result = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(999_999),
            None,
            None,
            &db,
        )
        .await
        .unwrap();

        assert!(result.cursor.is_some());
    }

    #[tokio::test]
    async fn returns_no_cursor_when_no_records_match() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<crate::models::record_data::Model>::new()])
            .into_connection();

        let result = list_atproto_records::<SampleRecord>(
            String::from("did:plc:abc"),
            String::from("social.colibri.message"),
            Some(10),
            Some("r9"),
            None,
            &db,
        )
        .await
        .unwrap();

        assert!(result.records.is_empty());
        assert!(result.cursor.is_none());
    }
}
