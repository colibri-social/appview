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
