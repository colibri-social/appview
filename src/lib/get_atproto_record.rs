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
