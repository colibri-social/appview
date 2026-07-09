//! Best-effort self-heal that brings a community's stored protected ("Owner")
//! role record up to the current permission catalog

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use sea_orm::DatabaseConnection;
use serde_json::Value;

use crate::lib::colibri::ColibriRole;
use crate::lib::community_write;
use crate::lib::permissions::Permission;
use crate::models::record_data;

fn inflight() -> &'static Mutex<HashSet<String>> {
    static INFLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    INFLIGHT.get_or_init(|| Mutex::new(HashSet::new()))
}

pub fn spawn_heal(db: &DatabaseConnection, community_did: &str, records: &[record_data::Model]) {
    let full = Permission::all_strings();

    for record in records {
        let Ok(role) = serde_json::from_value::<ColibriRole>(record.data.clone()) else {
            continue;
        };
        if role.protected != Some(true) {
            continue;
        }
        if full.iter().all(|p| role.permissions.contains(p)) {
            continue;
        }

        let uri = format!("at://{}/social.colibri.role/{}", community_did, record.rkey);
        if !inflight().lock().unwrap().insert(uri.clone()) {
            continue;
        }

        let mut data = record.data.clone();
        data["permissions"] = Value::from(full.clone());
        let db = db.clone();
        let community_did = community_did.to_string();
        let rkey = record.rkey.clone();

        rocket::tokio::spawn(async move {
            if let Err(e) =
                community_write::put_record(&db, &community_did, "social.colibri.role", &rkey, data)
                    .await
            {
                log::warn!("owner-role permission heal for {uri} failed: {e}");
            }
            inflight().lock().unwrap().remove(&uri);
        });
    }
}
