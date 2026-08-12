use sea_orm::{ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder};

use crate::lib::colibri::{
    CANONICAL_HUB_DID, ColibriActorProfile, ColibriCommunity, community_hub_did,
};
use crate::lib::community_credentials;
use crate::lib::community_write;
use crate::lib::get_atproto_record::get_atproto_record;
use crate::lib::pds_client;
use crate::lib::repo_endpoint::{self, RepoEndpoint};
use crate::lib::responses::{NOT_COMMUNITY_HUB_MARKER, PDS_UNAVAILABLE_MARKER};
use crate::lib::service_auth::appview_did;
use crate::models::record_data;

const COMMUNITY_NSID: &str = "social.colibri.community";
const PROFILE_NSID: &str = "social.colibri.actor.profile";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubRouting {
    Local,
    Remote(String),
}

pub fn not_hub_err(hub: &str) -> DbErr {
    DbErr::Custom(format!("{NOT_COMMUNITY_HUB_MARKER}{hub}"))
}

pub fn hub_from_err(err: &DbErr) -> Option<String> {
    let message = err.to_string();
    let index = message.find(NOT_COMMUNITY_HUB_MARKER)?;
    Some(
        message[index + NOT_COMMUNITY_HUB_MARKER.len()..]
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string(),
    )
}

pub async fn resolve_routing(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<HubRouting, DbErr> {
    resolve_routing_as(db, community_did, &appview_did()).await
}

pub async fn resolve_routing_as(
    db: &DatabaseConnection,
    community_did: &str,
    me: &str,
) -> Result<HubRouting, DbErr> {
    if let Some(declared) = declared_hub(db, community_did).await? {
        return Ok(if declared == me {
            HubRouting::Local
        } else {
            HubRouting::Remote(declared)
        });
    }

    if community_credentials::stored_pds_endpoint(db, community_did)
        .await?
        .is_some()
    {
        return Ok(HubRouting::Local);
    }

    Ok(if me == CANONICAL_HUB_DID {
        HubRouting::Local
    } else {
        HubRouting::Remote(String::from(CANONICAL_HUB_DID))
    })
}

pub async fn ensure_hub(db: &DatabaseConnection, community_did: &str) -> Result<(), DbErr> {
    match resolve_routing(db, community_did).await? {
        HubRouting::Local => Ok(()),
        HubRouting::Remote(hub) => Err(not_hub_err(&hub)),
    }
}

pub async fn declared_appview(
    db: &DatabaseConnection,
    actor: &str,
) -> Result<Option<String>, DbErr> {
    match get_atproto_record::<ColibriActorProfile>(
        actor.to_string(),
        PROFILE_NSID.to_string(),
        String::from("self"),
        db,
    )
    .await
    {
        Ok(profile) => return Ok(profile.presence_service),
        Err(DbErr::RecordNotFound(_)) => {}
        Err(e) => return Err(e),
    }

    let endpoint = repo_endpoint::resolve(db, actor)
        .await
        .map_err(|e| pds_unavailable_db(format!("resolving {actor}'s PDS failed: {e}")))?;

    let fetched = match &endpoint {
        RepoEndpoint::Trusted(_) => {
            pds_client::get_record_trusted(endpoint.as_str(), actor, PROFILE_NSID, "self").await
        }
        RepoEndpoint::Untrusted(_) => {
            pds_client::get_record(endpoint.as_str(), actor, PROFILE_NSID, "self").await
        }
    }
    .map_err(|e| pds_unavailable_db(format!("reading {actor}'s profile failed: {e}")))?;

    let Some(value) = fetched else {
        return Ok(None);
    };

    let profile = serde_json::from_value::<ColibriActorProfile>(value.clone())
        .map_err(|e| DbErr::Custom(format!("{actor}'s profile did not parse: {e}")))?;

    community_write::cache_upsert(db, actor, PROFILE_NSID, "self", value).await;

    Ok(profile.presence_service)
}

fn pds_unavailable_db(message: String) -> DbErr {
    DbErr::Custom(format!("{PDS_UNAVAILABLE_MARKER}{message}"))
}

async fn declared_hub(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<Option<String>, DbErr> {
    let Some(row) = record_data::Entity::find()
        .filter(record_data::Column::Did.eq(community_did))
        .filter(record_data::Column::Nsid.eq(COMMUNITY_NSID))
        .order_by_asc(record_data::Column::Rkey)
        .one(db)
        .await?
    else {
        return Ok(None);
    };

    let Ok(record) = serde_json::from_value::<ColibriCommunity>(row.data) else {
        log::warn!("community record for {community_did} did not parse; ignoring for hub routing");
        return Ok(None);
    };

    if record.appview.as_deref().is_none_or(str::is_empty) {
        return Ok(None);
    }

    Ok(Some(community_hub_did(&record)))
}

#[cfg(test)]
mod tests {
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    use super::*;
    use crate::models::community_credentials;

    const ME: &str = "did:web:mine.example";
    const THEM: &str = "did:web:theirs.example";
    const COMMUNITY: &str = "did:plc:community";

    fn community_row(appview: Option<&str>) -> record_data::Model {
        let mut data = serde_json::json!({
            "$type": COMMUNITY_NSID,
            "name": "Test",
            "description": "",
            "categoryOrder": [],
        });
        if let Some(appview) = appview {
            data["appview"] = serde_json::Value::String(appview.to_string());
        }

        record_data::Model {
            id: 1,
            did: String::from(COMMUNITY),
            nsid: String::from(COMMUNITY_NSID),
            rkey: String::from("self"),
            data,
            indexed_at: String::from("2026-08-12T00:00:00Z"),
        }
    }

    fn credentials_row() -> community_credentials::Model {
        community_credentials::Model {
            community_did: String::from(COMMUNITY),
            pds_endpoint: String::from("https://pds.example"),
            identifier: String::from("c-abc.test"),
            password_ciphertext_b64: String::from("x"),
            password_nonce_b64: String::from("y"),
            source: String::from("byo"),
            created_at: String::from("2026-08-12T00:00:00Z"),
        }
    }

    fn db(
        record: Vec<record_data::Model>,
        credentials: Vec<community_credentials::Model>,
    ) -> DatabaseConnection {
        MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([record])
            .append_query_results([credentials])
            .into_connection()
    }

    async fn routing(
        record: Vec<record_data::Model>,
        credentials: Vec<community_credentials::Model>,
        me: &str,
    ) -> HubRouting {
        resolve_routing_as(&db(record, credentials), COMMUNITY, me)
            .await
            .expect("routing should resolve")
    }

    #[tokio::test]
    async fn an_explicitly_named_peer_routes_remote() {
        assert_eq!(
            routing(vec![community_row(Some(THEM))], vec![], ME).await,
            HubRouting::Remote(String::from(THEM))
        );
    }

    #[tokio::test]
    async fn being_explicitly_named_routes_local() {
        assert_eq!(
            routing(vec![community_row(Some(ME))], vec![], ME).await,
            HubRouting::Local
        );
    }

    #[tokio::test]
    async fn a_named_peer_wins_over_credentials_we_still_hold() {
        assert_eq!(
            routing(vec![community_row(Some(THEM))], vec![credentials_row()], ME).await,
            HubRouting::Remote(String::from(THEM))
        );
    }

    #[tokio::test]
    async fn holding_credentials_makes_us_the_hub_when_nobody_is_named() {
        assert_eq!(
            routing(vec![community_row(None)], vec![credentials_row()], ME).await,
            HubRouting::Local
        );
    }

    #[tokio::test]
    async fn an_unmirrored_community_still_resolves_from_credentials() {
        assert_eq!(
            routing(vec![], vec![credentials_row()], ME).await,
            HubRouting::Local
        );
    }

    #[tokio::test]
    async fn an_undeclared_community_defers_to_the_canonical_appview() {
        assert_eq!(
            routing(vec![], vec![], ME).await,
            HubRouting::Remote(String::from(CANONICAL_HUB_DID))
        );
    }

    #[tokio::test]
    async fn the_canonical_appview_answers_for_undeclared_communities() {
        assert_eq!(
            routing(vec![], vec![], CANONICAL_HUB_DID).await,
            HubRouting::Local
        );
    }

    #[tokio::test]
    async fn ensure_hub_refuses_a_community_we_do_not_administer() {
        let err = ensure_hub(&db(vec![community_row(Some(THEM))], vec![]), COMMUNITY)
            .await
            .expect_err("a foreign community must be refused");
        assert_eq!(hub_from_err(&err).as_deref(), Some(THEM));
    }

    #[tokio::test]
    async fn an_unparseable_record_falls_through_rather_than_locking_us_out() {
        let mut row = community_row(Some(THEM));
        row.data = serde_json::json!({ "nonsense": true });

        assert_eq!(
            resolve_routing_as(&db(vec![row], vec![credentials_row()]), COMMUNITY, ME)
                .await
                .expect("routing should resolve"),
            HubRouting::Local
        );
    }

    #[test]
    fn not_hub_err_round_trips_the_hub_did() {
        let err = not_hub_err("did:web:other.example");
        assert_eq!(hub_from_err(&err).as_deref(), Some("did:web:other.example"));
    }

    #[test]
    fn an_unrelated_error_is_not_the_not_hub_case() {
        let err = DbErr::Custom(String::from("connection pool exhausted"));
        assert!(hub_from_err(&err).is_none());
    }

    #[test]
    fn a_not_hub_error_surfaces_as_misdirected_naming_the_hub() {
        use crate::lib::responses::{ErrorCode, ErrorResponse};

        let res = ErrorResponse::from(not_hub_err("did:web:other.example"));
        assert_eq!(res.body.error, ErrorCode::NotCommunityHub.as_str());
        assert_eq!(res.body.hub.as_deref(), Some("did:web:other.example"));
    }
}
