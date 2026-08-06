use std::collections::HashSet;

use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DbErr, EntityTrait,
    PaginatorTrait, QueryFilter, TransactionTrait,
};
use serde::Serialize;

use crate::lib::colibri::ColibriMember;
use crate::lib::community_credentials;
use crate::lib::embed_fetch;
use crate::lib::moderation;
use crate::lib::repo_endpoint;
use crate::lib::tap;
use crate::models::{
    community_invitations, dismissed_applications, notifications, push_subscriptions, record_data,
    user_states,
};
use crate::xrpc::social::colibri::actor::list_communities_handler::get_authorized_communities;

const COMMUNITY_NSID: &str = "social.colibri.community";
const MEMBER_NSID: &str = "social.colibri.member";
const MEMBERSHIP_NSID: &str = "social.colibri.membership";
const APPROVAL_NSID: &str = "social.colibri.approval";
const ROLE_NSID: &str = "social.colibri.role";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapTeardown {
    Remove,
    AlreadyGone,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct SoleOwnedCommunity {
    pub uri: String,
    pub name: String,
    #[serde(rename = "memberCount")]
    pub member_count: u64,
}

#[derive(Serialize, Debug, Default, PartialEq, Eq)]
pub struct DeletionCounts {
    pub records: u64,
    pub notifications: u64,
    #[serde(rename = "pushSubscriptions")]
    pub push_subscriptions: u64,
    pub invitations: u64,
}

#[derive(Serialize, Debug, Default, PartialEq, Eq)]
pub struct DeletedCounts {
    #[serde(rename = "recordData")]
    pub record_data: u64,
    #[serde(rename = "communityRecords")]
    pub community_records: u64,
    pub notifications: u64,
    #[serde(rename = "pushSubscriptions")]
    pub push_subscriptions: u64,
    #[serde(rename = "userState")]
    pub user_state: u64,
    pub invitations: u64,
    #[serde(rename = "dismissedApplications")]
    pub dismissed_applications: u64,
}

pub fn community_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/{COMMUNITY_NSID}/{rkey}")
}

async fn owner_dids(
    db: &DatabaseConnection,
    community_did: &str,
) -> Result<HashSet<String>, DbErr> {
    let protected: HashSet<String> = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(ROLE_NSID))
        .filter(record_data::Column::Did.eq(community_did))
        .filter(Expr::cust(r#""record_data"."data"->>'protected' = 'true'"#))
        .all(db)
        .await?
        .into_iter()
        .map(|role| role.rkey)
        .collect();

    if protected.is_empty() {
        return Ok(HashSet::new());
    }

    let members = record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(record_data::Column::Did.eq(community_did))
        .all(db)
        .await?;

    let mut owners = HashSet::new();
    for record in members {
        if let Ok(member) = serde_json::from_value::<ColibriMember>(record.data)
            && member.roles.iter().any(|rkey| protected.contains(rkey))
        {
            owners.insert(member.subject);
        }
    }

    Ok(owners)
}

async fn member_count(
    db: &DatabaseConnection,
    community_did: &str,
    uri: &str,
    is_legacy: bool,
) -> Result<u64, DbErr> {
    if is_legacy {
        return record_data::Entity::find()
            .filter(record_data::Column::Nsid.eq(MEMBERSHIP_NSID))
            .filter(Expr::cust_with_values(
                r#""record_data"."data"->>'community' = $1"#,
                vec![sea_orm::Value::from(uri)],
            ))
            .count(db)
            .await;
    }

    record_data::Entity::find()
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(record_data::Column::Did.eq(community_did))
        .count(db)
        .await
}

pub async fn sole_owned_communities(
    db: &DatabaseConnection,
    did: &str,
) -> Result<Vec<SoleOwnedCommunity>, DbErr> {
    let mut blocked = Vec::new();

    for community in get_authorized_communities(db, did).await? {
        if !community.is_owner {
            continue;
        }

        let community_did = community.community.did.clone();
        let uri = community_uri(&community_did, &community.community.rkey);

        let alone = if community_did == did {
            true
        } else {
            let owners = owner_dids(db, &community_did).await?;
            owners.iter().all(|owner| owner == did)
        };

        if !alone {
            continue;
        }

        let name = community
            .community
            .data
            .get("name")
            .and_then(|value| value.as_str())
            .unwrap_or("Untitled community")
            .to_string();

        blocked.push(SoleOwnedCommunity {
            member_count: member_count(db, &community_did, &uri, community.is_legacy).await?,
            uri,
            name,
        });
    }

    Ok(blocked)
}

pub async fn deletion_counts(db: &DatabaseConnection, did: &str) -> Result<DeletionCounts, DbErr> {
    Ok(DeletionCounts {
        records: record_data::Entity::find()
            .filter(record_data::Column::Did.eq(did))
            .count(db)
            .await?,
        notifications: notifications::Entity::find()
            .filter(
                Condition::any()
                    .add(notifications::Column::RecipientDid.eq(did))
                    .add(notifications::Column::AuthorDid.eq(did)),
            )
            .count(db)
            .await?,
        push_subscriptions: push_subscriptions::Entity::find()
            .filter(push_subscriptions::Column::ActorDid.eq(did))
            .count(db)
            .await?,
        invitations: community_invitations::Entity::find()
            .filter(community_invitations::Column::CreatedBy.eq(did))
            .count(db)
            .await?,
    })
}

pub async fn pds_account_page(db: &DatabaseConnection, did: &str) -> Option<String> {
    let endpoint = repo_endpoint::resolve(db, did).await.ok()?;
    let url = format!("{}/account", endpoint.as_str());

    match embed_fetch::guarded_get(&url).await {
        Ok(response) if response.status().is_success() => {
            let is_html = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/html"));
            is_html.then_some(url)
        }
        _ => None,
    }
}

async fn revoke_memberships(db: &DatabaseConnection, did: &str) {
    let communities = match get_authorized_communities(db, did).await {
        Ok(communities) => communities,
        Err(e) => {
            log::warn!("Could not list {did}'s communities before purging: {e}");
            return;
        }
    };

    for community in communities {
        let community_did = &community.community.did;
        if community_did == did {
            continue;
        }
        if let Err(e) = moderation::revoke_community_member(db, community_did, did).await
            && !community_credentials::is_missing_credentials(&e)
        {
            log::warn!("Could not revoke {did}'s membership in {community_did}: {e}");
        }
    }
}

async fn delete_rows<C: ConnectionTrait>(txn: &C, did: &str) -> Result<DeletedCounts, DbErr> {
    let record_data_rows = record_data::Entity::delete_many()
        .filter(record_data::Column::Did.eq(did))
        .exec(txn)
        .await?
        .rows_affected;

    let member_rows = record_data::Entity::delete_many()
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'subject' = $1"#,
            vec![sea_orm::Value::from(did)],
        ))
        .exec(txn)
        .await?
        .rows_affected;

    let approval_rows = record_data::Entity::delete_many()
        .filter(record_data::Column::Nsid.eq(APPROVAL_NSID))
        .filter(Expr::cust_with_values(
            r#""record_data"."data"->>'membership' LIKE $1"#,
            vec![sea_orm::Value::from(format!("at://{did}/%"))],
        ))
        .exec(txn)
        .await?
        .rows_affected;

    Ok(DeletedCounts {
        record_data: record_data_rows,
        community_records: member_rows + approval_rows,
        notifications: notifications::Entity::delete_many()
            .filter(
                Condition::any()
                    .add(notifications::Column::RecipientDid.eq(did))
                    .add(notifications::Column::AuthorDid.eq(did)),
            )
            .exec(txn)
            .await?
            .rows_affected,
        push_subscriptions: push_subscriptions::Entity::delete_many()
            .filter(push_subscriptions::Column::ActorDid.eq(did))
            .exec(txn)
            .await?
            .rows_affected,
        user_state: user_states::Entity::delete_many()
            .filter(user_states::Column::Did.eq(did))
            .exec(txn)
            .await?
            .rows_affected,
        invitations: community_invitations::Entity::delete_many()
            .filter(community_invitations::Column::CreatedBy.eq(did))
            .exec(txn)
            .await?
            .rows_affected,
        dismissed_applications: dismissed_applications::Entity::delete_many()
            .filter(dismissed_applications::Column::ApplicantDid.eq(did))
            .exec(txn)
            .await?
            .rows_affected,
    })
}

pub async fn purge_did(
    db: &DatabaseConnection,
    did: &str,
    teardown: TapTeardown,
) -> Result<DeletedCounts, DbErr> {
    revoke_memberships(db, did).await;

    let txn = db.begin().await?;
    let mut deleted = delete_rows(&txn, did).await?;
    txn.commit().await?;

    if teardown == TapTeardown::Remove {
        tap::remove_dids(vec![did.to_string()]).await;
    }

    deleted.record_data += record_data::Entity::delete_many()
        .filter(record_data::Column::Did.eq(did))
        .exec(db)
        .await?
        .rows_affected;

    Ok(deleted)
}
