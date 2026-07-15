//! Full-network refill: a boot-time self-heal knob for an AppView whose local
//! `record_data` cache has drifted from the network (partial backfills, past
//! bugs, tap outages that outlasted its redelivery window, etc.).
//!
//! Triggered by setting `REFILL_FROM_SCRATCH` before starting the process.
//! This is deliberately separate from the AppView's normal, always-on backfill
//! behavior (registering a DID with tap on login/community-create/membership,
//! see [`crate::lib::tap::register_dids`]). It uses
//! [`crate::lib::tap::force_redo_backfill`] rather than a plain re-register:
//! tap only backfills repos it isn't already tracking, so simply re-adding a
//! DID it already knows about would silently do nothing.

use crate::lib::tap::force_redo_backfill;
use crate::models::{
    community_credentials, community_invitations, dismissed_applications, notifications,
    push_subscriptions, record_data, user_states,
};
use sea_orm::{DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QuerySelect};
use std::collections::HashSet;

/// Tap's `/repos/add` and `/repos/remove` endpoints are one JSON POST per
/// call; batching keeps any single request modest in size even for an
/// AppView that has accumulated a very large number of known DIDs.
const REGISTER_BATCH_SIZE: usize = 500;

/// Whether `REFILL_FROM_SCRATCH` is set to a non-empty value. Presence-based,
/// like the codebase's other optional boot flags (e.g. `KLIPY_API_KEY`)
pub fn refill_from_scratch_requested() -> bool {
    std::env::var("REFILL_FROM_SCRATCH")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

/// Harvests every DID this AppView has ever recorded anywhere, clears the
/// mirrored `record_data` cache, then forces tap to redo a full historical
/// backfill for every harvested DID, re-streaming each repo through the
/// normal ingestion pipeline ([`crate::lib::tap::process_event`]).
///
/// Uses [`force_redo_backfill`] (remove, then re-add) rather than plain
/// `register_dids`: tap only backfills repos it isn't already tracking, so a
/// bare re-add would silently no-op for every DID tap already has — exactly
/// the case this refill needs to override.
pub async fn refill_network_from_scratch(db: &DatabaseConnection) -> Result<(), DbErr> {
    log::warn!(
        "REFILL_FROM_SCRATCH set: clearing record_data cache and forcing tap to redo backfill for every known DID"
    );

    let dids = collect_known_dids(db).await?;
    log::info!(
        "refill: collected {} known DID(s) to re-backfill",
        dids.len()
    );

    record_data::Entity::delete_many().exec(db).await?;
    log::info!("refill: cleared record_data cache");

    let dids: Vec<String> = dids.into_iter().collect();
    for chunk in dids.chunks(REGISTER_BATCH_SIZE) {
        force_redo_backfill(chunk.to_vec()).await;
    }

    log::info!(
        "refill: forced tap to redo backfill for {} DID(s)",
        dids.len()
    );

    Ok(())
}

#[derive(FromQueryResult)]
struct DidColumn {
    did: String,
}

/// Gathers the distinct set of DIDs known to this AppView: every author in
/// the mirrored record cache, plus every `did`/`*_did` column across the
/// AppView's own internal tables
async fn collect_known_dids(db: &DatabaseConnection) -> Result<HashSet<String>, DbErr> {
    let mut dids = HashSet::new();

    let record_data_dids = record_data::Entity::find()
        .select_only()
        .column_as(record_data::Column::Did, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(record_data_dids.into_iter().map(|r| r.did));

    let user_state_dids = user_states::Entity::find()
        .select_only()
        .column_as(user_states::Column::Did, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(user_state_dids.into_iter().map(|r| r.did));

    let notif_recipients = notifications::Entity::find()
        .select_only()
        .column_as(notifications::Column::RecipientDid, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(notif_recipients.into_iter().map(|r| r.did));

    let notif_authors = notifications::Entity::find()
        .select_only()
        .column_as(notifications::Column::AuthorDid, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(notif_authors.into_iter().map(|r| r.did));

    let credential_dids = community_credentials::Entity::find()
        .select_only()
        .column_as(community_credentials::Column::CommunityDid, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(credential_dids.into_iter().map(|r| r.did));

    let invitation_creator_dids = community_invitations::Entity::find()
        .select_only()
        .column_as(community_invitations::Column::CreatedBy, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(invitation_creator_dids.into_iter().map(|r| r.did));

    let dismissed_community_dids = dismissed_applications::Entity::find()
        .select_only()
        .column_as(dismissed_applications::Column::CommunityDid, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(dismissed_community_dids.into_iter().map(|r| r.did));

    let dismissed_applicant_dids = dismissed_applications::Entity::find()
        .select_only()
        .column_as(dismissed_applications::Column::ApplicantDid, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(dismissed_applicant_dids.into_iter().map(|r| r.did));

    let push_actor_dids = push_subscriptions::Entity::find()
        .select_only()
        .column_as(push_subscriptions::Column::ActorDid, "did")
        .distinct()
        .into_model::<DidColumn>()
        .all(db)
        .await?;
    dids.extend(push_actor_dids.into_iter().map(|r| r.did));

    Ok(dids)
}
