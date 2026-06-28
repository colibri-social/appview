//! `social.colibri.community.undismissApplication` — restores a previously
//! dismissed join application to the active moderation queue. The inverse of
//! `dismissApplication`; see that module's docs for the off-protocol
//! rationale. Requires the `approval.manage` permission.

use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::dismissed_applications;
use crate::lib::events::{ApplicationEventData, ColibriServerEvent, ColibriServerEventData};
use crate::lib::handler::{
    LoadAuthzFn, VerifyAuthFn, load_authz_boxed, verify_auth_boxed, with_community_authz,
};
use crate::lib::permissions::Permission;
use crate::lib::responses::ErrorResponse;
use crate::lib::tap::CommsBridge;
use crate::xrpc::social::colibri::community::dismiss_application_handler::{
    BroadcastFn, FindApplicationFn, find_application_boxed,
};
use crate::xrpc::social::colibri::community::list_applications_handler::Application;

#[derive(Serialize, Debug)]
pub struct UndismissApplicationResponse {
    pub did: String,
    pub community: String,
}

pub type UndismissFn = dyn Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<(), DbErr>>
    + Send
    + Sync;

pub fn undismiss_boxed(
    db: DatabaseConnection,
    community_authority: String,
    did: String,
) -> BoxFuture<'static, Result<(), DbErr>> {
    Box::pin(
        async move { dismissed_applications::undismiss(&db, &community_authority, &did).await },
    )
}

fn application_event(community: String, app: Application) -> ColibriServerEvent {
    ColibriServerEvent {
        event_type: String::from("application_event"),
        data: Some(ColibriServerEventData::Application(ApplicationEventData {
            event: String::from("undismiss"),
            community,
            membership: app.membership,
            did: Some(app.did),
            handle: None,
            created_at: None,
            data: None,
        })),
    }
}

#[allow(clippy::too_many_arguments)]
async fn undismiss_application_with(
    community_uri: String,
    did: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    undismiss_fn: &UndismissFn,
    find_application_fn: &FindApplicationFn,
    broadcast_fn: &BroadcastFn,
) -> Result<Json<UndismissApplicationResponse>, ErrorResponse> {
    let did_for_body = did.clone();

    with_community_authz(
        auth,
        "social.colibri.community.undismissApplication",
        community_uri,
        Some(Permission::ApprovalManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        move |ctx, db| async move {
            undismiss_fn(db.clone(), ctx.community.authority.clone(), did.clone()).await?;

            if let Some(app) = find_application_fn(
                db,
                ctx.community.authority.clone(),
                ctx.community_uri.clone(),
                did.clone(),
            )
            .await?
            {
                broadcast_fn(application_event(ctx.community_uri.clone(), app));
            }

            Ok(Json(UndismissApplicationResponse {
                did: did_for_body,
                community: ctx.community_uri.clone(),
            }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.community.undismissApplication?<community>&<did>&<auth>")]
/// Restores a dismissed join application to the active moderation queue.
/// Requires `approval.manage`.
pub async fn undismiss_application(
    community: &str,
    did: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
) -> Result<Json<UndismissApplicationResponse>, ErrorResponse> {
    let sender = bridge.applications.clone();
    undismiss_application_with(
        community.to_string(),
        did.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &undismiss_boxed,
        &find_application_boxed,
        &move |event| {
            let _ = sender.send(event);
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::community_authz::ActorAuthz;
    use crate::lib::test_fixtures::{member, mock_db, role};
    use crate::xrpc::social::colibri::actor::get_data_handler::{ActorData, ActorStatus};
    use rocket::tokio;
    use std::sync::{Arc, Mutex};

    fn sample_application() -> Application {
        Application {
            did: String::from("did:plc:applicant"),
            handle: String::from("applicant.test"),
            membership: String::from("at://did:plc:applicant/social.colibri.membership/m1"),
            created_at: String::from("2026-01-01T00:00:00Z"),
            data: ActorData {
                display_name: String::from("Applicant"),
                avatar: None,
                banner: None,
                description: None,
                is_bot: false,
                online_state: String::from("offline"),
                sync_bluesky: false,
                theme: None,
                status: ActorStatus {
                    text: String::new(),
                    emoji: None,
                },
            },
        }
    }

    #[tokio::test]
    async fn undismisses_and_broadcasts_when_caller_has_permission() {
        let db = mock_db();
        let undismiss_captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let undismiss_clone = undismiss_captured.clone();
        let broadcast_captured: Arc<Mutex<Option<ColibriServerEvent>>> = Arc::new(Mutex::new(None));
        let broadcast_clone = broadcast_captured.clone();

        let result = undismiss_application_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:applicant"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:mod")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: Some(member("did:plc:mod", vec!["r1"])),
                        roles: vec![role("Moderator", 10, vec![Permission::ApprovalManage])],
                    })
                })
            },
            &move |_, community_authority, did| {
                let captured = undismiss_clone.clone();
                Box::pin(async move {
                    *captured.lock().unwrap() = Some((community_authority, did));
                    Ok(())
                })
            },
            &|_, _, _, _| Box::pin(async { Ok(Some(sample_application())) }),
            &move |event| {
                *broadcast_clone.lock().unwrap() = Some(event);
            },
        )
        .await
        .unwrap();

        assert_eq!(result.did, "did:plc:applicant");
        let (community_authority, did) = undismiss_captured.lock().unwrap().take().unwrap();
        assert_eq!(community_authority, "did:plc:owner");
        assert_eq!(did, "did:plc:applicant");

        let event = broadcast_captured.lock().unwrap().take().unwrap();
        if let Some(crate::lib::events::ColibriServerEventData::Application(data)) = event.data {
            assert_eq!(data.event, "undismiss");
        } else {
            panic!("expected application_event payload");
        }
    }

    #[tokio::test]
    async fn rejects_when_caller_lacks_permission() {
        let db = mock_db();
        let result = undismiss_application_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("did:plc:applicant"),
            String::from("token"),
            db,
            &|_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            &|_, _, _| {
                Box::pin(async {
                    Ok(ActorAuthz {
                        is_owner: false,
                        member: None,
                        roles: vec![],
                    })
                })
            },
            &|_, _, _| Box::pin(async { panic!("should not undismiss when permission missing") }),
            &|_, _, _, _| {
                Box::pin(async { panic!("should not find application when permission missing") })
            },
            &|_| panic!("should not broadcast when permission missing"),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }
}
