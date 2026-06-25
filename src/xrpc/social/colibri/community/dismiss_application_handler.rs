//! `social.colibri.community.dismissApplication` — moderator-only, AppView-only
//! action that hides a pending join application from the active queue
//! without resolving it.
//!
//! This is deliberately off-protocol: dismissal is a local moderation
//! convenience (e.g. "I've seen this, deal with it later") with no
//! `social.colibri.*` record written anywhere. The underlying
//! `social.colibri.membership` is untouched, so the application is still
//! "pending" as far as the protocol and `approveMembership` are concerned —
//! it just stops showing up in the active `listApplications` queue until a
//! moderator calls `undismissApplication` or it resolves normally.
//!
//! Requires the `approval.manage` permission, same as `listApplications` and
//! `approveMembership`.

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
use crate::xrpc::social::colibri::community::list_applications_handler::{
    Application, find_application_for_did,
};

#[derive(Serialize, Debug)]
pub struct DismissApplicationResponse {
    pub did: String,
    pub community: String,
}

pub type DismissFn = dyn Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<(), DbErr>>
    + Send
    + Sync;

pub type FindApplicationFn = dyn Fn(
        DatabaseConnection,
        String,
        String,
        String,
    ) -> BoxFuture<'static, Result<Option<Application>, DbErr>>
    + Send
    + Sync;

/// Trait-object seam for the broadcast side effect — synchronous, since
/// `broadcast::Sender::send` doesn't need to await anything.
pub type BroadcastFn = dyn Fn(ColibriServerEvent) + Send + Sync;

pub fn dismiss_boxed(
    db: DatabaseConnection,
    community_authority: String,
    did: String,
) -> BoxFuture<'static, Result<(), DbErr>> {
    Box::pin(async move { dismissed_applications::dismiss(&db, &community_authority, &did).await })
}

pub fn find_application_boxed(
    db: DatabaseConnection,
    community_authority: String,
    community_uri: String,
    did: String,
) -> BoxFuture<'static, Result<Option<Application>, DbErr>> {
    Box::pin(async move {
        find_application_for_did(&db, &community_authority, &community_uri, &did).await
    })
}

fn application_event(
    event: &'static str,
    community: String,
    app: Application,
) -> ColibriServerEvent {
    ColibriServerEvent {
        event_type: String::from("application_event"),
        data: Some(ColibriServerEventData::Application(ApplicationEventData {
            event: String::from(event),
            community,
            membership: app.membership,
            did: Some(app.did),
            handle: None,
            created_at: None,
            data: None,
        })),
        is_relevant: true,
    }
}

#[allow(clippy::too_many_arguments)]
async fn dismiss_application_with(
    community_uri: String,
    did: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: &VerifyAuthFn,
    load_authz_fn: &LoadAuthzFn,
    dismiss_fn: &DismissFn,
    find_application_fn: &FindApplicationFn,
    broadcast_fn: &BroadcastFn,
) -> Result<Json<DismissApplicationResponse>, ErrorResponse> {
    let did_for_body = did.clone();

    with_community_authz(
        auth,
        "social.colibri.community.dismissApplication",
        community_uri,
        Some(Permission::ApprovalManage),
        db,
        verify_auth_fn,
        load_authz_fn,
        move |ctx, db| async move {
            dismiss_fn(db.clone(), ctx.community.authority.clone(), did.clone()).await?;

            if let Some(app) = find_application_fn(
                db,
                ctx.community.authority.clone(),
                ctx.community_uri.clone(),
                did.clone(),
            )
            .await?
            {
                broadcast_fn(application_event("dismiss", ctx.community_uri.clone(), app));
            }

            Ok(Json(DismissApplicationResponse {
                did: did_for_body,
                community: ctx.community_uri.clone(),
            }))
        },
    )
    .await
}

#[post("/xrpc/social.colibri.community.dismissApplication?<community>&<did>&<auth>")]
/// Hides a pending join application from the active moderation queue.
/// Off-protocol — see module docs. Requires `approval.manage`.
pub async fn dismiss_application(
    community: &str,
    did: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
) -> Result<Json<DismissApplicationResponse>, ErrorResponse> {
    let sender = bridge.applications.clone();
    dismiss_application_with(
        community.to_string(),
        did.to_string(),
        auth.to_string(),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        &dismiss_boxed,
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
                online_state: String::from("offline"),
                status: ActorStatus {
                    text: String::new(),
                    emoji: None,
                },
            },
        }
    }

    #[tokio::test]
    async fn dismisses_and_broadcasts_when_caller_has_permission() {
        let db = mock_db();
        let dismiss_captured: Arc<Mutex<Option<(String, String)>>> = Arc::new(Mutex::new(None));
        let dismiss_clone = dismiss_captured.clone();
        let broadcast_captured: Arc<Mutex<Option<ColibriServerEvent>>> = Arc::new(Mutex::new(None));
        let broadcast_clone = broadcast_captured.clone();

        let result = dismiss_application_with(
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
                let captured = dismiss_clone.clone();
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
        let (community_authority, did) = dismiss_captured.lock().unwrap().take().unwrap();
        assert_eq!(community_authority, "did:plc:owner");
        assert_eq!(did, "did:plc:applicant");

        let event = broadcast_captured.lock().unwrap().take().unwrap();
        assert_eq!(event.event_type, "application_event");
        if let Some(crate::lib::events::ColibriServerEventData::Application(data)) = event.data {
            assert_eq!(data.event, "dismiss");
            assert_eq!(data.did.as_deref(), Some("did:plc:applicant"));
        } else {
            panic!("expected application_event payload");
        }
    }

    #[tokio::test]
    async fn rejects_when_caller_lacks_permission() {
        let db = mock_db();
        let result = dismiss_application_with(
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
            &|_, _, _| Box::pin(async { panic!("should not dismiss when permission missing") }),
            &|_, _, _, _| {
                Box::pin(async { panic!("should not find application when permission missing") })
            },
            &|_| panic!("should not broadcast when permission missing"),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn skips_broadcast_when_no_pending_application_found() {
        let db = mock_db();
        let broadcast_called = Arc::new(Mutex::new(false));
        let broadcast_clone = broadcast_called.clone();

        let result = dismiss_application_with(
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
            &|_, _, _| Box::pin(async { Ok(()) }),
            &|_, _, _, _| Box::pin(async { Ok(None) }),
            &move |_| {
                *broadcast_clone.lock().unwrap() = true;
            },
        )
        .await;

        assert!(result.is_ok());
        assert!(!*broadcast_called.lock().unwrap());
    }
}
