use futures::future::BoxFuture;
use rand::Rng;
use rand::distributions::Alphanumeric;
use rocket::serde::json::Json;
use rocket::{State, get, post};
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, sea_query,
};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::community_authz::{self, ActorAuthz};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError};
use crate::lib::time::current_iso8601_utc;
use crate::models::community_invitations::{
    self, ActiveModel as InvitationModel, Entity as Invitations, Model as Invitation,
};

#[derive(Serialize, Debug)]
pub struct InvitationView {
    pub code: String,
    pub community: String,
    #[serde(rename = "createdBy")]
    pub created_by: String,
    pub active: bool,
}

#[derive(Serialize, Debug)]
pub struct InvitationListResponse {
    pub codes: Vec<InvitationView>,
}

#[derive(Serialize, Debug)]
pub struct DeleteInvitationResponse {
    pub code: String,
}

impl From<Invitation> for InvitationView {
    fn from(value: Invitation) -> Self {
        InvitationView {
            code: value.code,
            community: value.community_uri,
            created_by: value.created_by,
            active: value.active,
        }
    }
}

const CODE_LENGTH: usize = 16;

fn generate_invitation_code() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(CODE_LENGTH)
        .map(char::from)
        .collect()
}

// ---- DB helpers ----------------------------------------------------------

pub async fn insert_invitation(
    db: &DatabaseConnection,
    code: String,
    community_uri: String,
    created_by: String,
) -> Result<Invitation, DbErr> {
    let row = InvitationModel {
        code: ActiveValue::Set(code.clone()),
        community_uri: ActiveValue::Set(community_uri),
        created_by: ActiveValue::Set(created_by),
        active: ActiveValue::Set(true),
        created_at: ActiveValue::Set(current_iso8601_utc()),
    };
    Invitations::insert(row).exec(db).await?;
    Invitations::find()
        .filter(community_invitations::Column::Code.eq(&code))
        .one(db)
        .await?
        .ok_or_else(|| DbErr::Custom(format!("inserted invitation missing: {code}")))
}

pub async fn fetch_invitation(
    db: &DatabaseConnection,
    code: &str,
) -> Result<Option<Invitation>, DbErr> {
    Invitations::find()
        .filter(community_invitations::Column::Code.eq(code))
        .one(db)
        .await
}

pub async fn fetch_invitations_for_community(
    db: &DatabaseConnection,
    community_uri: &str,
) -> Result<Vec<Invitation>, DbErr> {
    Invitations::find()
        .filter(community_invitations::Column::CommunityUri.eq(community_uri))
        .all(db)
        .await
}

pub async fn deactivate_invitation(db: &DatabaseConnection, code: &str) -> Result<u64, DbErr> {
    let res = Invitations::update_many()
        .col_expr(
            community_invitations::Column::Active,
            sea_query::Expr::value(false),
        )
        .filter(community_invitations::Column::Code.eq(code))
        .exec(db)
        .await?;
    Ok(res.rows_affected)
}

// ---- createInvitation ----------------------------------------------------

#[derive(Serialize, Debug)]
pub struct CreateInvitationResponse {
    pub code: String,
    pub community: String,
    #[serde(rename = "createdBy")]
    pub created_by: String,
    pub active: bool,
}

async fn create_invitation_with<V, W, I>(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: V,
    load_authz_fn: W,
    insert_fn: I,
    code_generator: fn() -> String,
) -> Result<Json<CreateInvitationResponse>, ErrorResponse>
where
    V: Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>,
    W: Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<ActorAuthz, DbErr>>,
    I: Fn(
        DatabaseConnection,
        String,
        String,
        String,
    ) -> BoxFuture<'static, Result<Invitation, DbErr>>,
{
    if AtUri::parse(&community_uri).is_none() {
        return Err(invalid_community());
    }

    let caller_did = verify_auth_fn(
        auth,
        String::from("social.colibri.community.createInvitation"),
    )
    .await
    .map_err(auth_error)?;

    let authz = load_authz_fn(db.clone(), community_uri.clone(), caller_did.clone()).await?;
    require_permission(&authz, Permission::InvitationCreate)?;

    let code = code_generator();
    let inv = insert_fn(db, code, community_uri, caller_did).await?;

    Ok(Json(CreateInvitationResponse {
        code: inv.code,
        community: inv.community_uri,
        created_by: inv.created_by,
        active: inv.active,
    }))
}

// ---- getInvitation -------------------------------------------------------

async fn get_invitation_with<F>(
    code: String,
    fetch_fn: F,
) -> Result<Json<InvitationView>, ErrorResponse>
where
    F: FnOnce(String) -> BoxFuture<'static, Result<Option<Invitation>, DbErr>>,
{
    let row = fetch_fn(code.clone()).await?.ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("NotFound"),
            message: format!("Invitation '{code}' not found."),
        }),
    })?;
    Ok(Json(row.into()))
}

// ---- listInvitations -----------------------------------------------------

async fn list_invitations_with<V, W, F>(
    community_uri: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: V,
    load_authz_fn: W,
    fetch_fn: F,
) -> Result<Json<InvitationListResponse>, ErrorResponse>
where
    V: Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>,
    W: Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<ActorAuthz, DbErr>>,
    F: Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<Invitation>, DbErr>>,
{
    if AtUri::parse(&community_uri).is_none() {
        return Err(invalid_community());
    }

    let caller_did = verify_auth_fn(
        auth,
        String::from("social.colibri.community.listInvitations"),
    )
    .await
    .map_err(auth_error)?;

    let authz = load_authz_fn(db.clone(), community_uri.clone(), caller_did).await?;
    require_permission(&authz, Permission::InvitationCreate)?;

    let rows = fetch_fn(db, community_uri).await?;
    Ok(Json(InvitationListResponse {
        codes: rows.into_iter().map(Into::into).collect(),
    }))
}

// ---- deleteInvitation ----------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn delete_invitation_with<V, W, F, D>(
    community_uri: String,
    code: String,
    auth: String,
    db: DatabaseConnection,
    verify_auth_fn: V,
    load_authz_fn: W,
    fetch_fn: F,
    deactivate_fn: D,
) -> Result<Json<DeleteInvitationResponse>, ErrorResponse>
where
    V: Fn(String, String) -> BoxFuture<'static, Result<String, ServiceAuthError>>,
    W: Fn(DatabaseConnection, String, String) -> BoxFuture<'static, Result<ActorAuthz, DbErr>>,
    F: Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Option<Invitation>, DbErr>>,
    D: Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<u64, DbErr>>,
{
    if AtUri::parse(&community_uri).is_none() {
        return Err(invalid_community());
    }

    let caller_did = verify_auth_fn(
        auth,
        String::from("social.colibri.community.deleteInvitation"),
    )
    .await
    .map_err(auth_error)?;

    let authz = load_authz_fn(db.clone(), community_uri.clone(), caller_did).await?;
    require_permission(&authz, Permission::InvitationDelete)?;

    let invitation = fetch_fn(db.clone(), code.clone())
        .await?
        .ok_or_else(|| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("NotFound"),
                message: format!("Invitation '{code}' not found."),
            }),
        })?;

    if invitation.community_uri != community_uri {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Invitation does not belong to the specified community."),
            }),
        });
    }

    deactivate_fn(db, code.clone()).await?;
    Ok(Json(DeleteInvitationResponse { code }))
}

// ---- shared error helpers ------------------------------------------------

fn auth_error(err: ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

fn invalid_community() -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid community AT-URI."),
        }),
    }
}

fn require_permission(authz: &ActorAuthz, permission: Permission) -> Result<(), ErrorResponse> {
    if !authz.has(permission, None) {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("Forbidden"),
                message: format!("Missing permission: {permission}"),
            }),
        });
    }
    Ok(())
}

// ---- Boxed dependencies --------------------------------------------------

fn verify_auth_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<String, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn load_authz_boxed(
    db: DatabaseConnection,
    community_uri: String,
    did: String,
) -> BoxFuture<'static, Result<ActorAuthz, DbErr>> {
    Box::pin(async move { community_authz::load_actor_authz(&db, &community_uri, &did).await })
}

fn insert_boxed(
    db: DatabaseConnection,
    code: String,
    community_uri: String,
    created_by: String,
) -> BoxFuture<'static, Result<Invitation, DbErr>> {
    Box::pin(async move { insert_invitation(&db, code, community_uri, created_by).await })
}

fn fetch_boxed(
    db: DatabaseConnection,
    code: String,
) -> BoxFuture<'static, Result<Option<Invitation>, DbErr>> {
    Box::pin(async move { fetch_invitation(&db, &code).await })
}

fn fetch_by_community_boxed(
    db: DatabaseConnection,
    community_uri: String,
) -> BoxFuture<'static, Result<Vec<Invitation>, DbErr>> {
    Box::pin(async move { fetch_invitations_for_community(&db, &community_uri).await })
}

fn deactivate_boxed(
    db: DatabaseConnection,
    code: String,
) -> BoxFuture<'static, Result<u64, DbErr>> {
    Box::pin(async move { deactivate_invitation(&db, &code).await })
}

// ---- Public Rocket routes -----------------------------------------------

#[post("/xrpc/social.colibri.community.createInvitation?<community>&<auth>")]
pub async fn create_invitation(
    community: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<CreateInvitationResponse>, ErrorResponse> {
    create_invitation_with(
        community.to_string(),
        auth.to_string(),
        db.inner().clone(),
        verify_auth_boxed,
        load_authz_boxed,
        insert_boxed,
        generate_invitation_code,
    )
    .await
}

#[get("/xrpc/social.colibri.community.getInvitation?<code>")]
pub async fn get_invitation(
    code: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<InvitationView>, ErrorResponse> {
    let db = db.inner().clone();
    get_invitation_with(code.to_string(), move |c| {
        Box::pin(async move { fetch_invitation(&db, &c).await })
    })
    .await
}

#[post("/xrpc/social.colibri.community.listInvitations?<uri>&<auth>")]
pub async fn list_invitations(
    uri: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<InvitationListResponse>, ErrorResponse> {
    list_invitations_with(
        uri.to_string(),
        auth.to_string(),
        db.inner().clone(),
        verify_auth_boxed,
        load_authz_boxed,
        fetch_by_community_boxed,
    )
    .await
}

#[post("/xrpc/social.colibri.community.deleteInvitation?<uri>&<code>&<auth>")]
pub async fn delete_invitation(
    uri: &str,
    code: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<DeleteInvitationResponse>, ErrorResponse> {
    delete_invitation_with(
        uri.to_string(),
        code.to_string(),
        auth.to_string(),
        db.inner().clone(),
        verify_auth_boxed,
        load_authz_boxed,
        fetch_boxed,
        deactivate_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};
    use std::sync::{Arc, Mutex};

    fn invite(code: &str, community: &str, by: &str, active: bool) -> Invitation {
        Invitation {
            code: code.to_string(),
            community_uri: community.to_string(),
            created_by: by.to_string(),
            active,
            created_at: String::from("2026-05-14T00:00:00Z"),
        }
    }

    fn owner_authz() -> ActorAuthz {
        ActorAuthz {
            is_owner: true,
            member: None,
            roles: vec![],
        }
    }

    fn empty_authz() -> ActorAuthz {
        ActorAuthz {
            is_owner: false,
            member: None,
            roles: vec![],
        }
    }

    #[tokio::test]
    async fn create_invitation_returns_inserted_view() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let captured: Arc<Mutex<Option<(String, String, String)>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();

        let result = create_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            |_, _, _| Box::pin(async { Ok(owner_authz()) }),
            move |_, code, community, created_by| {
                let captured = captured_clone.clone();
                Box::pin(async move {
                    *captured.lock().unwrap() =
                        Some((code.clone(), community.clone(), created_by.clone()));
                    Ok(invite(&code, &community, &created_by, true))
                })
            },
            || String::from("CODE-FIXED"),
        )
        .await
        .unwrap();

        let inserted = captured.lock().unwrap().take().unwrap();
        assert_eq!(inserted.0, "CODE-FIXED");
        assert_eq!(inserted.1, "at://did:plc:owner/social.colibri.community/c1");
        assert_eq!(inserted.2, "did:plc:owner");

        assert_eq!(result.code, "CODE-FIXED");
        assert!(result.active);
        assert_eq!(result.created_by, "did:plc:owner");
    }

    #[tokio::test]
    async fn create_invitation_rejects_without_permission() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = create_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:rando")) }),
            |_, _, _| Box::pin(async { Ok(empty_authz()) }),
            |_, _, _, _| Box::pin(async { panic!("should not insert") }),
            || String::from("CODE"),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "Forbidden");
    }

    #[tokio::test]
    async fn get_invitation_returns_view() {
        let result = get_invitation_with(String::from("CODE"), |c| {
            Box::pin(async move {
                Ok(Some(invite(
                    &c,
                    "at://did:plc:owner/social.colibri.community/c1",
                    "did:plc:owner",
                    true,
                )))
            })
        })
        .await
        .unwrap();

        assert_eq!(result.code, "CODE");
        assert_eq!(
            result.community,
            "at://did:plc:owner/social.colibri.community/c1"
        );
        assert!(result.active);
    }

    #[tokio::test]
    async fn get_invitation_returns_not_found_when_missing() {
        let result =
            get_invitation_with(String::from("NOPE"), |_| Box::pin(async { Ok(None) })).await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "NotFound");
    }

    #[tokio::test]
    async fn list_invitations_returns_all_for_community() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = list_invitations_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            |_, _, _| Box::pin(async { Ok(owner_authz()) }),
            |_, _| {
                Box::pin(async {
                    Ok(vec![
                        invite(
                            "A",
                            "at://did:plc:owner/social.colibri.community/c1",
                            "did:plc:owner",
                            true,
                        ),
                        invite(
                            "B",
                            "at://did:plc:owner/social.colibri.community/c1",
                            "did:plc:owner",
                            false,
                        ),
                    ])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.codes.len(), 2);
        assert_eq!(result.codes[0].code, "A");
        assert!(result.codes[0].active);
        assert!(!result.codes[1].active);
    }

    #[tokio::test]
    async fn delete_invitation_deactivates_when_authorized() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
        let calls_clone = calls.clone();

        let result = delete_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("CODE"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            |_, _, _| Box::pin(async { Ok(owner_authz()) }),
            |_, c| {
                Box::pin(async move {
                    Ok(Some(invite(
                        &c,
                        "at://did:plc:owner/social.colibri.community/c1",
                        "did:plc:owner",
                        true,
                    )))
                })
            },
            move |_, c| {
                let calls = calls_clone.clone();
                Box::pin(async move {
                    calls.lock().unwrap().push(c);
                    Ok(1)
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.code, "CODE");
        assert_eq!(*calls.lock().unwrap(), vec![String::from("CODE")]);
    }

    #[tokio::test]
    async fn delete_invitation_rejects_when_community_mismatches() {
        let db = MockDatabase::new(DatabaseBackend::Postgres).into_connection();
        let result = delete_invitation_with(
            String::from("at://did:plc:owner/social.colibri.community/c1"),
            String::from("CODE"),
            String::from("token"),
            db,
            |_, _| Box::pin(async { Ok(String::from("did:plc:owner")) }),
            |_, _, _| Box::pin(async { Ok(owner_authz()) }),
            |_, c| {
                Box::pin(async move {
                    Ok(Some(invite(
                        &c,
                        "at://did:plc:other/social.colibri.community/c2",
                        "did:plc:other",
                        true,
                    )))
                })
            },
            |_, _| Box::pin(async { panic!("should not deactivate when community mismatches") }),
        )
        .await;

        assert!(result.is_err());
        let err = result.err().unwrap().body.into_inner();
        assert_eq!(err.error, "InvalidRequest");
    }

    #[test]
    fn invitation_codes_are_unique_alphanumeric_strings_of_expected_length() {
        let a = generate_invitation_code();
        let b = generate_invitation_code();
        assert_eq!(a.len(), CODE_LENGTH);
        assert_eq!(b.len(), CODE_LENGTH);
        assert_ne!(a, b);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
