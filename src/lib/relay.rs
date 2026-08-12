use std::future::Future;

use futures::future::BoxFuture;
use rocket::Request;
use rocket::http::uri::Origin;
use rocket::request::{FromRequest, Outcome};
use rocket::serde::DeserializeOwned;
use rocket::serde::json::Json;
use sea_orm::{DatabaseConnection, DbErr};

use crate::lib::at_uri::AtUri;
use crate::lib::community_hub::{self, HubRouting};
use crate::lib::handler::{
    CallerContext, LoadAuthzFn, auth_error, forbidden, invalid_community_uri,
};
use crate::lib::peer_client::{self, ForwardXrpcFn, PeerBody, PeerReply, RelayCall};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorCode, ErrorResponse};
use crate::lib::service_auth::{self, ServiceAuthError, VerifiedCaller};

#[derive(Debug, Clone)]
pub struct RelayContext {
    pub method: String,
    pub path_and_query: String,
}

#[derive(Debug, Clone)]
pub struct RelayRequest {
    pub context: RelayContext,
    pub body: Option<PeerBody>,
}

impl RelayContext {
    pub fn with_body(self, body: PeerBody) -> RelayRequest {
        RelayRequest {
            context: self,
            body: Some(body),
        }
    }
}

impl From<RelayContext> for RelayRequest {
    fn from(context: RelayContext) -> Self {
        Self {
            context,
            body: None,
        }
    }
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for RelayContext {
    type Error = std::convert::Infallible;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        Outcome::Success(RelayContext {
            method: req.method().as_str().to_string(),
            path_and_query: without_auth(req.uri()),
        })
    }
}

fn without_auth(uri: &Origin<'_>) -> String {
    let Some(query) = uri.query() else {
        return uri.path().to_string();
    };

    let kept: Vec<&str> = query
        .as_str()
        .split('&')
        .filter(|segment| !segment.is_empty())
        .filter(|segment| segment.split('=').next() != Some("auth"))
        .collect();

    if kept.is_empty() {
        return uri.path().to_string();
    }

    format!("{}?{}", uri.path(), kept.join("&"))
}

pub type VerifyDelegatedFn = dyn Fn(String, String) -> BoxFuture<'static, Result<VerifiedCaller, ServiceAuthError>>
    + Send
    + Sync;
pub type RoutingFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<HubRouting, DbErr>>
    + Send
    + Sync;
pub type DeclaredAppViewFn = dyn Fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Option<String>, DbErr>>
    + Send
    + Sync;

pub fn verify_delegated_boxed(
    auth: String,
    lxm: String,
) -> BoxFuture<'static, Result<VerifiedCaller, ServiceAuthError>> {
    Box::pin(async move { service_auth::verify_delegated_auth(&auth, &lxm).await })
}

pub fn routing_boxed(
    db: DatabaseConnection,
    community_did: String,
) -> BoxFuture<'static, Result<HubRouting, DbErr>> {
    Box::pin(async move { community_hub::resolve_routing(&db, &community_did).await })
}

pub fn declared_appview_boxed(
    db: DatabaseConnection,
    actor: String,
) -> BoxFuture<'static, Result<Option<String>, DbErr>> {
    Box::pin(async move { community_hub::declared_appview(&db, &actor).await })
}

#[derive(Clone, Copy)]
pub struct WriteDeps<'a> {
    pub verify_delegated_fn: &'a VerifyDelegatedFn,
    pub load_authz_fn: &'a LoadAuthzFn,
    pub routing_fn: &'a RoutingFn,
    pub declared_appview_fn: &'a DeclaredAppViewFn,
    pub forward_fn: &'a ForwardXrpcFn,
}

impl WriteDeps<'static> {
    pub fn production() -> Self {
        Self {
            verify_delegated_fn: &verify_delegated_boxed,
            load_authz_fn: &crate::lib::handler::load_authz_boxed,
            routing_fn: &routing_boxed,
            declared_appview_fn: &declared_appview_boxed,
            forward_fn: &peer_client::forward_xrpc_boxed,
        }
    }
}

enum Disposition {
    Local(String),
    Relay { hub: String, act: String },
}

async fn disposition(
    auth: String,
    lxm: &'static str,
    community_did: String,
    db: &DatabaseConnection,
    deps: &WriteDeps<'_>,
) -> Result<Disposition, ErrorResponse> {
    let VerifiedCaller { iss, act } = (deps.verify_delegated_fn)(auth, lxm.to_string())
        .await
        .map_err(auth_error)?;

    let routing = (deps.routing_fn)(db.clone(), community_did).await?;

    match (routing, act) {
        (HubRouting::Local, Some(actor)) => {
            let declared = (deps.declared_appview_fn)(db.clone(), actor.clone()).await?;
            if declared.as_deref() != Some(iss.as_str()) {
                return Err(ErrorCode::AppViewNotAuthorized.with(
                    "This account has not published the AppView that sent this request as \
                     authorized to act for it. Turn on presence sharing to publish it.",
                ));
            }
            Ok(Disposition::Local(actor))
        }

        (HubRouting::Local, None) => Ok(Disposition::Local(iss)),

        (HubRouting::Remote(hub), Some(_)) => Err(ErrorCode::NotCommunityHub
            .with(format!("This community is administered by {hub}."))
            .with_hub(hub)),

        (HubRouting::Remote(hub), None) => Ok(Disposition::Relay { hub, act: iss }),
    }
}

fn from_reply<R: DeserializeOwned>(reply: PeerReply) -> Result<Json<R>, ErrorResponse> {
    if !(200..300).contains(&reply.status) {
        return Err(ErrorResponse::from_peer(&reply.body));
    }

    serde_json::from_slice::<R>(&reply.body)
        .map(Json)
        .map_err(|e| {
            log::warn!("could not read a relayed response from the community's AppView: {e}");
            ErrorCode::UpstreamFailure.with(
                "The AppView administering this community answered in a shape we did not \
                 recognise. It may be running a different version.",
            )
        })
}

async fn relay<R: DeserializeOwned>(
    request: RelayRequest,
    lxm: &'static str,
    hub: String,
    act: String,
    deps: &WriteDeps<'_>,
) -> Result<Json<R>, ErrorResponse> {
    let reply = (deps.forward_fn)(RelayCall {
        hub_did: hub,
        lxm: lxm.to_string(),
        method: request.context.method,
        path_and_query: request.context.path_and_query,
        act,
        body: request.body,
    })
    .await
    .map_err(|e| {
        log::warn!("relaying {lxm} to the community's AppView failed: {e}");
        ErrorCode::PdsUnavailable
            .with("Could not reach the AppView that administers this community.")
    })?;

    from_reply(reply)
}

#[allow(clippy::too_many_arguments)]
pub async fn with_community_write<F, Fut, R>(
    request: impl Into<RelayRequest>,
    auth: String,
    lxm: &'static str,
    community_uri: String,
    permission: Option<Permission>,
    channel_rkey: Option<&str>,
    db: DatabaseConnection,
    deps: &WriteDeps<'_>,
    body: F,
) -> Result<Json<R>, ErrorResponse>
where
    F: FnOnce(CallerContext, DatabaseConnection) -> Fut,
    Fut: Future<Output = Result<Json<R>, ErrorResponse>>,
    R: DeserializeOwned,
{
    let community = AtUri::parse(&community_uri).ok_or_else(invalid_community_uri)?;

    let caller = disposition(auth, lxm, community.authority.clone(), &db, deps).await?;

    let caller_did = match caller {
        Disposition::Relay { hub, act } => {
            return relay(request.into(), lxm, hub, act, deps).await;
        }
        Disposition::Local(did) => did,
    };

    let authz = (deps.load_authz_fn)(db.clone(), community_uri.clone(), caller_did.clone()).await?;

    if let Some(perm) = permission
        && !authz.has(perm, channel_rkey)
    {
        return Err(forbidden(perm));
    }

    body(
        CallerContext {
            caller_did,
            community,
            community_uri,
            authz,
        },
        db,
    )
    .await
}

pub async fn with_authenticated_write<F, Fut, R>(
    request: impl Into<RelayRequest>,
    auth: String,
    lxm: &'static str,
    community_did: String,
    db: DatabaseConnection,
    deps: &WriteDeps<'_>,
    body: F,
) -> Result<Json<R>, ErrorResponse>
where
    F: FnOnce(String, DatabaseConnection) -> Fut,
    Fut: Future<Output = Result<Json<R>, ErrorResponse>>,
    R: DeserializeOwned,
{
    match disposition(auth, lxm, community_did, &db, deps).await? {
        Disposition::Relay { hub, act } => relay(request.into(), lxm, hub, act, deps).await,
        Disposition::Local(caller_did) => body(caller_did, db).await,
    }
}

#[cfg(test)]
mod tests {
    use rocket::tokio;

    use super::*;
    use crate::lib::test_fixtures::{mock_db, owner_authz};

    const ME: &str = "did:web:mine.example";
    const THEM: &str = "did:web:theirs.example";
    const USER: &str = "did:plc:user";
    const COMMUNITY_URI: &str = "at://did:plc:community/social.colibri.community/self";
    const LXM: &str = "social.colibri.community.banUser";

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
    struct Reply {
        ok: bool,
    }

    fn context() -> RelayContext {
        RelayContext {
            method: String::from("POST"),
            path_and_query: String::from("/xrpc/social.colibri.community.banUser?community=x"),
        }
    }

    fn caller(iss: &'static str, act: Option<&'static str>) -> Box<VerifyDelegatedFn> {
        Box::new(move |_, _| {
            Box::pin(async move {
                Ok(VerifiedCaller {
                    iss: String::from(iss),
                    act: act.map(String::from),
                })
            })
        })
    }

    fn routing(result: HubRouting) -> Box<RoutingFn> {
        Box::new(move |_, _| {
            let result = result.clone();
            Box::pin(async move { Ok(result) })
        })
    }

    fn declared(value: Option<&'static str>) -> Box<DeclaredAppViewFn> {
        Box::new(move |_, _| Box::pin(async move { Ok(value.map(String::from)) }))
    }

    fn authz() -> Box<LoadAuthzFn> {
        Box::new(|_, _, _| Box::pin(async { Ok(owner_authz()) }))
    }

    fn replies(status: u16, body: &'static str) -> Box<ForwardXrpcFn> {
        Box::new(move |_| {
            Box::pin(async move {
                Ok(PeerReply {
                    status,
                    body: body.as_bytes().to_vec(),
                })
            })
        })
    }

    type Recorded = std::sync::Arc<std::sync::Mutex<Option<RelayCall>>>;

    fn recording(recorded: Recorded) -> Box<ForwardXrpcFn> {
        Box::new(move |call| {
            *recorded.lock().unwrap() = Some(call);
            Box::pin(async move {
                Ok(PeerReply {
                    status: 200,
                    body: br#"{"ok":true}"#.to_vec(),
                })
            })
        })
    }

    async fn run(
        verify: Box<VerifyDelegatedFn>,
        route: Box<RoutingFn>,
        declared_fn: Box<DeclaredAppViewFn>,
        forward: Box<ForwardXrpcFn>,
    ) -> Result<Json<Reply>, ErrorResponse> {
        let load = authz();
        let deps = WriteDeps {
            verify_delegated_fn: &*verify,
            load_authz_fn: &*load,
            routing_fn: &*route,
            declared_appview_fn: &*declared_fn,
            forward_fn: &*forward,
        };

        with_community_write(
            context(),
            String::from("token"),
            LXM,
            String::from(COMMUNITY_URI),
            None,
            None,
            mock_db(),
            &deps,
            |_ctx: CallerContext, _db| async { Ok(Json(Reply { ok: false })) },
        )
        .await
    }

    #[tokio::test]
    async fn a_local_community_runs_the_body() {
        let reply = run(
            caller(USER, None),
            routing(HubRouting::Local),
            declared(None),
            replies(500, "unused"),
        )
        .await
        .expect("a local write should run");

        assert_eq!(reply.0, Reply { ok: false });
    }

    #[tokio::test]
    async fn a_remote_community_relays_and_returns_the_hubs_answer() {
        let recorded: Recorded = Default::default();
        let forward = recording(recorded.clone());

        let reply = run(
            caller(USER, None),
            routing(HubRouting::Remote(String::from(THEM))),
            declared(None),
            forward,
        )
        .await
        .expect("a remote write should relay");

        assert_eq!(reply.0, Reply { ok: true });

        let call = recorded.lock().unwrap().clone().expect("relay was called");
        assert_eq!(call.hub_did, THEM);
        assert_eq!(call.lxm, LXM);
        assert_eq!(call.act, USER);
    }

    #[tokio::test]
    async fn a_delegated_request_is_honoured_when_the_actor_named_the_sender() {
        let reply = run(
            caller(THEM, Some(USER)),
            routing(HubRouting::Local),
            declared(Some(THEM)),
            replies(500, "unused"),
        )
        .await
        .expect("a properly delegated write should run");

        assert_eq!(reply.0, Reply { ok: false });
    }

    #[tokio::test]
    async fn a_delegated_request_is_refused_when_the_actor_named_nobody() {
        let err = run(
            caller(THEM, Some(USER)),
            routing(HubRouting::Local),
            declared(None),
            replies(500, "unused"),
        )
        .await
        .expect_err("an undeclared actor must be refused");

        assert_eq!(err.code, ErrorCode::AppViewNotAuthorized);
    }

    #[tokio::test]
    async fn a_delegated_request_is_refused_when_the_actor_named_somebody_else() {
        let err = run(
            caller(THEM, Some(USER)),
            routing(HubRouting::Local),
            declared(Some(ME)),
            replies(500, "unused"),
        )
        .await
        .expect_err("a mismatched declaration must be refused");

        assert_eq!(err.code, ErrorCode::AppViewNotAuthorized);
    }

    #[tokio::test]
    async fn a_relayed_request_is_never_relayed_onward() {
        let recorded: Recorded = Default::default();
        let forward = recording(recorded.clone());

        let err = run(
            caller(THEM, Some(USER)),
            routing(HubRouting::Remote(String::from(ME))),
            declared(Some(THEM)),
            forward,
        )
        .await
        .expect_err("a second hop must be refused");

        assert_eq!(err.code, ErrorCode::NotCommunityHub);
        assert_eq!(err.body.hub.as_deref(), Some(ME));
        assert!(recorded.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_hubs_error_is_relayed_with_its_own_code() {
        let err = run(
            caller(USER, None),
            routing(HubRouting::Remote(String::from(THEM))),
            declared(None),
            replies(
                403,
                r#"{"error":"Forbidden","message":"Missing permission: member.ban"}"#,
            ),
        )
        .await
        .expect_err("the hub refused");

        assert_eq!(err.code, ErrorCode::Forbidden);
        assert_eq!(err.body.message, "Missing permission: member.ban");
    }

    #[tokio::test]
    async fn an_unreadable_hub_response_is_an_upstream_failure() {
        let err = run(
            caller(USER, None),
            routing(HubRouting::Remote(String::from(THEM))),
            declared(None),
            replies(200, r#"{"unexpected":1}"#),
        )
        .await
        .expect_err("a shape we cannot read must not look like success");

        assert_eq!(err.code, ErrorCode::UpstreamFailure);
    }

    #[tokio::test]
    async fn an_unreachable_hub_is_reported_as_unavailable() {
        let forward: Box<ForwardXrpcFn> = Box::new(|_| {
            Box::pin(async {
                Err(peer_client::PeerError::Transport(String::from(
                    "connection refused",
                )))
            })
        });

        let err = run(
            caller(USER, None),
            routing(HubRouting::Remote(String::from(THEM))),
            declared(None),
            forward,
        )
        .await
        .expect_err("an unreachable hub must surface");

        assert_eq!(err.code, ErrorCode::PdsUnavailable);
    }

    #[tokio::test]
    async fn an_invalid_community_uri_is_refused_before_anything_else() {
        let verify = caller(USER, None);
        let route = routing(HubRouting::Local);
        let declared_fn = declared(None);
        let forward = replies(500, "unused");
        let load = authz();
        let deps = WriteDeps {
            verify_delegated_fn: &*verify,
            load_authz_fn: &*load,
            routing_fn: &*route,
            declared_appview_fn: &*declared_fn,
            forward_fn: &*forward,
        };

        let err = with_community_write::<_, _, Reply>(
            context(),
            String::from("token"),
            LXM,
            String::from("not-an-at-uri"),
            None,
            None,
            mock_db(),
            &deps,
            |_ctx: CallerContext, _db| async { Ok(Json(Reply { ok: false })) },
        )
        .await
        .expect_err("a malformed community URI must be refused");

        assert_eq!(err.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn the_inbound_auth_token_is_never_forwarded() {
        let uri = Origin::parse(
            "/xrpc/social.colibri.community.banUser?community=at%3A%2F%2Fx&auth=secret-jwt&identifier=bob",
        )
        .unwrap();
        let forwarded = without_auth(&uri);

        assert!(!forwarded.contains("secret-jwt"), "{forwarded}");
        assert!(!forwarded.contains("auth="), "{forwarded}");
        assert!(forwarded.contains("community=at%3A%2F%2Fx"));
        assert!(forwarded.contains("identifier=bob"));
    }

    #[test]
    fn a_query_of_only_auth_leaves_a_bare_path() {
        let uri = Origin::parse("/xrpc/social.colibri.community.leave?auth=secret").unwrap();
        assert_eq!(without_auth(&uri), "/xrpc/social.colibri.community.leave");
    }

    #[test]
    fn a_queryless_uri_is_left_alone() {
        let uri = Origin::parse("/xrpc/social.colibri.community.leave").unwrap();
        assert_eq!(without_auth(&uri), "/xrpc/social.colibri.community.leave");
    }
}
