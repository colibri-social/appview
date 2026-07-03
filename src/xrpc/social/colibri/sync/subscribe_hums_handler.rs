//! `social.colibri.sync.subscribeHums` — egress-only Hum stream for peer AppViews.
//!
//! A peer AppView opens this WebSocket to receive the off-protocol Hums this
//! AppView relays in its hub role. The peer authenticates with an inter-service
//! auth JWT carried in the `Sec-WebSocket-Protocol` subprotocol (mirroring
//! `subscribeEvents`). The stream is one-directional: inbound frames are ignored
//! except for the close signal — peers push Hums via `sendHum`, never here.
//!
//! ## Need-to-know egress
//!
//! A valid peer token authenticates *who* is connecting, not *what* they may
//! see. Streaming every relayed Hum to any authenticated `did:web` would leak
//! the presence/typing/voice activity of every user in every community on this
//! hub. Instead, this AppView only forwards Hums for a community when that
//! community has a member who declared the connecting peer as their
//! `presenceService` — i.e. the peer already legitimately speaks for someone
//! there. The authorized-community set is computed at connect and refreshed
//! periodically as memberships and `presenceService` choices change.

use crate::lib::at_uri::AtUri;
use crate::lib::events::HumEnvelope;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;
use crate::lib::tap::CommsBridge;
use crate::models::record_data;
use crate::xrpc::social::colibri::sync::subscribe_events_handler::{
    AUTH_SUBPROTOCOL, ChannelWithProtocol, SubprotocolAuth,
};
use futures_util::{SinkExt, StreamExt};
use rocket::tokio::sync::broadcast::Receiver;
use rocket::tokio::sync::broadcast::error::RecvError;
use rocket::{State, get, serde::json::Json};
use rocket_ws::{Message as WsMessage, WebSocket, stream::DuplexStream};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
};
use std::collections::HashSet;
use std::time::Duration;

const MEMBER_NSID: &str = "social.colibri.member";

/// How often the connection recomputes which communities the peer may receive
/// Hums for. Bounds how long a just-departed member (or a `presenceService`
/// change) keeps a community authorized/unauthorized for the peer.
const AUTHZ_REFRESH_SECS: u64 = 30;

type WsSink = futures_util::stream::SplitSink<DuplexStream, WsMessage>;

#[derive(FromQueryResult)]
struct CommunityDidRow {
    did: String,
}

/// The set of community DIDs this AppView may stream to `peer_did`: those with
/// at least one member whose `social.colibri.actor.profile` names `peer_did` as
/// their `presenceService`. Mirrors the join `get_authorized_communities` uses,
/// keyed on the peer instead of a user.
async fn authorized_hum_communities(
    db: &DatabaseConnection,
    peer_did: &str,
) -> Result<HashSet<String>, DbErr> {
    let rows = record_data::Entity::find()
        .select_only()
        .column(record_data::Column::Did)
        .distinct()
        .filter(record_data::Column::Nsid.eq(MEMBER_NSID))
        .filter(Expr::cust_with_values(
            r#"
            EXISTS (
                SELECT 1 FROM record_data p
                WHERE "p"."nsid" = 'social.colibri.actor.profile'
                  AND "p"."rkey" = 'self'
                  AND "p"."did" = "record_data"."data"->>'subject'
                  AND "p"."data"->>'presenceService' = $1
            )
            "#,
            vec![sea_orm::Value::from(peer_did)],
        ))
        .into_model::<CommunityDidRow>()
        .all(db)
        .await?;

    Ok(rows.into_iter().map(|r| r.did).collect())
}

/// Whether a Hum for `community_uri` may be forwarded to a peer authorized for
/// `authorized`. A malformed community URI is treated as unauthorized.
fn community_authorized(community_uri: &str, authorized: &HashSet<String>) -> bool {
    AtUri::parse(community_uri).is_some_and(|uri| authorized.contains(&uri.authority))
}

/// Narrows the peer's authorized communities to those it declared interest in
/// (Option A). `authorized` is the security boundary — the intersection can only
/// shrink it, never grant a community the peer isn't authorized for. An empty
/// declaration means "no filter": stream everything authorized (back-compat for
/// peers that don't declare).
fn narrow(authorized: HashSet<String>, declared: &HashSet<String>) -> HashSet<String> {
    if declared.is_empty() {
        authorized
    } else {
        authorized
            .into_iter()
            .filter(|c| declared.contains(c))
            .collect()
    }
}

/// Forwards one relayed Hum to the peer iff they are authorized for its
/// community. Returns false only when the stream has closed.
async fn forward_hum(
    ws_sink: &mut WsSink,
    hum: Result<HumEnvelope, RecvError>,
    authorized: &HashSet<String>,
    peer_did: &str,
) -> bool {
    match hum {
        Ok(envelope) => {
            if !community_authorized(&envelope.community, authorized) {
                return true;
            }
            match serde_json::to_string(&envelope) {
                Ok(payload) => ws_sink.send(WsMessage::Text(payload)).await.is_ok(),
                Err(e) => {
                    log::warn!("failed to serialize Hum for {peer_did}: {e}");
                    true
                }
            }
        }
        Err(RecvError::Lagged(skipped)) => {
            log::warn!("Hum egress for {peer_did} lagged, skipped {skipped}");
            true
        }
        Err(RecvError::Closed) => false,
    }
}

async fn run_hum_egress_loop(
    io: DuplexStream,
    from_hums: Receiver<HumEnvelope>,
    db: DatabaseConnection,
    peer_did: String,
    declared: HashSet<String>,
) {
    let (mut ws_sink, mut ws_source) = io.split();
    let mut from_hums = from_hums;

    let load = |db: DatabaseConnection, peer: String| async move {
        authorized_hum_communities(&db, &peer)
            .await
            .unwrap_or_else(|e| {
                log::warn!("authorized-community load failed for {peer}: {e}");
                HashSet::new()
            })
    };

    let mut authorized = narrow(load(db.clone(), peer_did.clone()).await, &declared);

    let mut refresh = rocket::tokio::time::interval(Duration::from_secs(AUTHZ_REFRESH_SECS));
    // The first tick fires immediately; consume it so the periodic cadence
    // starts after one interval (we already loaded the set above).
    refresh.tick().await;

    loop {
        let connected = rocket::tokio::select! {
            hum = from_hums.recv() => forward_hum(&mut ws_sink, hum, &authorized, &peer_did).await,
            _ = refresh.tick() => {
                authorized = narrow(load(db.clone(), peer_did.clone()).await, &declared);
                true
            }
            // Egress-only: the peer sends nothing meaningful. Watch only for the
            // close/disconnect so we can wind the connection down.
            msg = ws_source.next() => !matches!(msg, Some(Ok(WsMessage::Close(_))) | None),
        };

        if !connected {
            break;
        }
    }
}

#[get("/xrpc/social.colibri.sync.subscribeHums?<auth>&<communities>")]
pub async fn subscribe_hums(
    auth: Option<&str>,
    communities: Vec<String>,
    subprotocol_auth: SubprotocolAuth,
    ws: WebSocket,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
) -> Result<ChannelWithProtocol, ErrorResponse> {
    if !crate::lib::hum_client::humming_enabled() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("NotEnabled"),
                message: String::from("Humming is disabled on this AppView"),
            }),
        });
    }

    let used_subprotocol = subprotocol_auth.token().is_some();
    let token = subprotocol_auth
        .token()
        .map(str::to_owned)
        .or_else(|| auth.map(str::to_owned))
        .unwrap_or_default();

    let peer_did = service_auth::verify_appview_auth(&token, "social.colibri.sync.subscribeHums")
        .await
        .map_err(|e| ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthRequired"),
                message: e.to_string(),
            }),
        })?;

    // Cap concurrent inbound streams so a community that unexpectedly spans many
    // AppViews can't exhaust a small self-hosted hub. The slot is released when
    // the connection ends (guard dropped inside the channel task).
    let slot = crate::lib::hum_guard::SubscriberSlot::try_acquire(
        crate::lib::hum_guard::max_subscribers(),
    )
    .ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("TooManySubscribers"),
            message: String::from("hub is at its subscribeHums connection limit"),
        }),
    })?;

    let declared: HashSet<String> = communities.into_iter().collect();
    log::info!(
        "Peer AppView subscribed to social.colibri.sync.subscribeHums: {peer_did} ({} declared communities)",
        declared.len()
    );

    let from_hums = bridge.hums.subscribe();
    let db = db.inner().clone();

    let channel = ws.channel(move |io| {
        Box::pin(async move {
            let _slot = slot;
            run_hum_egress_loop(io, from_hums, db, peer_did, declared).await;
            Ok(())
        })
    });

    Ok(ChannelWithProtocol::new(
        channel,
        used_subprotocol.then_some(AUTH_SUBPROTOCOL),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn community_authorized_matches_only_listed_communities() {
        let authorized: HashSet<String> =
            [String::from("did:plc:community-a")].into_iter().collect();

        assert!(community_authorized(
            "at://did:plc:community-a/social.colibri.community/self",
            &authorized
        ));
        assert!(!community_authorized(
            "at://did:plc:community-b/social.colibri.community/self",
            &authorized
        ));
    }

    #[test]
    fn community_authorized_rejects_malformed_uri() {
        let authorized: HashSet<String> =
            [String::from("did:plc:community-a")].into_iter().collect();
        assert!(!community_authorized("not-an-at-uri", &authorized));
    }

    #[test]
    fn empty_authorization_forwards_nothing() {
        let authorized = HashSet::new();
        assert!(!community_authorized(
            "at://did:plc:community-a/social.colibri.community/self",
            &authorized
        ));
    }

    #[test]
    fn narrow_intersects_with_declared_interest() {
        let authorized: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();

        // Declared narrows to the intersection...
        let declared: HashSet<String> = ["b", "z"].iter().map(|s| s.to_string()).collect();
        let narrowed = narrow(authorized.clone(), &declared);
        assert_eq!(narrowed, ["b"].iter().map(|s| s.to_string()).collect());

        // ...and can never widen beyond authorized (declared-only "z" is dropped).
        assert!(!narrowed.contains("z"));

        // Empty declaration = no filter.
        assert_eq!(narrow(authorized.clone(), &HashSet::new()), authorized);
    }
}
