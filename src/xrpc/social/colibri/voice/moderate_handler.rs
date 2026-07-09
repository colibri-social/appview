use rocket::serde::json::Json;
use rocket::{State, post};
use sea_orm::DatabaseConnection;
use serde::Serialize;

use crate::lib::handler::{load_authz_boxed, verify_auth_boxed, with_community_authz};
use crate::lib::permissions::Permission;
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::state::leave_vc;
use crate::lib::tap::CommsBridge;
use crate::lib::voice_control::{VoiceAction, VoiceControlCommand};
use crate::lib::voice_events::{broadcast_voice_presence, broadcast_voice_state};

#[derive(Serialize)]
pub struct ModerateVoiceResponse {
    pub did: String,
}

fn parse_action(action: &str) -> Result<VoiceAction, ErrorResponse> {
    match action {
        "mute" => Ok(VoiceAction::ServerMute(true)),
        "unmute" => Ok(VoiceAction::ServerMute(false)),
        "deafen" => Ok(VoiceAction::ServerDeafen(true)),
        "undeafen" => Ok(VoiceAction::ServerDeafen(false)),
        "disconnect" => Ok(VoiceAction::Disconnect),
        _ => Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("InvalidRequest"),
                message: String::from("Unknown voice moderation action."),
            }),
        }),
    }
}

#[post("/xrpc/social.colibri.voice.moderate?<community>&<channel>&<target>&<action>&<auth>")]
pub async fn moderate_voice(
    community: &str,
    channel: &str,
    target: &str,
    action: &str,
    auth: &str,
    db: &State<DatabaseConnection>,
    bridge: &State<CommsBridge>,
) -> Result<Json<ModerateVoiceResponse>, ErrorResponse> {
    let voice_action = parse_action(action)?;
    let channel_uri = channel.to_string();
    let target_did = target.to_string();
    let voice_control = bridge.voice_control.clone();
    let broadcast = bridge.broadcast.clone();

    with_community_authz(
        auth.to_string(),
        "social.colibri.voice.moderate",
        community.to_string(),
        Some(Permission::VoiceModerate),
        db.inner().clone(),
        &verify_auth_boxed,
        &load_authz_boxed,
        move |ctx, db| async move {
            let target_authz =
                load_authz_boxed(db.clone(), ctx.community_uri.clone(), target_did.clone()).await?;
            if !ctx.authz.outranks(&target_authz) {
                return Err(ErrorResponse {
                    body: Json(ErrorBody {
                        error: String::from("Forbidden"),
                        message: String::from(
                            "Cannot moderate a member with an equal or higher role position.",
                        ),
                    }),
                });
            }

            let community_did = ctx.community.authority.clone();

            let _ = voice_control.send(VoiceControlCommand {
                channel_uri: channel_uri.clone(),
                target_did: target_did.clone(),
                action: voice_action.clone(),
            });

            match voice_action {
                VoiceAction::ServerMute(muted) => {
                    broadcast_voice_state(
                        &broadcast,
                        &community_did,
                        &channel_uri,
                        &target_did,
                        None,
                        None,
                        Some(muted),
                        None,
                    );
                }
                VoiceAction::ServerDeafen(deafened) => {
                    broadcast_voice_state(
                        &broadcast,
                        &community_did,
                        &channel_uri,
                        &target_did,
                        None,
                        None,
                        None,
                        Some(deafened),
                    );
                }
                VoiceAction::Disconnect => {
                    leave_vc(target_did.clone(), &db).await;
                    broadcast_voice_presence(
                        &broadcast,
                        &community_did,
                        &channel_uri,
                        &target_did,
                        "leave",
                    );
                }
            }

            Ok(Json(ModerateVoiceResponse { did: target_did }))
        },
    )
    .await
}
