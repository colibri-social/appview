use crate::lib::{
    responses::{ErrorBody, ErrorResponse},
    service_auth,
};
use rocket::{get, serde::json::Json};
use rocket_ws::{Stream, WebSocket};

#[get("/xrpc/social.colibri.sync.subscribeEvents?<auth>")]
/// Exposes a WebSocket connection that allows clients to receive events and send messages from and to the AppView.
pub async fn subscribe_events(
    auth: &str,
    ws: WebSocket,
) -> Result<Stream!['static], ErrorResponse> {
    let verification = service_auth::verify_service_auth(auth).await;

    if verification.is_err() {
        return Err(ErrorResponse {
            body: Json(ErrorBody {
                error: String::from("AuthError"),
                message: String::from(verification.unwrap_err().to_string()),
            }),
        });
    }

    // TODO: This is a stub implementation. The frontend needs to switch to a client-based OAuth solution
    // so this can start to work.
    Ok(ws.stream(|io| io))
}
