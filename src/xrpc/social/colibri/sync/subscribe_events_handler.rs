use rocket::get;
use rocket_ws::{Stream, WebSocket};

#[get("/xrpc/social.colibri.sync.subscribeEvents")]
/// Exposes a WebSocket connection that allows clients to receive events and send messages from and to the AppView.
pub async fn subscribe_events(ws: WebSocket) -> Stream!['static] {
    // TODO: This is a stub implementation. The frontend needs to switch to a client-based OAuth solution
    // so this can start to work.
    ws.stream(|io| io)
}
