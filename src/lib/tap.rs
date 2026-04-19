use base64::Engine;
use rocket::tokio::net::TcpStream;
use std::io::Error;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::client::IntoClientRequest,
};

/// Opens a new connection to the tap instance.
pub async fn connect_to_tap() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let credentials =
        base64::engine::general_purpose::STANDARD.encode(format!("admin:{tap_password}"));

    let mut request = format!("ws://{tap_hostname}/channel")
        .into_client_request()
        .expect("Failed to build request");

    request.headers_mut().insert(
        "Authorization",
        format!("Basic {credentials}").parse().unwrap(),
    );

    let (ws_stream, _response) = connect_async(request).await.expect("Failed to connect");
    Ok(ws_stream)
}
