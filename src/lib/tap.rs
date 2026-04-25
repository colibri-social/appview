use base64::Engine;
use rocket::tokio::net::TcpStream;
use std::io::Error;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::client::IntoClientRequest,
};

use crate::xrpc::social::colibri::sync::subscribe_events_handler::DidStruct;

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

/// Sends a DID to Tap to register for backfilling.
pub async fn register_did(did: &String) {
    let tap_hostname = std::env::var("TAP_HOSTNAME").expect("TAP_HOSTNAME not found in .env");
    let tap_password =
        std::env::var("TAP_ADMIN_PASSWORD").expect("TAP_ADMIN_PASSWORD not found in .env");
    let client = reqwest::Client::new();

    let did_struct = DidStruct {
        dids: vec![did.clone()],
    };

    let res = client
        .post(format!("http://{tap_hostname}/repos/add"))
        .basic_auth(String::from("admin"), Some(tap_password))
        .json(&did_struct)
        .send()
        .await;

    if res.is_err() {
        log::error!("Unable to add DID {did}: {}", res.unwrap_err().to_string())
    } else {
        log::info!("Now tracking DID {did}");
    }
}
