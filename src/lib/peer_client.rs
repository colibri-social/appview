use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use futures::future::BoxFuture;

use crate::lib::embed_fetch::pinned_client;
use crate::lib::service_auth::{self, ServiceAuthError};

const PEER_TIMEOUT: Duration = Duration::from_secs(20);

const MAX_RESPONSE_BYTES: usize = 256 * 1024;

fn allow_loopback() -> bool {
    std::env::var("PEER_ALLOW_LOOPBACK")
        .map(|v| matches!(v.trim(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("peer {0} is not addressable: only did:web AppViews can be called")]
    NotAddressable(String),
    #[error("peer address is not allowed: {0}")]
    Blocked(String),
    #[error("could not resolve peer host {0}")]
    Unresolvable(String),
    #[error("could not sign the relay token: {0}")]
    Signing(#[from] ServiceAuthError),
    #[error("relay to peer failed: {0}")]
    Transport(String),
    #[error("peer response exceeded {MAX_RESPONSE_BYTES} bytes")]
    ResponseTooLarge,
}

#[derive(Debug, Clone)]
pub struct PeerBody {
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PeerReply {
    pub status: u16,
    pub body: Vec<u8>,
}

pub fn did_web_host(did: &str) -> Option<String> {
    did.strip_prefix("did:web:").map(|h| h.replace("%3A", ":"))
}

pub type ForwardXrpcFn =
    dyn Fn(RelayCall) -> BoxFuture<'static, Result<PeerReply, PeerError>> + Send + Sync;

#[derive(Debug, Clone)]
pub struct RelayCall {
    pub hub_did: String,
    pub lxm: String,
    pub method: String,
    pub path_and_query: String,
    pub act: String,
    pub body: Option<PeerBody>,
}

pub fn forward_xrpc_boxed(call: RelayCall) -> BoxFuture<'static, Result<PeerReply, PeerError>> {
    Box::pin(async move { forward(call).await })
}

pub async fn forward(call: RelayCall) -> Result<PeerReply, PeerError> {
    let host = did_web_host(&call.hub_did)
        .ok_or_else(|| PeerError::NotAddressable(call.hub_did.clone()))?;

    let url = format!("https://{host}{}", call.path_and_query);
    let addrs = resolve_peer(&host).await?;

    let token = service_auth::mint_appview_auth_for(&call.hub_did, &call.lxm, Some(&call.act))?;

    let client = pinned_client(host_only(&host), &addrs, PEER_TIMEOUT)
        .map_err(|e| PeerError::Transport(e.to_string()))?;

    let mut request = match call.method.as_str() {
        "GET" => client.get(&url),
        _ => client.post(&url),
    }
    .bearer_auth(&token);

    if let Some(body) = call.body {
        request = request
            .header(reqwest::header::CONTENT_TYPE, body.content_type)
            .body(body.bytes);
    }

    let response = request
        .send()
        .await
        .map_err(|e| PeerError::Transport(e.to_string()))?;

    let status = response.status().as_u16();
    let bytes = response
        .bytes()
        .await
        .map_err(|e| PeerError::Transport(e.to_string()))?;

    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(PeerError::ResponseTooLarge);
    }

    Ok(PeerReply {
        status,
        body: bytes.to_vec(),
    })
}

fn split_host_port(host: &str) -> (&str, u16) {
    if let Some(rest) = host.strip_prefix('[') {
        let (name, tail) = match rest.split_once(']') {
            Some(parts) => parts,
            None => return (host, 443),
        };
        let port = tail
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(443);
        return (name, port);
    }

    if host.matches(':').count() == 1
        && let Some((name, port)) = host.split_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        return (name, port);
    }

    (host, 443)
}

fn host_only(host: &str) -> &str {
    split_host_port(host).0
}

async fn resolve_peer(host: &str) -> Result<Vec<SocketAddr>, PeerError> {
    let (name, port) = split_host_port(host);

    if let Ok(ip) = name.parse::<IpAddr>() {
        reject_blocked(&ip, host)?;
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addrs: Vec<SocketAddr> = rocket::tokio::net::lookup_host((name, port))
        .await
        .map_err(|e| PeerError::Transport(format!("dns lookup failed: {e}")))?
        .collect();

    if addrs.is_empty() {
        return Err(PeerError::Unresolvable(host.to_string()));
    }

    for addr in &addrs {
        reject_blocked(&addr.ip(), host)?;
    }

    Ok(addrs)
}

fn reject_blocked(ip: &IpAddr, host: &str) -> Result<(), PeerError> {
    if is_blocked_peer_ip(ip) && !allow_loopback() {
        return Err(PeerError::Blocked(format!("{host} -> {ip}")));
    }
    Ok(())
}

fn is_blocked_peer_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_unspecified(),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_peer_ip(&IpAddr::V4(mapped));
            }
            v6.is_loopback() || v6.is_unspecified()
        }
    }
}

#[cfg(test)]
mod tests {
    use rocket::tokio;

    use super::*;

    #[test]
    fn did_web_hosts_decode_their_port() {
        assert_eq!(
            did_web_host("did:web:api.colibri.social").as_deref(),
            Some("api.colibri.social")
        );
        assert_eq!(
            did_web_host("did:web:localhost%3A8001").as_deref(),
            Some("localhost:8001")
        );
    }

    #[test]
    fn other_did_methods_are_not_addressable() {
        assert!(did_web_host("did:plc:abc").is_none());
    }

    #[test]
    fn a_port_is_split_off_the_host() {
        assert_eq!(split_host_port("example.com:8443"), ("example.com", 8443));
        assert_eq!(split_host_port("example.com"), ("example.com", 443));
    }

    #[test]
    fn a_bare_ipv6_literal_is_all_host() {
        assert_eq!(split_host_port("::1"), ("::1", 443));
        assert_eq!(split_host_port("2001:db8::7"), ("2001:db8::7", 443));
    }

    #[test]
    fn a_bracketed_ipv6_literal_keeps_its_port() {
        assert_eq!(split_host_port("[::1]:8443"), ("::1", 8443));
        assert_eq!(split_host_port("[::1]"), ("::1", 443));
    }

    #[test]
    fn loopback_is_refused() {
        assert!(is_blocked_peer_ip(&"127.0.0.1".parse().unwrap()));
        assert!(is_blocked_peer_ip(&"::1".parse().unwrap()));
        assert!(is_blocked_peer_ip(&"0.0.0.0".parse().unwrap()));
        assert!(is_blocked_peer_ip(&"::ffff:127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn private_and_link_local_peers_are_allowed() {
        assert!(!is_blocked_peer_ip(&"10.0.0.5".parse().unwrap()));
        assert!(!is_blocked_peer_ip(&"192.168.1.10".parse().unwrap()));
        assert!(!is_blocked_peer_ip(&"169.254.169.254".parse().unwrap()));
        assert!(!is_blocked_peer_ip(&"203.0.113.7".parse().unwrap()));
    }

    #[tokio::test]
    async fn a_non_did_web_peer_is_refused_before_any_network_work() {
        let err = forward(RelayCall {
            hub_did: String::from("did:plc:abc"),
            lxm: String::from("social.colibri.community.banUser"),
            method: String::from("POST"),
            path_and_query: String::from("/xrpc/social.colibri.community.banUser"),
            act: String::from("did:plc:actor"),
            body: None,
        })
        .await
        .unwrap_err();

        assert!(matches!(err, PeerError::NotAddressable(_)));
    }

    #[tokio::test]
    async fn a_loopback_peer_is_refused_by_default() {
        if allow_loopback() {
            return;
        }
        let err = resolve_peer("127.0.0.1:8001").await.unwrap_err();
        assert!(matches!(err, PeerError::Blocked(_)));
    }
}
