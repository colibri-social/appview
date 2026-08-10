use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard};

static CONNECTIONS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn connections() -> MutexGuard<'static, HashMap<String, usize>> {
    CONNECTIONS.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn add_connection(did: &str) -> usize {
    let mut connections = connections();
    let count = connections.entry(did.to_string()).or_insert(0);
    *count += 1;
    *count
}

pub fn remove_connection(did: &str) -> usize {
    let mut connections = connections();
    let Some(count) = connections.get_mut(did) else {
        return 0;
    };

    *count = count.saturating_sub(1);
    let remaining = *count;
    if remaining == 0 {
        connections.remove(did);
    }
    remaining
}

pub fn connection_count(did: &str) -> usize {
    connections().get(did).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_up_and_down_per_did() {
        let did = "did:plc:presence-counting";

        assert_eq!(add_connection(did), 1);
        assert_eq!(add_connection(did), 2);
        assert_eq!(connection_count(did), 2);

        assert_eq!(remove_connection(did), 1);
        assert_eq!(connection_count(did), 1);

        assert_eq!(remove_connection(did), 0);
        assert_eq!(connection_count(did), 0);
        assert!(!connections().contains_key(did));
    }

    #[test]
    fn tracks_dids_independently() {
        let one = "did:plc:presence-one";
        let two = "did:plc:presence-two";

        add_connection(one);
        add_connection(two);
        add_connection(two);

        assert_eq!(remove_connection(one), 0);
        assert_eq!(connection_count(two), 2);
    }

    #[test]
    fn removing_an_unknown_did_is_a_no_op() {
        let did = "did:plc:presence-unknown";

        assert_eq!(remove_connection(did), 0);
        assert_eq!(connection_count(did), 0);
    }
}
