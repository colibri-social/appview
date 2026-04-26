use crate::xrpc::social::colibri::actor::set_state_handler::UserState;

pub fn validate_state_str(maybe_state: &str) -> Option<&'static str> {
    match maybe_state {
        "online" => Some(UserState::Online.as_str()),
        "away" => Some(UserState::Away.as_str()),
        "dnd" => Some(UserState::Dnd.as_str()),
        "offline" => Some(UserState::Offline.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::validate_state_str;

    #[test]
    fn accepts_valid_states() {
        assert_eq!(validate_state_str("online"), Some("online"));
        assert_eq!(validate_state_str("away"), Some("away"));
        assert_eq!(validate_state_str("dnd"), Some("dnd"));
        assert_eq!(validate_state_str("offline"), Some("offline"));
    }

    #[test]
    fn rejects_invalid_state() {
        assert_eq!(validate_state_str("busy"), None);
    }
}
