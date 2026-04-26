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
