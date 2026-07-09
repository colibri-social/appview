#[derive(Clone, Debug)]
pub enum VoiceAction {
    ServerMute(bool),
    ServerDeafen(bool),
    Disconnect,
}

#[derive(Clone, Debug)]
pub struct VoiceControlCommand {
    pub channel_uri: String,
    pub target_did: String,
    pub action: VoiceAction,
}
