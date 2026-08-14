//! Community moderation endpoints — banning, unbanning, hiding messages,
//! and listing currently-banned users. All write paths persist
//! `social.colibri.moderation` records on the community repo; read paths
//! fold the event log into current state.

pub mod ban_user_handler;
pub mod block_message_handler;
pub mod list_banned_users_handler;
pub mod suppress_embeds_handler;

pub use ban_user_handler::{ban_user, kick, kick_user, unban_user};
pub use block_message_handler::block_message;
pub use list_banned_users_handler::list_banned_users;
pub use suppress_embeds_handler::{suppress_message_embeds, unsuppress_message_embeds};
