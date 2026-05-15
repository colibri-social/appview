//! Community moderation endpoints — banning, unbanning, hiding messages,
//! and listing currently-banned users. All write paths persist
//! `social.colibri.moderation` records on the community repo; read paths
//! fold the event log into current state.

pub mod block_message_handler;
pub mod block_user_handler;
pub mod list_blocked_users_handler;

pub use block_message_handler::block_message;
pub use block_user_handler::{block_user, unblock_user};
pub use list_blocked_users_handler::list_blocked_users;
