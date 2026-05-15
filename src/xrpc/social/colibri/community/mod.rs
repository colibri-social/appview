//! `social.colibri.community.*` XRPC endpoints, grouped by concern:
//!
//! - `moderation/` — block/unblock + the moderation event log readers.
//! - `invitations/` — off-protocol invitation CRUD.
//! - `reads/` — anonymous list endpoints (categories, channels, members).
//! - `create_handler` + `register_credentials_handler` — community lifecycle
//!   (top-level because they don't share the prelude shape).

pub mod create_handler;
pub mod invitations;
pub mod moderation;
pub mod reads;
pub mod register_credentials_handler;

pub use create_handler::create;
pub use invitations::{create_invitation, delete_invitation, get_invitation, list_invitations};
pub use moderation::{block_message, block_user, list_blocked_users, unblock_user};
pub use reads::{list_categories, list_channels, list_members};
pub use register_credentials_handler::register_credentials;
