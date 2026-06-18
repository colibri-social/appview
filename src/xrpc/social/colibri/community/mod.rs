//! `social.colibri.community.*` XRPC endpoints, grouped by concern:
//!
//! - `moderation/` — block/unblock + the moderation event log readers.
//! - `invitations/` — off-protocol invitation CRUD.
//! - `reads/` — anonymous list endpoints (categories, channels, members).
//! - `create_handler` + `register_credentials_handler` — community lifecycle
//!   (top-level because they don't share the prelude shape).

pub mod approve_membership_handler;
pub mod create_handler;
pub mod delete_handler;
pub mod invitations;
pub mod list_applications_handler;
pub mod moderation;
pub mod reads;
pub mod register_credentials_handler;
pub mod update_handler;
pub mod writes;

pub use approve_membership_handler::approve_membership;
pub use create_handler::create;
pub use delete_handler::delete_community;
pub use invitations::{create_invitation, delete_invitation, get_invitation, list_invitations};
pub use list_applications_handler::list_applications;
pub use moderation::{
    block_message, block_user, kick, kick_user, list_blocked_users, unblock_user,
};
pub use reads::{get_data, list_categories, list_channels, list_members, list_roles};
pub use register_credentials_handler::register_credentials;
pub use update_handler::update_community;
pub use writes::{
    create_category, create_role, delete_category, delete_role, reorder_categories,
    reorder_channels, set_member_roles, update_category, update_role,
};
