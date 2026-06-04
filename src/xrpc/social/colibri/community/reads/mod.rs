//! Read-only community endpoints — list categories, channels, members, roles,
//! and an aggregate getData endpoint. No service auth required.

pub mod get_data_handler;
pub mod list_categories_handler;
pub mod list_channels_handler;
pub mod list_members_handler;
pub mod list_roles_handler;

pub use get_data_handler::get_data;
pub use list_categories_handler::list_categories;
pub use list_channels_handler::list_channels;
pub use list_members_handler::list_members;
pub use list_roles_handler::list_roles;
