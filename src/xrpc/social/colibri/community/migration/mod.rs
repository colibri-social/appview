//! REMOVABLE MIGRATION SCAFFOLDING.
//!
//! Everything under this module exists solely to migrate pre-rework ("legacy")
//! communities onto the current schema. Once every legacy community has been
//! migrated this whole directory can be deleted, along with:
//!   - the `community::migration` route mount in `main.rs`,
//!   - the `migratedTo`/`migratedFrom` handling in `list_communities_handler`
//!     and `list_messages_handler`,
//!   - the `migrated_to`/`migrated_from` fields on the colibri record structs.
//!
//! It deliberately does not add logic to the shared create/read handlers; it
//! only *calls* the generic `pds_client` / `community_credentials` helpers.

pub mod migrate_handler;

pub use migrate_handler::migrate;
