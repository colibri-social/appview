pub mod identity {
    pub mod resolve_did_handler;
    pub mod resolve_handle_handler;
    pub mod resolve_identity_handler;

    pub use resolve_did_handler::resolve_did;
    pub use resolve_handle_handler::resolve_handle;
    pub use resolve_identity_handler::resolve_identity;
}

pub mod sync {
    pub mod get_record_handler;
    pub mod list_records_handler;

    pub use get_record_handler::get_record;
    pub use list_records_handler::list_records;
}
