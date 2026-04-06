pub mod identity {
    pub mod resolve_did_handler;
    pub mod resolve_handle_handler;
    pub mod resolve_identity_handler;

    pub use resolve_did_handler::resolve_did;
    pub use resolve_handle_handler::resolve_handle;
    pub use resolve_identity_handler::resolve_identity;
}
