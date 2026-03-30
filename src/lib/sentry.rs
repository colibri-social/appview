use sentry::{ClientInitGuard, ClientOptions};

/// Attempts to parse the `SENTRY_DSN` environment variable
/// and returns a sentry config along with the DSN.
///
/// Should the environment variable be empty or unused, an empty
/// string will be returned.
fn get_sentry_config() -> (String, ClientOptions) {
    let dsn = match std::env::var("SENTRY_DSN") {
        Ok(k) if !k.is_empty() => k,
        _ => String::new(),
    };

    (
        dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            send_default_pii: true,
            ..Default::default()
        },
    )
}

/// Initializes the sentry client and returns a result based on
/// whether the `SENTRY_DSN` variable is returned as a non-zero
/// length string from the `get_sentry_config` function.
pub fn init_sentry() -> Result<ClientInitGuard, ()> {
    let (dsn, conf) = get_sentry_config();

    if dsn.is_empty() {
        warn!("SENTRY_DSN env var is not set. Skipping Sentry initialization.");
        return Err(());
    }

    info!("Sentry initialized.");
    Ok(sentry::init((dsn, conf)))
}
