use log::{info, warn};
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

    let release = option_env!("APPVIEW_VERSION")
        .filter(|v| !v.is_empty())
        .map(|v| format!("colibri-appview@{v}").into())
        .or_else(|| sentry::release_name!());

    let environment = std::env::var("SENTRY_ENVIRONMENT")
        .ok()
        .filter(|v| !v.is_empty())
        .map(Into::into);

    (
        dsn,
        sentry::ClientOptions {
            release,
            environment,
            send_default_pii: false,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentry_dsn_is_empty_without_env() {
        let result = get_sentry_config();

        assert!(result.0.is_empty())
    }

    #[test]
    fn init_sentry_errors_with_empty_dsn() {
        let result = init_sentry();

        assert!(result.is_err())
    }
}
