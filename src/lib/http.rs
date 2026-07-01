use std::sync::LazyLock;
use std::time::Duration;

pub static HTTP: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(8)
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build shared HTTP client")
});
