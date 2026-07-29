use std::sync::Arc;

use futures::future::BoxFuture;

type LookupFn =
    Arc<dyn Fn(String, String) -> BoxFuture<'static, (bool, bool)> + Send + Sync + 'static>;

#[derive(Clone, Default)]
pub struct ModerationLookup {
    inner: Option<LookupFn>,
}

impl ModerationLookup {
    pub fn new(lookup: LookupFn) -> Self {
        Self {
            inner: Some(lookup),
        }
    }

    pub async fn get(&self, channel: &str, did: &str) -> (bool, bool) {
        match &self.inner {
            Some(lookup) => lookup(channel.to_string(), did.to_string()).await,
            None => (false, false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    const CHANNEL: &str = "at://did:plc:c/social.colibri.channel/vc";

    #[tokio::test]
    async fn default_reports_unmoderated() {
        let lookup = ModerationLookup::default();

        assert_eq!(lookup.get(CHANNEL, "did:plc:a").await, (false, false));
    }

    #[tokio::test]
    async fn delegates_to_the_backing_lookup() {
        let lookup = ModerationLookup::new(Arc::new(|channel: String, did: String| {
            Box::pin(async move { (channel.ends_with("/vc") && did == "did:plc:a", false) })
        }));

        assert_eq!(lookup.get(CHANNEL, "did:plc:a").await, (true, false));
        assert_eq!(lookup.get(CHANNEL, "did:plc:b").await, (false, false));
        assert_eq!(
            lookup
                .get("at://did:plc:c/social.colibri.channel/text", "did:plc:a")
                .await,
            (false, false)
        );
    }
}
