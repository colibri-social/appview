/// A parsed AT URI of the form `at://<authority>/<collection>/<rkey>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtUri {
    pub authority: String,
    pub collection: String,
    pub rkey: String,
}

impl AtUri {
    /// Parses a fully qualified AT URI. Returns `None` if the URI does not have
    /// the `at://<authority>/<collection>/<rkey>` shape.
    pub fn parse(uri: &str) -> Option<Self> {
        let rest = uri.strip_prefix("at://")?;
        let mut parts = rest.splitn(3, '/');
        let authority = parts.next()?;
        let collection = parts.next()?;
        let rkey = parts.next()?;

        if authority.is_empty() || collection.is_empty() || rkey.is_empty() {
            return None;
        }

        Some(AtUri {
            authority: authority.to_string(),
            collection: collection.to_string(),
            rkey: rkey.to_string(),
        })
    }

    /// Extracts the rkey from a full AT URI, or returns the input unchanged if
    /// it doesn't parse as one
    pub fn rkey_or_value(s: &str) -> String {
        Self::parse(s)
            .map(|u| u.rkey)
            .unwrap_or_else(|| s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_uri() {
        let uri = AtUri::parse("at://did:plc:abc/social.colibri.community/r1").unwrap();
        assert_eq!(uri.authority, "did:plc:abc");
        assert_eq!(uri.collection, "social.colibri.community");
        assert_eq!(uri.rkey, "r1");
    }

    #[test]
    fn rejects_missing_scheme() {
        assert!(AtUri::parse("did:plc:abc/social.colibri.community/r1").is_none());
    }

    #[test]
    fn rejects_missing_rkey() {
        assert!(AtUri::parse("at://did:plc:abc/social.colibri.community").is_none());
    }

    #[test]
    fn rejects_empty_segments() {
        assert!(AtUri::parse("at:///social.colibri.community/r1").is_none());
        assert!(AtUri::parse("at://did:plc:abc//r1").is_none());
        assert!(AtUri::parse("at://did:plc:abc/social.colibri.community/").is_none());
    }

    #[test]
    fn keeps_extra_path_segments_with_rkey() {
        let uri = AtUri::parse("at://did:plc:abc/social.colibri.community/r1/extra").unwrap();
        assert_eq!(uri.rkey, "r1/extra");
    }

    #[test]
    fn rkey_or_value_extracts_rkey_from_full_uri() {
        assert_eq!(
            AtUri::rkey_or_value("at://did:plc:abc/social.colibri.channel/chan-a"),
            "chan-a"
        );
    }

    #[test]
    fn rkey_or_value_passes_through_non_uri_unchanged() {
        assert_eq!(AtUri::rkey_or_value("chan-a"), "chan-a");
    }
}
