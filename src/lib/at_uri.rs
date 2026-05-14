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
}
