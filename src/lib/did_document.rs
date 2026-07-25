use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct VerificationMethod {
    pub id: String,
    #[serde(rename = "type")]
    pub verification_type: String,
    pub controller: String,
    #[serde(rename = "publicKeyMultibase", skip_serializing_if = "Option::is_none")]
    pub public_key_multibase: Option<String>,
    #[serde(
        rename = "publicKeyJwk",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub public_key_jwk: Option<serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Service {
    pub id: String,
    #[serde(rename = "type")]
    pub service_type: String,
    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DidDocument {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: String,
    #[serde(rename = "alsoKnownAs", skip_serializing_if = "Option::is_none")]
    pub also_known_as: Option<Vec<String>>,
    #[serde(rename = "verificationMethod")]
    pub verification_method: Vec<VerificationMethod>,
    pub service: Vec<Service>,
}

impl DidDocument {
    /// The `serviceEndpoint` of the document's PDS service entry, or `None` when
    /// it declares none. Accepts either spelling seen in the wild: the
    /// `#atproto_pds` id, or the `AtprotoPersonalDataServer` type.
    pub fn pds_endpoint(&self) -> Option<&str> {
        self.service
            .iter()
            .find(|s| s.id == "#atproto_pds" || s.service_type == "AtprotoPersonalDataServer")
            .map(|s| s.service_endpoint.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_with_service(id: &str, service_type: &str, endpoint: &str) -> DidDocument {
        DidDocument {
            context: vec![],
            id: String::from("did:plc:test"),
            also_known_as: None,
            verification_method: vec![],
            service: vec![Service {
                id: String::from(id),
                service_type: String::from(service_type),
                service_endpoint: endpoint.to_string(),
            }],
        }
    }

    #[test]
    fn pds_endpoint_finds_atproto_pds_service() {
        let doc = doc_with_service(
            "#atproto_pds",
            "AtprotoPersonalDataServer",
            "https://pds.example.com",
        );
        assert_eq!(doc.pds_endpoint(), Some("https://pds.example.com"));
    }

    #[test]
    fn pds_endpoint_returns_none_when_absent() {
        let doc = DidDocument {
            context: vec![],
            id: String::from("did:plc:test"),
            also_known_as: None,
            verification_method: vec![],
            service: vec![],
        };
        assert!(doc.pds_endpoint().is_none());
    }

    #[test]
    fn pds_endpoint_matches_by_service_type_too() {
        let doc = doc_with_service(
            "#other",
            "AtprotoPersonalDataServer",
            "https://pds.example.com",
        );
        assert_eq!(doc.pds_endpoint(), Some("https://pds.example.com"));
    }

    #[test]
    fn pds_endpoint_matches_by_id_too() {
        let doc = doc_with_service("#atproto_pds", "SomethingElse", "https://pds.example.com");
        assert_eq!(doc.pds_endpoint(), Some("https://pds.example.com"));
    }
}
