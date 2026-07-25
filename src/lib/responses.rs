use rocket::Responder;
use rocket::serde::{Serialize, json::Json};
use sea_orm::DbErr;
use trust_dns_resolver::error::ResolveError;

use crate::lib::embed_fetch::FetchError;

/// Error code returned when the PDS an operation needs is unreachable or isn't
/// a PDS at all
pub const PDS_UNAVAILABLE: &str = "PdsUnavailable";

/// Marks a [`DbErr::Custom`] as carrying a [`PDS_UNAVAILABLE`] failure.
pub const PDS_UNAVAILABLE_MARKER: &str = "\u{1}pds-unavailable\u{1}";

#[derive(Serialize, Debug)]
pub struct ErrorBody {
    pub message: String,
    pub error: String,
}

#[derive(Responder, Debug)]
#[response(status = 500, content_type = "json")]
pub struct ErrorResponse {
    pub body: Json<ErrorBody>,
}

/// Builds a [`PDS_UNAVAILABLE`] error response.
pub fn pds_unavailable(message: impl Into<String>) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from(PDS_UNAVAILABLE),
            message: message.into(),
        }),
    }
}

impl From<reqwest::Error> for ErrorResponse {
    fn from(err: reqwest::Error) -> Self {
        log::error!("{err:?}");
        let error = if err.is_connect() || err.is_timeout() {
            PDS_UNAVAILABLE
        } else {
            "UpstreamError"
        };
        ErrorResponse {
            body: Json(ErrorBody {
                error: error.into(),
                message: err.to_string(),
            }),
        }
    }
}

impl From<ResolveError> for ErrorResponse {
    fn from(err: ResolveError) -> Self {
        ErrorResponse {
            body: Json(ErrorBody {
                error: "UpstreamError".into(),
                message: err.to_string(),
            }),
        }
    }
}

impl From<DbErr> for ErrorResponse {
    fn from(err: DbErr) -> Self {
        let message = err.to_string();
        if let Some(index) = message.find(PDS_UNAVAILABLE_MARKER) {
            let detail = &message[index + PDS_UNAVAILABLE_MARKER.len()..];
            return pds_unavailable(detail);
        }

        ErrorResponse {
            body: Json(ErrorBody {
                error: "UpstreamError".into(),
                message,
            }),
        }
    }
}

impl From<FetchError> for ErrorResponse {
    fn from(err: FetchError) -> Self {
        ErrorResponse {
            body: Json(ErrorBody {
                error: "UpstreamError".into(),
                message: err.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_err_carrying_the_marker_becomes_pds_unavailable() {
        let err = DbErr::Custom(format!(
            "{PDS_UNAVAILABLE_MARKER}pds write failed: http error"
        ));
        let res = ErrorResponse::from(err);
        assert_eq!(res.body.error, PDS_UNAVAILABLE);
        assert_eq!(res.body.message, "pds write failed: http error");
    }

    #[test]
    fn plain_db_err_stays_an_upstream_error() {
        let res = ErrorResponse::from(DbErr::Custom(String::from("connection pool exhausted")));
        assert_eq!(res.body.error, "UpstreamError");
        assert!(res.body.message.contains("connection pool exhausted"));
    }
}
