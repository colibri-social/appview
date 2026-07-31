use rocket::Request;
use rocket::http::Status;
use rocket::response::{self, Responder, Response};
use rocket::serde::{Serialize, json::Json};
use sea_orm::DbErr;
use trust_dns_resolver::error::ResolveError;

use crate::lib::embed_fetch::FetchError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    AuthRequired,
    Forbidden,
    InvalidRequest,
    NotFound,
    NotEnabled,
    InvalidState,
    RateLimited,
    TooManySubscribers,
    NotAnImage,
    SfuError,
    PdsUnavailable,
    CommunityCredentialsUnrecoverable,
    UpstreamFailure,
    InternalError,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthRequired => "AuthRequired",
            Self::Forbidden => "Forbidden",
            Self::InvalidRequest => "InvalidRequest",
            Self::NotFound => "NotFound",
            Self::NotEnabled => "NotEnabled",
            Self::InvalidState => "InvalidState",
            Self::RateLimited => "RateLimited",
            Self::TooManySubscribers => "TooManySubscribers",
            Self::NotAnImage => "NotAnImage",
            Self::SfuError => "SfuError",
            Self::PdsUnavailable => "PdsUnavailable",
            Self::CommunityCredentialsUnrecoverable => "CommunityCredentialsUnrecoverable",
            Self::UpstreamFailure => "UpstreamFailure",
            Self::InternalError => "InternalError",
        }
    }

    pub const fn status(self) -> Status {
        match self {
            Self::AuthRequired => Status::Unauthorized,
            Self::Forbidden | Self::NotEnabled => Status::Forbidden,
            Self::InvalidRequest | Self::InvalidState => Status::BadRequest,
            Self::NotFound => Status::NotFound,
            Self::RateLimited => Status::TooManyRequests,
            Self::NotAnImage => Status::UnsupportedMediaType,
            Self::TooManySubscribers => Status::ServiceUnavailable,
            Self::SfuError | Self::PdsUnavailable | Self::UpstreamFailure => Status::BadGateway,
            Self::CommunityCredentialsUnrecoverable | Self::InternalError => {
                Status::InternalServerError
            }
        }
    }

    pub fn with(self, message: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            code: self,
            body: Json(ErrorBody {
                error: String::from(self.as_str()),
                message: message.into(),
            }),
        }
    }
}

/// Marks a [`DbErr::Custom`] as carrying an [`ErrorCode::PdsUnavailable`] failure.
pub const PDS_UNAVAILABLE_MARKER: &str = "\u{1}pds-unavailable\u{1}";

/// Marks a [`DbErr::Custom`] as carrying an
/// [`ErrorCode::CommunityCredentialsUnrecoverable`] failure.
pub const CREDENTIALS_UNRECOVERABLE_MARKER: &str = "\u{1}credentials-unrecoverable\u{1}";

#[derive(Serialize, Debug)]
pub struct ErrorBody {
    pub message: String,
    pub error: String,
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub body: Json<ErrorBody>,
}

impl<'r> Responder<'r, 'static> for ErrorResponse {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        let status = self.code.status();
        Response::build_from(self.body.respond_to(req)?)
            .status(status)
            .ok()
    }
}

/// Builds an [`ErrorCode::PdsUnavailable`] error response.
pub fn pds_unavailable(message: impl Into<String>) -> ErrorResponse {
    ErrorCode::PdsUnavailable.with(message)
}

impl From<reqwest::Error> for ErrorResponse {
    fn from(err: reqwest::Error) -> Self {
        log::error!("{err:?}");
        let code = if err.is_connect() || err.is_timeout() {
            ErrorCode::PdsUnavailable
        } else {
            ErrorCode::UpstreamFailure
        };
        code.with(err.to_string())
    }
}

impl From<ResolveError> for ErrorResponse {
    fn from(err: ResolveError) -> Self {
        ErrorCode::UpstreamFailure.with(err.to_string())
    }
}

impl From<DbErr> for ErrorResponse {
    fn from(err: DbErr) -> Self {
        let message = err.to_string();
        if let Some(index) = message.find(PDS_UNAVAILABLE_MARKER) {
            let detail = &message[index + PDS_UNAVAILABLE_MARKER.len()..];
            return pds_unavailable(detail);
        }
        if let Some(index) = message.find(CREDENTIALS_UNRECOVERABLE_MARKER) {
            let detail = &message[index + CREDENTIALS_UNRECOVERABLE_MARKER.len()..];
            return ErrorCode::CommunityCredentialsUnrecoverable.with(detail);
        }

        ErrorCode::InternalError.with(message)
    }
}

impl From<FetchError> for ErrorResponse {
    fn from(err: FetchError) -> Self {
        ErrorCode::from(&err).with(err.to_string())
    }
}

impl From<&FetchError> for ErrorCode {
    fn from(err: &FetchError) -> Self {
        match err {
            FetchError::InvalidUrl(_) | FetchError::Blocked(_) => Self::InvalidRequest,
            _ => Self::UpstreamFailure,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_CODES: [ErrorCode; 14] = [
        ErrorCode::AuthRequired,
        ErrorCode::Forbidden,
        ErrorCode::InvalidRequest,
        ErrorCode::NotFound,
        ErrorCode::NotEnabled,
        ErrorCode::InvalidState,
        ErrorCode::RateLimited,
        ErrorCode::TooManySubscribers,
        ErrorCode::NotAnImage,
        ErrorCode::SfuError,
        ErrorCode::PdsUnavailable,
        ErrorCode::CommunityCredentialsUnrecoverable,
        ErrorCode::UpstreamFailure,
        ErrorCode::InternalError,
    ];

    #[test]
    fn db_err_carrying_the_marker_becomes_pds_unavailable() {
        let err = DbErr::Custom(format!(
            "{PDS_UNAVAILABLE_MARKER}pds write failed: http error"
        ));
        let res = ErrorResponse::from(err);
        assert_eq!(res.body.error, ErrorCode::PdsUnavailable.as_str());
        assert_eq!(res.body.message, "pds write failed: http error");
        assert_eq!(res.code.status(), Status::BadGateway);
    }

    #[test]
    fn db_err_carrying_the_credentials_marker_becomes_unrecoverable() {
        let err = DbErr::Custom(format!(
            "{CREDENTIALS_UNRECOVERABLE_MARKER}no usable password"
        ));
        let res = ErrorResponse::from(err);
        assert_eq!(
            res.body.error,
            ErrorCode::CommunityCredentialsUnrecoverable.as_str()
        );
        assert_eq!(res.body.message, "no usable password");
        assert_eq!(res.code.status(), Status::InternalServerError);
    }

    #[test]
    fn plain_db_err_is_an_internal_error() {
        let res = ErrorResponse::from(DbErr::Custom(String::from("connection pool exhausted")));
        assert_eq!(res.body.error, "InternalError");
        assert!(res.body.message.contains("connection pool exhausted"));
        assert_eq!(res.code.status(), Status::InternalServerError);
    }

    #[test]
    fn the_serialized_error_field_always_matches_the_code() {
        for code in ALL_CODES {
            let res = code.with("boom");
            assert_eq!(res.body.error, code.as_str());
            assert_eq!(res.code, code);
        }
    }

    #[test]
    fn client_mistakes_are_never_retryable_statuses() {
        for code in [
            ErrorCode::AuthRequired,
            ErrorCode::Forbidden,
            ErrorCode::InvalidRequest,
            ErrorCode::InvalidState,
            ErrorCode::NotFound,
            ErrorCode::NotEnabled,
            ErrorCode::NotAnImage,
        ] {
            let status = code.status();
            assert!(
                status.code >= 400 && status.code < 500,
                "{} answered {}, expected a 4xx",
                code.as_str(),
                status.code
            );
            assert_ne!(
                status,
                Status::TooManyRequests,
                "{} must not answer 429",
                code.as_str()
            );
        }
    }

    #[test]
    fn every_code_has_a_distinct_name() {
        let mut names: Vec<&str> = ALL_CODES.iter().map(|code| code.as_str()).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count, "duplicate error code name");
    }
}
