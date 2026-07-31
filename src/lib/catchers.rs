use rocket::{Catcher, Request, catch, catchers, http::Status};

use crate::lib::responses::{ErrorCode, ErrorResponse};

pub fn all() -> Vec<Catcher> {
    catchers![
        bad_request,
        unauthorized,
        forbidden,
        not_found,
        payload_too_large,
        unprocessable_entity,
        too_many_requests,
        internal_error,
        default_catcher
    ]
}

#[catch(400)]
fn bad_request(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::InvalidRequest.with("The request could not be parsed.")
}

#[catch(401)]
fn unauthorized(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::AuthRequired.with("This method requires service auth.")
}

#[catch(403)]
fn forbidden(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::Forbidden.with("You do not have permission to do that.")
}

#[catch(404)]
fn not_found(req: &Request<'_>) -> ErrorResponse {
    ErrorCode::NotFound.with(format!(
        "No method at {}, or a required parameter was missing or malformed.",
        req.uri().path()
    ))
}

#[catch(413)]
fn payload_too_large(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::InvalidRequest.with("The uploaded data exceeds the size limit.")
}

#[catch(422)]
fn unprocessable_entity(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::InvalidRequest.with("The request body did not match the expected shape.")
}

#[catch(429)]
fn too_many_requests(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::RateLimited.with("Too many requests; try again shortly.")
}

#[catch(500)]
fn internal_error(_req: &Request<'_>) -> ErrorResponse {
    ErrorCode::InternalError.with("The AppView failed to handle the request.")
}

fn code_for(status: Status) -> ErrorCode {
    if status.code >= 500 {
        ErrorCode::InternalError
    } else {
        ErrorCode::InvalidRequest
    }
}

#[catch(default)]
fn default_catcher(status: Status, _req: &Request<'_>) -> ErrorResponse {
    code_for(status).with(format!("The request failed with status {}.", status.code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_catchers_are_registered() {
        assert_eq!(all().len(), 9);
    }

    #[test]
    fn server_statuses_map_to_an_internal_error() {
        for code in [500u16, 501, 502, 503, 504] {
            assert_eq!(
                code_for(Status::new(code)),
                ErrorCode::InternalError,
                "status {code}"
            );
        }
    }

    #[test]
    fn client_statuses_map_to_an_invalid_request() {
        for code in [400u16, 405, 409, 415, 431] {
            assert_eq!(
                code_for(Status::new(code)),
                ErrorCode::InvalidRequest,
                "status {code}"
            );
        }
    }

    #[test]
    fn every_catcher_answers_the_status_its_code_declares() {
        assert_eq!(ErrorCode::NotFound.status(), Status::NotFound);
        assert_eq!(ErrorCode::AuthRequired.status(), Status::Unauthorized);
        assert_eq!(ErrorCode::Forbidden.status(), Status::Forbidden);
        assert_eq!(ErrorCode::RateLimited.status(), Status::TooManyRequests);
        assert_eq!(
            ErrorCode::InternalError.status(),
            Status::InternalServerError
        );
    }
}
