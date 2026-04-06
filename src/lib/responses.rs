use rocket::Responder;
use rocket::serde::{Serialize, json::Json};
use sea_orm::DbErr;
use trust_dns_resolver::error::ResolveError;

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

impl From<reqwest::Error> for ErrorResponse {
    fn from(err: reqwest::Error) -> Self {
        println!("{err:?}");
        ErrorResponse {
            body: Json(ErrorBody {
                error: "UpstreamError".into(),
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
        ErrorResponse {
            body: Json(ErrorBody {
                error: "UpstreamError".into(),
                message: err.to_string(),
            }),
        }
    }
}
