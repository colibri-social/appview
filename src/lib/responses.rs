use rocket::Responder;
use rocket::serde::{Serialize, json::Json};

#[derive(Serialize)]
pub struct ErrorBody {
    pub message: String,
    pub error: String,
}

#[derive(Responder)]
#[response(status = 500, content_type = "json")]
pub struct ErrorResponse {
    pub body: Json<ErrorBody>,
}

impl From<reqwest::Error> for ErrorResponse {
    fn from(err: reqwest::Error) -> Self {
        ErrorResponse {
            body: Json(ErrorBody {
                error: "UpstreamError".into(),
                message: err.to_string(),
            }),
        }
    }
}

#[derive(Responder)]
pub enum ResponseEnum<T> {
    #[response(status = 200, content_type = "json")]
    Success(Json<T>),

    #[response(status = 500, content_type = "json")]
    Error(ErrorResponse),
}
