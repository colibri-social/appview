use futures::future::BoxFuture;
use rocket::get;
use rocket::serde::json::Json;
use serde::Serialize;

use crate::lib::klipy::{self, GifCategory};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;

const LXM: &str = "social.colibri.embed.gifCategories";

type AuthFut = BoxFuture<'static, Result<String, service_auth::ServiceAuthError>>;
type CategoriesFut = BoxFuture<'static, Result<Vec<GifCategory>, ErrorResponse>>;

#[derive(Serialize)]
pub struct GifCategoriesResponse {
    pub categories: Vec<GifCategory>,
}

fn auth_error(err: service_auth::ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

async fn gif_categories_with<VA, FE>(
    auth: String,
    verify_auth_fn: VA,
    fetch_fn: FE,
) -> Result<Json<GifCategoriesResponse>, ErrorResponse>
where
    VA: Fn(String, String) -> AuthFut,
    FE: Fn() -> CategoriesFut,
{
    verify_auth_fn(auth, String::from(LXM))
        .await
        .map_err(auth_error)?;
    let categories = fetch_fn().await?;
    Ok(Json(GifCategoriesResponse { categories }))
}

fn verify_auth_boxed(auth: String, lxm: String) -> AuthFut {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn fetch_boxed() -> CategoriesFut {
    Box::pin(async move { klipy::categories().await })
}

#[get("/xrpc/social.colibri.embed.gifCategories?<auth>")]
pub async fn gif_categories(auth: &str) -> Result<Json<GifCategoriesResponse>, ErrorResponse> {
    gif_categories_with(auth.to_string(), verify_auth_boxed, fetch_boxed).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_auth_error_on_bad_token() {
        let result = gif_categories_with(
            String::from("bad"),
            |_, _| Box::pin(async { Err(service_auth::ServiceAuthError::InvalidSignature) }),
            || Box::pin(async { panic!("must not fetch when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
