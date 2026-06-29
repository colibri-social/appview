use futures::future::BoxFuture;
use rocket::get;
use rocket::serde::json::Json;

use crate::lib::klipy::{self, GifPage};
use crate::lib::responses::{ErrorBody, ErrorResponse};
use crate::lib::service_auth;

const LXM: &str = "social.colibri.embed.trendingGifs";

type AuthFut = BoxFuture<'static, Result<String, service_auth::ServiceAuthError>>;
type PageFut = BoxFuture<'static, Result<GifPage, ErrorResponse>>;

fn auth_error(err: service_auth::ServiceAuthError) -> ErrorResponse {
    ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("AuthError"),
            message: err.to_string(),
        }),
    }
}

async fn trending_gifs_with<VA, FE>(
    page: u32,
    auth: String,
    verify_auth_fn: VA,
    fetch_fn: FE,
) -> Result<Json<GifPage>, ErrorResponse>
where
    VA: Fn(String, String) -> AuthFut,
    FE: Fn(u32) -> PageFut,
{
    verify_auth_fn(auth, String::from(LXM))
        .await
        .map_err(auth_error)?;
    Ok(Json(fetch_fn(page).await?))
}

fn verify_auth_boxed(auth: String, lxm: String) -> AuthFut {
    Box::pin(async move { service_auth::verify_service_auth(&auth, &lxm).await })
}

fn fetch_boxed(page: u32) -> PageFut {
    Box::pin(async move { klipy::trending(page).await })
}

#[get("/xrpc/social.colibri.embed.trendingGifs?<page>&<auth>")]
pub async fn trending_gifs(page: Option<u32>, auth: &str) -> Result<Json<GifPage>, ErrorResponse> {
    trending_gifs_with(
        page.unwrap_or(1),
        auth.to_string(),
        verify_auth_boxed,
        fetch_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_auth_error_on_bad_token() {
        let result = trending_gifs_with(
            1,
            String::from("bad"),
            |_, _| Box::pin(async { Err(service_auth::ServiceAuthError::InvalidSignature) }),
            |_| Box::pin(async { panic!("must not fetch when auth fails") }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().body.into_inner().error, "AuthError");
    }
}
