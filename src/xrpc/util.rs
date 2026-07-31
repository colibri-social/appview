use rocket::{data::ByteUnit, fs::TempFile, tokio::io::AsyncReadExt};

use crate::lib::responses::{ErrorCode, ErrorResponse};

pub async fn unpack_image_file(
    f: &Option<TempFile<'_>>,
    max_size: ByteUnit,
) -> Result<Option<(Vec<u8>, String)>, ErrorResponse> {
    let Some(f) = f.as_ref() else {
        return Ok(None);
    };

    if f.len() > max_size {
        return Err(ErrorCode::InvalidRequest.with(format!(
            "picture exceeds the maximum allowed size of {max_size}"
        )));
    }

    let Ok(mut buf) = f.open().await else {
        return Ok(None);
    };

    let mut contents = Vec::with_capacity(f.len() as usize);
    let _ = buf.read_to_end(&mut contents).await;

    let Some(content_type) = f.content_type() else {
        return Ok(None);
    };

    let media_type = content_type.media_type();
    let mime_type = format!("{}/{}", media_type.top(), media_type.sub());

    Ok(Some((contents, mime_type)))
}
