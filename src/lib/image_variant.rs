use std::io::Cursor;

use bytes::Bytes;
use image::{DynamicImage, ImageEncoder, ImageReader, imageops::FilterType};

pub const JPEG_QUALITY: u8 = 82;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    Small,
    Base,
    Large,
}

impl Variant {
    pub const ALL: [Variant; 3] = [Variant::Small, Variant::Base, Variant::Large];

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "small" => Some(Variant::Small),
            "base" => Some(Variant::Base),
            "large" => Some(Variant::Large),
            _ => None,
        }
    }

    pub fn edge(self) -> u32 {
        match self {
            Variant::Small => 48,
            Variant::Base => 96,
            Variant::Large => 160,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Variant::Small => "small",
            Variant::Base => "base",
            Variant::Large => "large",
        }
    }
}

pub struct Rendered {
    pub bytes: Bytes,
    pub content_type: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("unsupported image data: {0}")]
    Decode(String),
    #[error("failed to encode variant: {0}")]
    Encode(String),
}

pub fn is_resizable(content_type: &str) -> bool {
    let base = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    matches!(
        base.as_str(),
        "image/jpeg"
            | "image/jpg"
            | "image/png"
            | "image/webp"
            | "image/gif"
            | "image/bmp"
            | "image/x-icon"
            | "image/vnd.microsoft.icon"
            | "image/tiff"
    )
}

/// Decodes `bytes` once and derives every [`Variant`] from it, so a single
/// upstream fetch and a single decode serve an avatar at all the sizes the
/// client asks for.
pub fn render_all(bytes: &[u8]) -> Result<Vec<(Variant, Rendered)>, RenderError> {
    let decoded = decode(bytes)?;

    Variant::ALL
        .into_iter()
        .map(|variant| render_decoded(&decoded, variant).map(|rendered| (variant, rendered)))
        .collect()
}

fn decode(bytes: &[u8]) -> Result<DynamicImage, RenderError> {
    ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| RenderError::Decode(e.to_string()))?
        .decode()
        .map_err(|e| RenderError::Decode(e.to_string()))
}

pub fn dimensions(bytes: &[u8]) -> Result<(u32, u32), RenderError> {
    ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| RenderError::Decode(e.to_string()))?
        .into_dimensions()
        .map_err(|e| RenderError::Decode(e.to_string()))
}

fn render_decoded(decoded: &DynamicImage, variant: Variant) -> Result<Rendered, RenderError> {
    let edge = variant.edge();
    let resized = if decoded.width() <= edge && decoded.height() <= edge {
        decoded.clone()
    } else {
        decoded.resize_to_fill(edge, edge, FilterType::Triangle)
    };

    if has_alpha(&resized) {
        encode_png(&resized)
    } else {
        encode_jpeg(&resized)
    }
}

fn has_alpha(image: &DynamicImage) -> bool {
    image.color().has_alpha()
}

fn encode_png(image: &DynamicImage) -> Result<Rendered, RenderError> {
    let rgba = image.to_rgba8();
    let mut out = Vec::new();

    image::codecs::png::PngEncoder::new_with_quality(
        &mut out,
        image::codecs::png::CompressionType::Best,
        image::codecs::png::FilterType::Adaptive,
    )
    .write_image(
        rgba.as_raw(),
        rgba.width(),
        rgba.height(),
        image::ExtendedColorType::Rgba8,
    )
    .map_err(|e| RenderError::Encode(e.to_string()))?;

    Ok(Rendered {
        bytes: Bytes::from(out),
        content_type: String::from("image/png"),
    })
}

fn encode_jpeg(image: &DynamicImage) -> Result<Rendered, RenderError> {
    let rgb = image.to_rgb8();
    let mut out = Vec::new();

    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, JPEG_QUALITY)
        .write_image(
            rgb.as_raw(),
            rgb.width(),
            rgb.height(),
            image::ExtendedColorType::Rgb8,
        )
        .map_err(|e| RenderError::Encode(e.to_string()))?;

    Ok(Rendered {
        bytes: Bytes::from(out),
        content_type: String::from("image/jpeg"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageFormat, Rgb, RgbImage, Rgba, RgbaImage};

    fn render(bytes: &[u8], variant: Variant) -> Result<Rendered, RenderError> {
        render_decoded(&decode(bytes)?, variant)
    }

    fn opaque_source(size: u32) -> Vec<u8> {
        let mut img = RgbImage::new(size, size);
        for (x, y, px) in img.enumerate_pixels_mut() {
            *px = Rgb([(x % 256) as u8, (y % 256) as u8, 128]);
        }
        let mut out = Vec::new();
        DynamicImage::ImageRgb8(img)
            .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
            .unwrap();
        out
    }

    fn alpha_source(size: u32) -> Vec<u8> {
        let mut img = RgbaImage::new(size, size);
        for (x, y, px) in img.enumerate_pixels_mut() {
            *px = Rgba([(x % 256) as u8, (y % 256) as u8, 200, 128]);
        }
        let mut out = Vec::new();
        DynamicImage::ImageRgba8(img)
            .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
            .unwrap();
        out
    }

    fn dimensions(bytes: &[u8]) -> (u32, u32) {
        let img = ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .unwrap()
            .decode()
            .unwrap();
        (img.width(), img.height())
    }

    #[test]
    fn parses_only_known_variants() {
        assert_eq!(Variant::parse("small"), Some(Variant::Small));
        assert_eq!(Variant::parse("base"), Some(Variant::Base));
        assert_eq!(Variant::parse("large"), Some(Variant::Large));
        assert_eq!(Variant::parse("2048"), None);
        assert_eq!(Variant::parse(""), None);
    }

    #[test]
    fn downscales_to_the_variant_edge() {
        let rendered = render(&opaque_source(512), Variant::Base).unwrap();
        assert_eq!(dimensions(&rendered.bytes), (96, 96));
    }

    #[test]
    fn opaque_sources_encode_as_jpeg() {
        let rendered = render(&opaque_source(512), Variant::Small).unwrap();
        assert_eq!(rendered.content_type, "image/jpeg");
    }

    #[test]
    fn transparent_sources_keep_their_alpha_channel() {
        let rendered = render(&alpha_source(512), Variant::Small).unwrap();
        assert_eq!(rendered.content_type, "image/png");

        let decoded = ImageReader::new(Cursor::new(&rendered.bytes))
            .with_guessed_format()
            .unwrap()
            .decode()
            .unwrap();
        assert!(decoded.color().has_alpha());
    }

    /// The point of the whole module: whatever the source weighs, a rendition
    /// is small in absolute terms. Real avatars run to hundreds of kilobytes.
    #[test]
    fn every_variant_of_a_large_source_stays_tiny() {
        let source = opaque_source(1024);

        for (variant, out) in render_all(&source).unwrap() {
            assert!(
                out.bytes.len() < 16 * 1024,
                "{} variant is {} bytes, source was {} bytes",
                variant.as_str(),
                out.bytes.len(),
                source.len()
            );
        }
    }

    #[test]
    fn sources_smaller_than_the_edge_are_not_upscaled() {
        let rendered = render(&opaque_source(32), Variant::Large).unwrap();
        assert_eq!(dimensions(&rendered.bytes), (32, 32));
    }

    #[test]
    fn render_all_covers_every_variant_from_one_decode() {
        let rendered = render_all(&opaque_source(512)).unwrap();

        assert_eq!(rendered.len(), Variant::ALL.len());
        for (variant, out) in &rendered {
            assert_eq!(dimensions(&out.bytes), (variant.edge(), variant.edge()));
        }
    }

    #[test]
    fn non_image_bytes_fail_to_decode() {
        assert!(render(b"definitely not an image", Variant::Base).is_err());
    }

    #[test]
    fn only_decodable_image_types_are_resizable() {
        assert!(is_resizable("image/jpeg"));
        assert!(is_resizable("image/png; charset=binary"));
        assert!(is_resizable("IMAGE/WEBP"));
        assert!(!is_resizable("image/svg+xml"));
        assert!(!is_resizable("video/mp4"));
        assert!(!is_resizable("application/octet-stream"));
    }
}
