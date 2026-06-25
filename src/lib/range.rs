//! Minimal RFC 7233 single byte-range parser for the blob proxy.
//!
//! The PDS upstream doesn't support Range, so the AppView caches the full blob
//! and serves byte ranges itself. This module turns a raw `Range` request
//! header into a decision the responder can act on. Only a single range is
//! honoured — multi-range requests fall back to serving the whole resource,
//! which browsers never need for media playback.

#[derive(Debug, PartialEq, Eq)]
pub enum RangeResult {
    /// No (or non-byte / multi) Range header — serve the whole resource (200).
    Full,
    /// A satisfiable range, inclusive and 0-based, with `end < total` (206).
    Partial { start: u64, end: u64 },
    /// A syntactically valid range that can't be satisfied for `total` (416).
    Unsatisfiable,
}

/// Parses an HTTP `Range` header against a resource of `total` bytes. Handles
/// `bytes=START-END`, `bytes=START-`, and the suffix form `bytes=-SUFFIX`.
pub fn parse_range(header: Option<&str>, total: u64) -> RangeResult {
    let Some(raw) = header else {
        return RangeResult::Full;
    };

    let Some(spec) = raw.trim().strip_prefix("bytes=") else {
        return RangeResult::Full;
    };

    // Multi-range (comma-separated) isn't supported; serve the whole resource.
    if spec.contains(',') {
        return RangeResult::Full;
    }

    let Some((start_str, end_str)) = spec.trim().split_once('-') else {
        return RangeResult::Full;
    };
    let start_str = start_str.trim();
    let end_str = end_str.trim();

    // An empty resource can never satisfy a byte range.
    if total == 0 {
        return RangeResult::Unsatisfiable;
    }

    // Suffix range: `bytes=-N` → the last N bytes.
    if start_str.is_empty() {
        let suffix: u64 = match end_str.parse() {
            Ok(n) => n,
            Err(_) => return RangeResult::Full,
        };
        if suffix == 0 {
            return RangeResult::Unsatisfiable;
        }
        return RangeResult::Partial {
            start: total.saturating_sub(suffix),
            end: total - 1,
        };
    }

    let start: u64 = match start_str.parse() {
        Ok(n) => n,
        Err(_) => return RangeResult::Full,
    };

    // Range starts past the end of the resource → unsatisfiable.
    if start >= total {
        return RangeResult::Unsatisfiable;
    }

    let end: u64 = if end_str.is_empty() {
        total - 1
    } else {
        match end_str.parse() {
            Ok(n) => n,
            Err(_) => return RangeResult::Full,
        }
    };

    if end < start {
        return RangeResult::Unsatisfiable;
    }

    // Clamp an over-long end to the last byte.
    RangeResult::Partial {
        start,
        end: end.min(total - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_header_is_full() {
        assert_eq!(parse_range(None, 1000), RangeResult::Full);
    }

    #[test]
    fn open_ended_range() {
        assert_eq!(
            parse_range(Some("bytes=0-"), 1000),
            RangeResult::Partial { start: 0, end: 999 }
        );
    }

    #[test]
    fn closed_range_from_zero() {
        assert_eq!(
            parse_range(Some("bytes=0-99"), 1000),
            RangeResult::Partial { start: 0, end: 99 }
        );
    }

    #[test]
    fn closed_range_mid() {
        assert_eq!(
            parse_range(Some("bytes=100-199"), 1000),
            RangeResult::Partial {
                start: 100,
                end: 199
            }
        );
    }

    #[test]
    fn suffix_range() {
        assert_eq!(
            parse_range(Some("bytes=-500"), 1000),
            RangeResult::Partial {
                start: 500,
                end: 999
            }
        );
    }

    #[test]
    fn suffix_larger_than_file_is_whole_file() {
        assert_eq!(
            parse_range(Some("bytes=-5000"), 1000),
            RangeResult::Partial { start: 0, end: 999 }
        );
    }

    #[test]
    fn start_past_end_is_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes=999999-"), 1000),
            RangeResult::Unsatisfiable
        );
    }

    #[test]
    fn reversed_range_is_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes=5-2"), 1000),
            RangeResult::Unsatisfiable
        );
    }

    #[test]
    fn end_clamped_to_last_byte() {
        assert_eq!(
            parse_range(Some("bytes=900-100000"), 1000),
            RangeResult::Partial {
                start: 900,
                end: 999
            }
        );
    }

    #[test]
    fn zero_suffix_is_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes=-0"), 1000),
            RangeResult::Unsatisfiable
        );
    }

    #[test]
    fn non_byte_unit_is_full() {
        assert_eq!(parse_range(Some("items=0-9"), 1000), RangeResult::Full);
    }

    #[test]
    fn multi_range_falls_back_to_full() {
        assert_eq!(
            parse_range(Some("bytes=0-9,20-29"), 1000),
            RangeResult::Full
        );
    }

    #[test]
    fn empty_resource_is_unsatisfiable() {
        assert_eq!(parse_range(Some("bytes=0-"), 0), RangeResult::Unsatisfiable);
    }
}
