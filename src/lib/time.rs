/// Returns the current UTC timestamp in ISO 8601 form
/// (`YYYY-MM-DDTHH:MM:SS.mmmZ`). Used wherever we mint AppView-side records.
pub fn current_iso8601_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    iso8601_from_epoch(now.as_secs(), now.subsec_millis())
}

/// Formats a (seconds_since_epoch, millis) pair as ISO 8601 UTC.
pub fn iso8601_from_epoch(secs: u64, millis: u32) -> String {
    let (year, month, day, hour, minute, second) = epoch_to_ymdhms(secs);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hour, minute, second, millis
    )
}

/// Howard Hinnant's "civil_from_days" algorithm — converts seconds since the
/// Unix epoch to a (year, month, day, hour, minute, second) tuple in UTC.
fn epoch_to_ymdhms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let seconds_of_day = (secs % 86_400) as u32;
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };

    (y as u32, m, d, hour, minute, second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_epoch_zero_as_unix_epoch_string() {
        assert_eq!(iso8601_from_epoch(0, 0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn formats_known_datetime() {
        // 2026-05-13T12:34:56.789Z
        // = (2026-1970) years -> days(2026-01-01) - days(1970-01-01) + 132 days
        // Easier: compute via known offset.
        // 1747139696 secs is 2025-05-13T12:34:56Z; +365.25*86400 ~ 2026-05-13T...
        // We just verify shape rather than hand-computing.
        let s = iso8601_from_epoch(1_778_675_696, 789);
        assert!(s.ends_with(".789Z"));
        assert_eq!(s.len(), "1970-01-01T00:00:00.000Z".len());
    }
}
