/// Returns `true` if `s` is a single, recognized Unicode emoji.
///
/// The `emojis` crate ships a compile-time list of every emoji in the Unicode
/// standard (CLDR / Emoji 15.x) and performs an O(log n) lookup.
///
/// # TODO — Custom emojis
///
/// To support server-defined custom emojis in the future:
/// 1. Add a `custom_emojis` table (id TEXT PK, name TEXT UNIQUE, image_url TEXT).
/// 2. Agree on a naming convention with the lexicon, e.g. `:emoji_name:` (a
///    colon-wrapped slug) so custom and Unicode emojis are unambiguous.
/// 3. Extend `is_valid_emoji` to accept a `pool: &PgPool` argument and, after
///    the Unicode check fails, query `custom_emojis` for a matching `name`.
/// 4. Cache the custom-emoji set in memory (e.g. a `DashMap` managed by Rocket)
///    so every reaction does not hit the DB.
/// 5. Update the Jetstream handler and backfill path to pass the cache/pool.
pub fn is_valid_emoji(s: &str) -> bool {
    emojis::get(s).is_some()
}
