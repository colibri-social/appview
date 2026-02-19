-- Cache of ATProto actor profiles (displayName, avatar).
-- Updated on first sight of each DID; updated via JetStream.
CREATE TABLE IF NOT EXISTS author_profiles (
    did          TEXT        PRIMARY KEY,
    display_name TEXT,
    avatar_url   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tracks per-DID backfill progress.
-- completed = false, cursor IS NULL  → not yet started
-- completed = false, cursor IS NOT NULL → in progress
-- completed = true                   → finished
CREATE TABLE IF NOT EXISTS backfill_state (
    did        TEXT        PRIMARY KEY,
    completed  BOOLEAN     NOT NULL DEFAULT FALSE,
    cursor     TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
