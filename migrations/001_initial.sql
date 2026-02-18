-- Messages from the social.colibri.message ATProto lexicon
CREATE TABLE IF NOT EXISTS messages (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    rkey        TEXT        NOT NULL,
    author_did  TEXT        NOT NULL,
    text        TEXT        NOT NULL,
    channel     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT messages_author_rkey_unique UNIQUE (author_did, rkey)
);

CREATE INDEX IF NOT EXISTS idx_messages_channel    ON messages (channel, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_author     ON messages (author_did, created_at DESC);

-- Tracks the last known status for each DID.
-- Status values are application-defined (e.g. "online", "away", "offline").
CREATE TABLE IF NOT EXISTS user_status (
    did         TEXT        PRIMARY KEY,
    status      TEXT        NOT NULL DEFAULT 'offline',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
