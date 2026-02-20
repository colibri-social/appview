-- Reactions: social.colibri.reaction
CREATE TABLE IF NOT EXISTS reactions (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    rkey        TEXT        NOT NULL,
    author_did  TEXT        NOT NULL,
    emoji       TEXT        NOT NULL,
    target_rkey TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT reactions_author_rkey_unique UNIQUE (author_did, rkey)
);

CREATE INDEX IF NOT EXISTS idx_reactions_target ON reactions (target_rkey);

-- Evolve backfill_state to track progress per (did, collection) pair.
-- Existing rows (social.colibri.message) are preserved via the DEFAULT.
ALTER TABLE backfill_state DROP CONSTRAINT IF EXISTS backfill_state_pkey;
ALTER TABLE backfill_state ADD COLUMN IF NOT EXISTS collection TEXT NOT NULL DEFAULT 'social.colibri.message';
ALTER TABLE backfill_state ADD PRIMARY KEY (did, collection);
