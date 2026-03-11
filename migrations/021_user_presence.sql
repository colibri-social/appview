ALTER TABLE author_profiles
    ADD COLUMN IF NOT EXISTS state           TEXT,
    ADD COLUMN IF NOT EXISTS preferred_state TEXT;
