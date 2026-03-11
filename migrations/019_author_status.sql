ALTER TABLE author_profiles
    ADD COLUMN IF NOT EXISTS status      TEXT,
    ADD COLUMN IF NOT EXISTS emoji       TEXT,
    ADD COLUMN IF NOT EXISTS banner_url  TEXT,
    ADD COLUMN IF NOT EXISTS handle      TEXT;