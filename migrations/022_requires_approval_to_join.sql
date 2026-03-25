ALTER TABLE communities
    ADD COLUMN IF NOT EXISTS requires_approval_to_join BOOLEAN NOT NULL DEFAULT TRUE;
