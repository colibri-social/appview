-- Add owner_only flag to channels
ALTER TABLE channels ADD COLUMN owner_only BOOLEAN NOT NULL DEFAULT false;
