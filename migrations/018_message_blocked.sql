ALTER TABLE messages ADD COLUMN blocked BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX idx_messages_blocked ON messages(blocked) WHERE blocked = TRUE;
