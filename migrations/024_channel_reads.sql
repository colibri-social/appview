CREATE TABLE IF NOT EXISTS channel_reads (
    did TEXT NOT NULL,
    channel_uri TEXT NOT NULL,
    cursor_at TIMESTAMPTZ NOT NULL,
    record_rkey TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (did, channel_uri)
);

CREATE INDEX IF NOT EXISTS idx_channel_reads_did ON channel_reads (did);
