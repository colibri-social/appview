CREATE TABLE IF NOT EXISTS community_bans (
    community_uri TEXT NOT NULL,
    member_did TEXT NOT NULL,
    banned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (community_uri, member_did)
);
