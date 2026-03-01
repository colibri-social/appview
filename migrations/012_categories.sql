CREATE TABLE IF NOT EXISTS categories (
    uri          TEXT PRIMARY KEY,
    owner_did    TEXT NOT NULL,
    rkey         TEXT NOT NULL,
    community_uri TEXT NOT NULL,
    name         TEXT NOT NULL,
    emoji        TEXT NOT NULL,
    parent_rkey  TEXT
);

CREATE INDEX IF NOT EXISTS idx_categories_community_uri ON categories (community_uri);
