-- Communities: indexed from social.colibri.community records
CREATE TABLE communities (
    uri         TEXT        PRIMARY KEY,  -- at://owner_did/social.colibri.community/rkey
    owner_did   TEXT        NOT NULL,
    rkey        TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    description TEXT,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Channels: indexed from social.colibri.channel records
-- community_uri is a plain TEXT reference (no FK) to avoid ordering constraints
-- when Jetstream delivers channels before their community.
CREATE TABLE channels (
    uri           TEXT        PRIMARY KEY,  -- at://owner_did/social.colibri.channel/rkey
    owner_did     TEXT        NOT NULL,
    rkey          TEXT        NOT NULL,
    community_uri TEXT        NOT NULL,
    name          TEXT        NOT NULL,
    description   TEXT,
    channel_type  TEXT        NOT NULL DEFAULT 'text',
    category_rkey TEXT,
    indexed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_channels_rkey      ON channels(rkey);
CREATE INDEX idx_channels_community ON channels(community_uri);

-- Two-sided membership: member writes social.colibri.membership,
-- community owner writes social.colibri.approval referencing it.
-- Status is 'pending' until the approval record is indexed.
CREATE TABLE community_members (
    id              BIGSERIAL   PRIMARY KEY,
    community_uri   TEXT        NOT NULL,
    member_did      TEXT        NOT NULL,
    membership_rkey TEXT        NOT NULL,
    membership_uri  TEXT        NOT NULL UNIQUE,  -- at://member_did/social.colibri.membership/rkey
    approval_rkey   TEXT,
    approval_uri    TEXT        UNIQUE,            -- at://owner_did/social.colibri.approval/rkey
    status          TEXT        NOT NULL DEFAULT 'pending',  -- pending | approved
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(community_uri, member_did)
);

CREATE INDEX idx_community_members_community ON community_members(community_uri);
CREATE INDEX idx_community_members_member    ON community_members(member_did);

-- Invite codes: stored in the appview DB; created by the website on the
-- community owner's behalf.  max_uses = NULL means unlimited.
CREATE TABLE invite_codes (
    code            TEXT        PRIMARY KEY,
    community_uri   TEXT        NOT NULL,
    created_by_did  TEXT        NOT NULL,
    max_uses        INT,
    use_count       INT         NOT NULL DEFAULT 0,
    active          BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
