-- Staging table for approval records that arrive before the corresponding
-- membership row exists. save_membership drains this on insert.
CREATE TABLE pending_approvals (
    approval_uri    TEXT PRIMARY KEY,
    membership_uri  TEXT NOT NULL,
    approval_rkey   TEXT NOT NULL
);
