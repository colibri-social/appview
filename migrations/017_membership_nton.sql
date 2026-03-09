-- Drop the unique constraint that enforced one membership row per (community,
-- member) pair.  Each social.colibri.membership record is now its own row,
-- keyed only by membership_uri.  Queries that list members / communities use
-- DISTINCT ON to collapse multiple rows for the same member.
ALTER TABLE community_members
    DROP CONSTRAINT community_members_community_uri_member_did_key;
