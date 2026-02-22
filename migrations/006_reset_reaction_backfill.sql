-- Reactions were previously backfilled with the full AT-URI stored in target_rkey
-- instead of just the rkey component. Reset completed reaction backfill entries so
-- they re-run and populate target_rkey correctly.

-- Also clear any reactions that were stored with a full AT-URI (they will be
-- re-inserted correctly during the re-run backfill).
DELETE FROM reactions WHERE target_rkey LIKE 'at://%';

-- Mark all reaction backfill entries as incomplete so the backfill re-runs.
UPDATE backfill_state
SET completed = FALSE, cursor = NULL, updated_at = NOW()
WHERE collection = 'social.colibri.reaction';
