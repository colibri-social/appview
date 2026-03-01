-- Force a fresh backfill for communities so any records that were saved
-- before migration 013 (image → picture rename) or during the transition
-- window where the column rename hadn't applied yet are re-fetched with the
-- correct `picture` field.
DELETE FROM backfill_state
 WHERE collection = 'social.colibri.community';
