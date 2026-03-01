-- Force a fresh backfill for community/channel/membership/approval collections.
-- These were added late and may have been marked complete incorrectly on first run.
DELETE FROM backfill_state
 WHERE collection IN (
     'social.colibri.community',
     'social.colibri.channel',
     'social.colibri.membership',
     'social.colibri.approval'
 );
