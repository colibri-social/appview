-- community: rename image -> picture (lexicon uses `picture` blob field)
ALTER TABLE communities RENAME COLUMN image TO picture;

-- categories: remove emoji + parent_rkey (not in lexicon); add channel_order
ALTER TABLE categories DROP COLUMN IF EXISTS emoji;
ALTER TABLE categories DROP COLUMN IF EXISTS parent_rkey;
ALTER TABLE categories ADD COLUMN IF NOT EXISTS channel_order JSONB;
