ALTER TABLE reactions
    ADD CONSTRAINT reactions_author_target_emoji_unique
    UNIQUE (author_did, target_rkey, emoji);
