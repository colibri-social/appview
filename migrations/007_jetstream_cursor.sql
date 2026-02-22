-- Persists the Jetstream time_us cursor so we can resume after a restart
-- and replay any events we missed during downtime.
CREATE TABLE jetstream_cursor (
    id         BOOLEAN     PRIMARY KEY DEFAULT TRUE,
    time_us    BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = TRUE)
);

INSERT INTO jetstream_cursor (id, time_us) VALUES (TRUE, 0) ON CONFLICT DO NOTHING;
