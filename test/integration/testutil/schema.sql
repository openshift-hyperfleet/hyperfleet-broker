-- Event log schema fixture for the postgres backend integration suite.
-- The production schema and maintenance routine come from the event_store
-- migration artifacts; this fixture mirrors the shape the backend expects
-- and must stay in sync with it.

CREATE TABLE IF NOT EXISTS broker_subscriptions (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, subscription_id)
);

CREATE TABLE IF NOT EXISTS broker_topic_seq (
    topic TEXT NOT NULL PRIMARY KEY,
    next  BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS broker_events (
    topic      TEXT NOT NULL,
    seq        BIGINT NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS broker_events_topic_seq_idx ON broker_events (topic, seq);

CREATE TABLE IF NOT EXISTS broker_ranges (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    range_start     BIGINT NOT NULL,
    range_end       BIGINT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending',
    lease_expires   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (topic, subscription_id, range_start)
);

CREATE TABLE IF NOT EXISTS broker_cursors (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    acked           BIGINT NOT NULL DEFAULT 0,
    reserved        BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, subscription_id)
);

CREATE TABLE IF NOT EXISTS broker_event_attempts (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    seq             BIGINT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, subscription_id, seq)
);

-- broker_maintenance rotates the event log's daily partitions and cleans
-- pending ranges below the retention boundary. The retention reaper
-- invokes it with the configured retention window; the one-day
-- pre-create margin is fixed inside. All date math runs in UTC.
CREATE OR REPLACE FUNCTION broker_maintenance(p_retention_secs double precision)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    today_date date;
    d date;
    part text;
    cutoff timestamptz;
    part_row record;
BEGIN
    PERFORM set_config('TimeZone', 'utc', true);
    today_date := (now() AT TIME ZONE 'utc')::date;
    cutoff := now() - make_interval(secs => p_retention_secs);

    -- Partition pre-creation: today and one day ahead, so inserts never
    -- land on a missing partition.
    FOREACH d IN ARRAY ARRAY[today_date, today_date + 1] LOOP
        part := 'broker_events_' || to_char(d, 'YYYYMMDD');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF broker_events FOR VALUES FROM (%L) TO (%L)',
            part, d, d + 1);
    END LOOP;

    -- Expired-partition drop: retention by rotation, no per-row deletes.
    FOR part_row IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'broker_events'::regclass
    LOOP
        IF part_row.relname ~ '^broker_events_[0-9]{8}$' THEN
            d := to_date(substring(part_row.relname from '[0-9]{8}$'), 'YYYYMMDD');
            IF ((d + 1)::timestamp AT TIME ZONE 'utc') < cutoff THEN
                EXECUTE format('DROP TABLE IF EXISTS %I', part_row.relname);
            END IF;
        END IF;
    END LOOP;

    -- Pending-range cleanup: ranges entirely below the oldest retained
    -- sequence of their topic; a topic with no retained rows has all of
    -- its ranges qualify.
    DELETE FROM broker_ranges br
    WHERE NOT EXISTS (
        SELECT 1 FROM broker_events e
        WHERE e.topic = br.topic AND e.seq <= br.range_end
    );
END;
$$;
