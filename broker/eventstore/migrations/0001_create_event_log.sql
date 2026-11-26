-- Event log tables for the postgres backend. Runs as event_store_migrate,
-- which owns the event_store schema and every object created here.
-- Partition creation and rotation belong to the maintenance routine.

CREATE TABLE IF NOT EXISTS event_store.broker_subscriptions (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, subscription_id)
);

CREATE TABLE IF NOT EXISTS event_store.broker_topic_seq (
    topic TEXT NOT NULL PRIMARY KEY,
    next  BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS event_store.broker_events (
    topic      TEXT NOT NULL,
    seq        BIGINT NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS broker_events_topic_seq_idx ON event_store.broker_events (topic, seq);

CREATE TABLE IF NOT EXISTS event_store.broker_ranges (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    range_start     BIGINT NOT NULL,
    range_end       BIGINT NOT NULL,
    delivered       BIGINT NOT NULL DEFAULT 0,
    owner           TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending',
    lease_expires   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (topic, subscription_id, range_start)
);

CREATE TABLE IF NOT EXISTS event_store.broker_cursors (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    acked           BIGINT NOT NULL DEFAULT 0,
    reserved        BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, subscription_id)
);

CREATE TABLE IF NOT EXISTS event_store.broker_event_attempts (
    topic           TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    seq             BIGINT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, subscription_id, seq)
);

-- Runtime grants (ADR-0026, ADR-0031): event_store_app gets schema USAGE
-- and DML, never CREATE, and no DELETE on the event-log tables; deletes
-- there run only through the maintenance routine. Attempt counters are
-- per-delivery bookkeeping the backend clears on ack, so they keep DELETE.
REVOKE ALL ON SCHEMA event_store FROM PUBLIC;
GRANT USAGE ON SCHEMA event_store TO event_store_app;

REVOKE ALL ON
    event_store.broker_subscriptions,
    event_store.broker_topic_seq,
    event_store.broker_events,
    event_store.broker_ranges,
    event_store.broker_cursors,
    event_store.broker_event_attempts
FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON
    event_store.broker_subscriptions,
    event_store.broker_topic_seq,
    event_store.broker_events,
    event_store.broker_ranges,
    event_store.broker_cursors
TO event_store_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON event_store.broker_event_attempts TO event_store_app;
