-- TODO(HYPERFLEET-1878): delete this file when the routine ships as an
-- event_store migration.
--
-- Stand-in for the event_store maintenance routine until HYPERFLEET-1878
-- ships it as a migration. The tables come from the event_store migrations
-- (broker/eventstore/migrations). This file holds only the routine that the
-- backend's retention reaper invokes. Install it as event_store_migrate.
-- It follows ADR-0031: SECURITY DEFINER owned by the migration role, a
-- fixed search_path, qualified references, and EXECUTE for event_store_app.

-- broker_maintenance rotates the event log's daily partitions and cleans
-- pending ranges below the retention boundary. The retention reaper
-- invokes it with the configured retention window; the one-day
-- pre-create margin is fixed inside. All date math runs in UTC.
CREATE OR REPLACE FUNCTION event_store.broker_maintenance(p_retention_secs double precision)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = event_store, pg_temp
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
            'CREATE TABLE IF NOT EXISTS event_store.%I PARTITION OF event_store.broker_events FOR VALUES FROM (%L) TO (%L)',
            part, d, d + 1);
    END LOOP;

    -- Expired-partition drop: retention by rotation, no per-row deletes.
    FOR part_row IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'event_store.broker_events'::regclass
    LOOP
        IF part_row.relname ~ '^broker_events_[0-9]{8}$' THEN
            d := to_date(substring(part_row.relname from '[0-9]{8}$'), 'YYYYMMDD');
            IF ((d + 1)::timestamp AT TIME ZONE 'utc') < cutoff THEN
                EXECUTE format('DROP TABLE IF EXISTS event_store.%I', part_row.relname);
            END IF;
        END IF;
    END LOOP;

    -- Pending-range cleanup: ranges entirely below the oldest retained
    -- sequence of their topic; a topic with no retained rows has all of
    -- its ranges qualify.
    DELETE FROM event_store.broker_ranges br
    WHERE NOT EXISTS (
        SELECT 1 FROM event_store.broker_events e
        WHERE e.topic = br.topic AND e.seq <= br.range_end
    );
END;
$$;

REVOKE EXECUTE ON FUNCTION event_store.broker_maintenance(double precision) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.broker_maintenance(double precision) TO event_store_app;
