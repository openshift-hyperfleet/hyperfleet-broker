-- Create the live daily partitions. Runs after the schema fixture (01),
-- which defines the maintenance routine; the event_store migration
-- artifacts will own both in production.
SELECT broker_maintenance(86400);
