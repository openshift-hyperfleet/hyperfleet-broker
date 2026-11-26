-- Platform admin steps for the event_store database (README, Migrations):
-- the role split, the per-database CONNECT/TEMPORARY ACL, CREATE for the
-- migration role so its first run creates the schema, and the runtime
-- role's search_path. initdb runs this on first start as the admin user.
CREATE ROLE event_store_migrate LOGIN PASSWORD 'event_store_migrate';
CREATE ROLE event_store_app LOGIN PASSWORD 'event_store_app';

REVOKE CONNECT, TEMPORARY ON DATABASE event_store FROM PUBLIC;
GRANT CONNECT ON DATABASE event_store TO event_store_migrate, event_store_app;
GRANT TEMPORARY ON DATABASE event_store TO event_store_migrate;
GRANT CREATE ON DATABASE event_store TO event_store_migrate;
ALTER ROLE event_store_app IN DATABASE event_store SET search_path = event_store;
