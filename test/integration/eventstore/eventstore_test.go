package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker/eventstore"
	"github.com/openshift-hyperfleet/hyperfleet-broker/test/integration/testutil"
)

const (
	migrateRole = testutil.EventStoreMigrateRole
	appRole     = testutil.EventStoreAppRole
	// operatorRole is hyperfleet-operator's login; it holds operatorGroupRole,
	// so the access check covers a role the operator holds, not only its own.
	operatorRole      = "hyperfleet-operator"
	operatorGroupRole = "hyperfleet_operator_access"
)

var adminDSN string

// TestMain starts one Postgres container and creates the operator's roles.
// Each test provisions its own database the way the platform admin does
// (testutil.ProvisionEventStoreDatabase), so cases with different database
// settings do not share state.
func TestMain(m *testing.M) {
	testutil.SetupTestEnvironment()
	ctx := context.Background()

	container, dsn, err := testutil.StartPostgresContainer(ctx, testutil.PostgresContainerRequest())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n", err)
		os.Exit(1)
	}
	adminDSN = dsn

	if err := createOperatorRoles(ctx); err != nil {
		if termErr := container.Terminate(ctx); termErr != nil {
			fmt.Fprintf(os.Stderr, "failed to terminate postgres container: %v\n", termErr)
		}
		fmt.Fprintf(os.Stderr, "failed to create operator roles: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate postgres container: %v\n", err)
	}
	os.Exit(code)
}

func createOperatorRoles(ctx context.Context) error {
	conn, err := connectAs(ctx, "", "")
	if err != nil {
		return err
	}
	defer testutil.CloseConn(conn)
	op := pgx.Identifier{operatorRole}.Sanitize()
	for _, stmt := range []string{
		`CREATE ROLE ` + op + ` LOGIN`,
		`CREATE ROLE ` + operatorGroupRole,
		`GRANT ` + operatorGroupRole + ` TO ` + op,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// connectAs opens a connection as role (admin when empty) to db (the admin
// database when empty).
func connectAs(ctx context.Context, role, db string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		return nil, errors.New("failed to parse postgres DSN")
	}
	if role != "" {
		cfg.User = role
	}
	if db != "" {
		cfg.Database = db
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect as %q to %q: %w", role, db, err)
	}
	return conn, nil
}

// newEventStoreDatabase creates database db, applies adminSettings to it,
// and provisions it for the event_store roles.
func newEventStoreDatabase(t *testing.T, db string, precreateSchema bool, adminSettings ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	admin, err := connectAs(ctx, "", "")
	require.NoError(t, err)
	defer testutil.CloseConn(admin)
	dbIdent := pgx.Identifier{db}.Sanitize()
	_, err = admin.Exec(ctx, `CREATE DATABASE `+dbIdent)
	require.NoError(t, err)
	// Drop the database after the test, so a repeated run (-count) can
	// create it again. FORCE ends connections a failed test left open.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		conn, connErr := connectAs(cleanupCtx, "", "")
		if connErr != nil {
			t.Errorf("drop database %s: %v", db, connErr)
			return
		}
		defer testutil.CloseConn(conn)
		if _, dropErr := conn.Exec(cleanupCtx, `DROP DATABASE IF EXISTS `+dbIdent+` WITH (FORCE)`); dropErr != nil {
			t.Errorf("drop database %s: %v", db, dropErr)
		}
	})
	for _, stmt := range adminSettings {
		_, err = admin.Exec(ctx, stmt)
		require.NoError(t, err)
	}

	adminInDB, err := connectAs(ctx, "", db)
	require.NoError(t, err)
	defer testutil.CloseConn(adminInDB)
	require.NoError(t, testutil.ProvisionEventStoreDatabase(ctx, adminInDB, precreateSchema))
}

// migrateConcurrently runs concurrent migrators against db and then a
// rerun; every run must succeed and record each migration exactly once,
// with the schema owned by the migration role.
func migrateConcurrently(t *testing.T, db string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const runners = 4
	conns := make([]*pgx.Conn, runners)
	for i := range conns {
		conn, err := connectAs(ctx, migrateRole, db)
		require.NoError(t, err)
		defer testutil.CloseConn(conn)
		conns[i] = conn
	}
	// Connect first and release the runners together, so they contend on
	// the lock instead of running one after another.
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, runners)
	for i, conn := range conns {
		wg.Go(func() {
			<-start
			errs[i] = eventstore.Migrate(ctx, conn)
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "concurrent migrator %d", i)
	}

	conn, err := connectAs(ctx, migrateRole, db)
	require.NoError(t, err)
	defer testutil.CloseConn(conn)
	require.NoError(t, eventstore.Migrate(ctx, conn), "rerun must be a no-op")

	migrationFiles, err := filepath.Glob("../../../broker/eventstore/migrations/*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, migrationFiles)
	var recorded int
	require.NoError(t, conn.QueryRow(ctx,
		`SELECT count(*) FROM event_store.broker_schema_migrations`).Scan(&recorded))
	assert.Equal(t, len(migrationFiles), recorded)

	var owner string
	require.NoError(t, conn.QueryRow(ctx,
		`SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'event_store'`).Scan(&owner))
	assert.Equal(t, migrateRole, owner)
}

func TestMigrateConcurrentRunsAndRerun(t *testing.T) {
	const db = "es_concurrent"
	newEventStoreDatabase(t, db, false)
	migrateConcurrently(t, db)
}

// A database whose default isolation is REPEATABLE READ must not break the
// advisory lock: a waiting runner has to see what the winner committed.
func TestMigrateUnderRepeatableReadDefault(t *testing.T) {
	const db = "es_repeatable_read"
	newEventStoreDatabase(t, db, false,
		`ALTER DATABASE `+db+` SET default_transaction_isolation = 'repeatable read'`)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := connectAs(ctx, migrateRole, db)
	require.NoError(t, err)
	var isolation string
	require.NoError(t, conn.QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&isolation))
	testutil.CloseConn(conn)
	require.Equal(t, "repeatable read", isolation, "the fixture must set the non-default isolation")

	migrateConcurrently(t, db)
}

// The platform admin may pre-create the schema owned by the migration role
// instead of granting database CREATE; Migrate must not need that grant.
func TestMigrateIntoPrecreatedSchema(t *testing.T) {
	const db = "es_precreated"
	newEventStoreDatabase(t, db, true)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := connectAs(ctx, "", db)
	require.NoError(t, err)
	var canCreate bool
	require.NoError(t, admin.QueryRow(ctx,
		`SELECT has_database_privilege($1, current_database(), 'CREATE')`, migrateRole).Scan(&canCreate))
	testutil.CloseConn(admin)
	require.False(t, canCreate, "the fixture must not grant the migration role database CREATE")

	migrateConcurrently(t, db)
}

func requireInsufficientPrivilege(t *testing.T, err error, stmt string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "statement must be refused: %s", stmt)
	assert.Equal(t, "42501", pgErr.Code, "statement %q: %s", stmt, pgErr.Message)
}

// Objects take their owner from the migrating role, so any other role must
// be refused before it creates anything.
func TestMigrateRefusesOtherRole(t *testing.T) {
	const db = "es_wrong_role"
	newEventStoreDatabase(t, db, false)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := connectAs(ctx, "", db)
	require.NoError(t, err)
	defer testutil.CloseConn(admin)

	err = eventstore.Migrate(ctx, admin)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must run as "+migrateRole)

	var schemaExists bool
	require.NoError(t, admin.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'event_store')`).Scan(&schemaExists))
	assert.False(t, schemaExists, "a refused run must create nothing")
}

func TestEventStoreRolePermissions(t *testing.T) {
	const db = "es_roles"
	newEventStoreDatabase(t, db, false)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	owner, err := connectAs(ctx, migrateRole, db)
	require.NoError(t, err)
	defer testutil.CloseConn(owner)
	require.NoError(t, eventstore.Migrate(ctx, owner))

	// Seed one partition and row as the owner so DELETE has a target.
	_, err = owner.Exec(ctx, `
		CREATE TABLE event_store.broker_events_test PARTITION OF event_store.broker_events
			FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
		INSERT INTO event_store.broker_events (topic, seq, payload) VALUES ('t', 1, '{}');
		INSERT INTO event_store.broker_ranges (topic, subscription_id, range_start, range_end, owner, lease_expires)
			VALUES ('t', 's', 1, 1, 'o', NOW());`)
	require.NoError(t, err)

	app, err := connectAs(ctx, appRole, db)
	require.NoError(t, err)
	defer testutil.CloseConn(app)

	t.Run("runtime DML works", func(t *testing.T) {
		for _, stmt := range []string{
			`INSERT INTO broker_topic_seq (topic, next) VALUES ('t', 1) ON CONFLICT (topic) DO NOTHING`,
			`UPDATE broker_topic_seq SET next = next + 1 WHERE topic = 't'`,
			`INSERT INTO broker_events (topic, seq, payload) VALUES ('t', 2, '{}')`,
			`SELECT count(*) FROM broker_events`,
			`INSERT INTO broker_subscriptions (topic, subscription_id) VALUES ('t', 's')`,
			`INSERT INTO broker_cursors (topic, subscription_id) VALUES ('t', 's')`,
			`UPDATE broker_ranges SET state = 'pending' WHERE topic = 't'`,
			`INSERT INTO broker_event_attempts (topic, subscription_id, seq, attempts) VALUES ('t', 's', 1, 1)`,
			`DELETE FROM broker_event_attempts WHERE topic = 't'`,
		} {
			_, err := app.Exec(ctx, stmt)
			assert.NoError(t, err, stmt)
		}
	})

	t.Run("runtime cannot DELETE event-log rows", func(t *testing.T) {
		for _, table := range []string{
			"broker_events", "broker_ranges", "broker_cursors", "broker_subscriptions", "broker_topic_seq",
		} {
			stmt := "DELETE FROM " + table
			_, err := app.Exec(ctx, stmt)
			requireInsufficientPrivilege(t, err, stmt)
			stmt = "TRUNCATE " + table
			_, err = app.Exec(ctx, stmt)
			requireInsufficientPrivilege(t, err, stmt)
		}
	})

	t.Run("runtime cannot run DDL", func(t *testing.T) {
		for _, stmt := range []string{
			`CREATE TABLE event_store.app_owned (id INT)`,
			`CREATE TABLE public.app_owned (id INT)`,
			`CREATE TEMP TABLE app_temp (id INT)`,
			`CREATE TABLE event_store.broker_events_19700101 PARTITION OF event_store.broker_events
				FOR VALUES FROM ('1970-01-01') TO ('1970-01-02')`,
			`ALTER TABLE broker_events ADD COLUMN extra INT`,
			`DROP TABLE broker_events_test`,
			`DROP TABLE broker_ranges`,
			`CREATE INDEX app_idx ON broker_cursors (acked)`,
			`CREATE SCHEMA app_schema`,
		} {
			_, err := app.Exec(ctx, stmt)
			requireInsufficientPrivilege(t, err, stmt)
		}
	})

	t.Run("no other role is granted access", func(t *testing.T) {
		rows, err := owner.Query(ctx, `
			SELECT DISTINCT grantee FROM information_schema.role_table_grants
			WHERE table_schema = 'event_store'
			UNION
			SELECT DISTINCT COALESCE(r.rolname, 'PUBLIC') FROM pg_namespace n
				CROSS JOIN LATERAL aclexplode(n.nspacl) a
				LEFT JOIN pg_roles r ON r.oid = a.grantee
			WHERE n.nspname = 'event_store'`)
		require.NoError(t, err)
		grantees, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{migrateRole, appRole}, grantees)
	})

	t.Run("no role held by hyperfleet-operator has database access", func(t *testing.T) {
		_, err := connectAs(ctx, operatorRole, db)
		requireInsufficientPrivilege(t, err, "operator CONNECT")

		admin, err := connectAs(ctx, "", db)
		require.NoError(t, err)
		defer testutil.CloseConn(admin)
		rows, err := admin.Query(ctx, `
			SELECT r.rolname,
				has_database_privilege(r.oid, current_database(), 'CONNECT')
					OR has_database_privilege(r.oid, current_database(), 'TEMPORARY')
					OR has_database_privilege(r.oid, current_database(), 'CREATE')
					OR has_schema_privilege(r.oid, 'event_store', 'USAGE')
					OR EXISTS (
						SELECT 1 FROM pg_class c
						WHERE c.relnamespace = 'event_store'::regnamespace
						  AND has_table_privilege(r.oid, c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE')
					)
			FROM pg_roles r
			WHERE pg_has_role($1, r.oid, 'MEMBER')`, operatorRole)
		require.NoError(t, err)
		type roleAccess struct {
			Name      string
			HasAccess bool
		}
		held, err := pgx.CollectRows(rows, pgx.RowToStructByPos[roleAccess])
		require.NoError(t, err)
		names := make([]string, 0, len(held))
		for _, r := range held {
			names = append(names, r.Name)
			assert.False(t, r.HasAccess, "role %q held by hyperfleet-operator has event_store access", r.Name)
		}
		require.ElementsMatch(t, []string{operatorRole, operatorGroupRole}, names,
			"the check must cover the operator's login and the role it holds")
	})
}
