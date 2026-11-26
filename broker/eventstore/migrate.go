// Package eventstore holds the versioned schema migrations for the event_store
// database used by the postgres broker backend. The backend runs no DDL.
// A migration executor applies the migrations as event_store_migrate,
// through broker.MigrateEventStore, which calls Migrate.
package eventstore

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// migrateRoleName is the role that owns the event_store schema and must run
// the migrations.
const migrateRoleName = "event_store_migrate"

// migrationLockTimeout bounds the wait for the advisory lock, so a runner
// stuck while holding it makes later runs fail instead of block forever.
const migrationLockTimeout = "5min"

// migrationLockKey is the pg_advisory_xact_lock key that serializes
// concurrent Migrate runs against one database.
const migrationLockKey int64 = 0x6866627273746f72 // "hfbrstor"

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migration is one versioned schema change.
type migration struct {
	Version string
	SQL     string
}

// loadMigrations returns the embedded migrations in apply order.
func loadMigrations() ([]migration, error) {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("list event_store migrations: %w", err)
	}
	sort.Strings(names)
	migrations := make([]migration, 0, len(names))
	for _, name := range names {
		body, err := migrationFiles.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read event_store migration %s: %w", name, err)
		}
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		migrations = append(migrations, migration{Version: version, SQL: string(body)})
	}
	return migrations, nil
}

// Migrate applies every pending migration in one transaction holding an
// advisory lock, so concurrent runners serialize and a rerun is a no-op.
// The connection must authenticate as event_store_migrate, the owner of the
// event_store schema; the event_store_app role must already exist.
func Migrate(ctx context.Context, conn *pgx.Conn) (err error) {
	if conn == nil {
		return errors.New("event_store migrate: connection is nil")
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	// READ COMMITTED regardless of default_transaction_isolation: under
	// REPEATABLE READ or SERIALIZABLE the snapshot is taken by the lock
	// statement before its wait ends, so a runner that waited would not see
	// the schema and versions the previous holder committed.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("event_store migrate: begin: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = fmt.Errorf("%w (rollback: %w)", err, rbErr)
			}
		}
	}()

	if err = applyPending(ctx, tx, migrations); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("event_store migrate: commit: %w", err)
	}
	return nil
}

// applyPending takes the migration lock, ensures the schema and version
// table exist, and applies each unrecorded migration inside tx.
func applyPending(ctx context.Context, tx pgx.Tx, migrations []migration) error {
	// Objects take the owner from current_user, so another role, such as a
	// superuser, would create tables that event_store_migrate cannot alter.
	var role string
	if err := tx.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		return fmt.Errorf("event_store migrate: check role: %w", err)
	}
	if role != migrateRoleName {
		return fmt.Errorf("event_store migrate: must run as %s, connected as %s", migrateRoleName, role)
	}

	// lock_timeout applies to advisory locks too. Restore the previous value
	// after the wait, so the migrations themselves run with it.
	var prevLockTimeout string
	if err := tx.QueryRow(ctx,
		`SELECT current_setting('lock_timeout'), set_config('lock_timeout', $1, true)`, migrationLockTimeout,
	).Scan(&prevLockTimeout, nil); err != nil {
		return fmt.Errorf("event_store migrate: set lock timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("event_store migrate: acquire advisory lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true)`, prevLockTimeout); err != nil {
		return fmt.Errorf("event_store migrate: restore lock timeout: %w", err)
	}

	// CREATE SCHEMA IF NOT EXISTS still needs database-level CREATE, which
	// the migration role lacks when the platform admin pre-created the schema.
	var schemaExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'event_store')`,
	).Scan(&schemaExists); err != nil {
		return fmt.Errorf("event_store migrate: check schema: %w", err)
	}
	if !schemaExists {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA event_store`); err != nil {
			return fmt.Errorf("event_store migrate: create schema: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS event_store.broker_schema_migrations (
			version    TEXT NOT NULL PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("event_store migrate: create version table: %w", err)
	}

	for _, m := range migrations {
		var applied bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM event_store.broker_schema_migrations WHERE version = $1)`, m.Version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("event_store migrate: check version %s: %w", m.Version, err)
		}
		if applied {
			continue
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			return fmt.Errorf("event_store migrate: apply %s: %w", m.Version, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO event_store.broker_schema_migrations (version) VALUES ($1)`, m.Version,
		); err != nil {
			return fmt.Errorf("event_store migrate: record %s: %w", m.Version, err)
		}
	}
	return nil
}
