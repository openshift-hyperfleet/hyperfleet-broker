package testutil

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
)

// Roles of the event_store role split (ADR-0026). Roles are cluster-global,
// so one pair serves every database in a test container.
const (
	EventStoreMigrateRole = "event_store_migrate"
	EventStoreAppRole     = "event_store_app"
)

// ProvisionEventStoreDatabase performs the platform admin's steps for the
// database adminConn is connected to, as the co-located databases design
// prescribes: it creates both roles if missing, revokes CONNECT and
// TEMPORARY from PUBLIC, grants CONNECT to both roles and TEMPORARY to the
// migration role only, and pins the runtime role's search_path to
// event_store. With precreateSchema it creates the event_store schema owned
// by the migration role and grants no database CREATE; otherwise it grants
// the migration role database CREATE so the first migration creates the
// schema.
func ProvisionEventStoreDatabase(ctx context.Context, adminConn *pgx.Conn, precreateSchema bool) error {
	var db string
	if err := adminConn.QueryRow(ctx, `SELECT current_database()`).Scan(&db); err != nil {
		return fmt.Errorf("read current database: %w", err)
	}
	dbIdent := pgx.Identifier{db}.Sanitize()
	stmts := []string{
		fmt.Sprintf(`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%[1]s') THEN CREATE ROLE %[1]s LOGIN; END IF;
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%[2]s') THEN CREATE ROLE %[2]s LOGIN; END IF;
		END $$`, EventStoreMigrateRole, EventStoreAppRole),
		fmt.Sprintf(`REVOKE CONNECT, TEMPORARY ON DATABASE %s FROM PUBLIC`, dbIdent),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s, %s`, dbIdent, EventStoreMigrateRole, EventStoreAppRole),
		fmt.Sprintf(`GRANT TEMPORARY ON DATABASE %s TO %s`, dbIdent, EventStoreMigrateRole),
		fmt.Sprintf(`ALTER ROLE %s IN DATABASE %s SET search_path = event_store`, EventStoreAppRole, dbIdent),
	}
	if precreateSchema {
		stmts = append(stmts, fmt.Sprintf(`CREATE SCHEMA event_store AUTHORIZATION %s`, EventStoreMigrateRole))
	} else {
		stmts = append(stmts, fmt.Sprintf(`GRANT CREATE ON DATABASE %s TO %s`, dbIdent, EventStoreMigrateRole))
	}
	for _, stmt := range stmts {
		if _, err := adminConn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("provision event_store database %s: %w", db, err)
		}
	}
	return nil
}

// dsnWithUser returns dsn with its user replaced by user and no password;
// the test containers use trust authentication.
func dsnWithUser(dsn, user string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse postgres DSN: %w", err)
	}
	u.User = url.User(user)
	return u.String(), nil
}
