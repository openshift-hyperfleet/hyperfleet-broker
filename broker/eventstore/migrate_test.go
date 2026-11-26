package eventstore

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var migrationName = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+$`)

// Migrate applies migrations in file-name order and records each by name,
// so a bad name silently changes the order or skips a file.
func TestMigrationFileNames(t *testing.T) {
	// Every file in the directory must be a migration: a file without the
	// .sql suffix is not embedded and would never apply.
	entries, err := os.ReadDir("migrations")
	require.NoError(t, err)
	for _, e := range entries {
		assert.Regexp(t, `\.sql$`, e.Name(), "migrations/ holds only .sql files")
	}

	migrations, err := loadMigrations()
	require.NoError(t, err)
	require.NotEmpty(t, migrations)
	for i, m := range migrations {
		match := migrationName.FindStringSubmatch(m.Version)
		require.NotNil(t, match, "migration %q: name must be NNNN_lowercase_words.sql", m.Version)
		number, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		assert.Equal(t, i+1, number,
			fmt.Sprintf("migration %q: numbers start at 0001 and have no gaps or duplicates", m.Version))
		assert.NotEmpty(t, m.SQL, "migration %q is empty", m.Version)
	}
}
