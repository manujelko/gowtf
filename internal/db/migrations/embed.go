package migrations

import (
	"embed"
	"sort"
	"strconv"
	"strings"
)

//go:embed *.sql
var migrationsFS embed.FS

// Migration represents a single migration file
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// GetMigrations returns all migrations sorted by version number
func GetMigrations() ([]Migration, error) {
	entries, err := migrationsFS.ReadDir(".")
	if err != nil {
		return nil, err
	}

	var migrations []Migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		// Extract version number from filename (e.g., "0001_initial.sql" -> 1)
		parts := strings.SplitN(entry.Name(), "_", 2)
		if len(parts) < 2 {
			continue
		}

		version, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		// Read migration SQL
		sql, err := migrationsFS.ReadFile(entry.Name())
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, Migration{
			Version: version,
			Name:    entry.Name(),
			SQL:     string(sql),
		})
	}

	// Sort by version number
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
