package migrations

import (
	"context"
	"database/sql"
	"log/slog"
	"sort"
)

// ApplyMigrations applies all pending migrations to the database
func ApplyMigrations(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	// Get all available migrations
	allMigrations, err := GetMigrations()
	if err != nil {
		return err
	}

	if len(allMigrations) == 0 {
		logger.Info("No migrations found")
		return nil
	}

	// Get applied migrations
	applied, err := getAppliedMigrations(ctx, db)
	if err != nil {
		return err
	}

	// Filter to only pending migrations
	var pending []Migration
	for _, migration := range allMigrations {
		if !applied[migration.Version] {
			pending = append(pending, migration)
		}
	}

	if len(pending) == 0 {
		logger.Info("All migrations already applied", "total_migrations", len(allMigrations))
		return nil
	}

	// Sort by version to ensure correct order
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].Version < pending[j].Version
	})

	logger.Info("Applying migrations",
		"pending_count", len(pending),
		"total_migrations", len(allMigrations))

	// Apply each pending migration in a transaction
	for _, migration := range pending {
		if err := applyMigration(ctx, db, migration, logger); err != nil {
			return err
		}
	}

	logger.Info("All migrations applied successfully")
	return nil
}

// getAppliedMigrations returns a set of applied migration version numbers
func getAppliedMigrations(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	// Check if schema_migrations table exists
	var tableExists bool
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&tableExists)
	if err != nil {
		return nil, err
	}

	applied := make(map[int]bool)
	if !tableExists {
		// Table doesn't exist yet, no migrations applied
		return applied, nil
	}

	// Query applied migrations
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}

	return applied, rows.Err()
}

// applyMigration applies a single migration within a transaction
func applyMigration(ctx context.Context, db *sql.DB, migration Migration, logger *slog.Logger) error {
	logger.Info("Applying migration",
		"version", migration.Version,
		"name", migration.Name)

	// Start transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Execute migration SQL
	if _, err := tx.ExecContext(ctx, migration.SQL); err != nil {
		logger.Error("Migration failed",
			"version", migration.Version,
			"name", migration.Name,
			"error", err)
		return err
	}

	// Record migration as applied
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version) VALUES (?)`,
		migration.Version); err != nil {
		logger.Error("Failed to record migration",
			"version", migration.Version,
			"name", migration.Name,
			"error", err)
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	logger.Info("Migration applied successfully",
		"version", migration.Version,
		"name", migration.Name)

	return nil
}
