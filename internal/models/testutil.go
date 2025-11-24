package models

import (
	"database/sql"
	"os"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/manujelko/gowtf/internal/db/migrations"
)

func NewTestDB(t *testing.T) *sql.DB {
	t.Helper()

	// Create a unique temp file for the SQLite DB
	tmp, err := os.CreateTemp("", "gowtf-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	path := tmp.Name()

	// Close the hanlde so SQLite can open it
	tmp.Close()

	// Remove the DB file after the test
	t.Cleanup(func() { os.Remove(path) })

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	// Close the DB after the test finishes
	t.Cleanup(func() { db.Close() })

	// Apply migration
	if _, err := db.Exec(migrations.InitialMigration); err != nil {
		t.Fatalf("failed applying migration: %v", err)
	}

	return db
}

func insertTestWorkflow(t *testing.T, db *sql.DB, name string) int {
	t.Helper()

	res, err := db.Exec(
		`INSERT INTO workflows (name, schedule, env, hash, enabled)
		VALUES (?, '* * * * *', NULL, 'test-hash', 1);`,
		name,
	)
	if err != nil {
		t.Fatalf("failed to insert workflow: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("failed to fetch workflow id: %v", err)
	}
	return int(id)
}

func insertTestTask(t *testing.T, db *sql.DB, workflowID int, name string) int {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO workflow_tasks (workflow_id, name, script, retries, retry_delay, timeout, condition, env)
		VALUES (?, ?, 'echo hi', 0, NULL, NULL, NULL, NULL);
	`, workflowID, name)
	if err != nil {
		t.Fatalf("insertTestTask failed: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("insertTestTask lastInsertId failed: %v", err)
	}

	return int(id)
}

func insertTestWorkflowRun(t *testing.T, db *sql.DB, workflowID int) int {
	t.Helper()

	res, err := db.Exec(`
		INSERT INTO workflow_runs (workflow_id, status)
		VALUES (?, 'pending');
	`, workflowID)
	if err != nil {
		t.Fatalf("insertTestWorkflowRun failed: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("insertTestWorkflowRun lastInsertId failed: %v", err)
	}

	return int(id)
}
