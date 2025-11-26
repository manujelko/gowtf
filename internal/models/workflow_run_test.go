package models

import (
	"context"
	"testing"
)

func TestWorkflowRunStore_InsertAndGetByID(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowRunStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowRunStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	run, err := store.Insert(ctx, wfID, RunPending)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if run.ID == 0 {
		t.Fatalf("expected non-zero ID")
	}
	if run.WorkflowID != wfID {
		t.Fatalf("expected Workflow %d, got %d", wfID, run.WorkflowID)
	}
	if run.Status != RunPending {
		t.Fatalf("expected status RunPending, got %v", run.Status)
	}
	if run.FinishedAt != nil {
		t.Fatalf("expected FinishedAt to be nil")
	}

	got, err := store.GetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if got.ID != run.ID {
		t.Fatalf("expected ID %d, got %d", run.ID, got.ID)
	}
	if got.Status != RunPending {
		t.Fatalf("expected RunPending, got %v", got.Status)
	}
}

func TestWorkflowRunStore_GetLatestForWorkflow(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowRunStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowRunStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	run, err := store.Insert(ctx, wfID, RunPending)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetLatestForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetLatestForWorkflow failed: %v", err)
	}

	if got.ID != run.ID {
		t.Fatalf("expected ID %d, got %d", run.ID, got.ID)
	}
	if got.Status != RunPending {
		t.Fatalf("expected RunPending, got %v", got.Status)
	}
	if got.StartedAt != run.StartedAt {
		t.Fatalf("expected StartedAt %v, got %v", run.StartedAt, got.StartedAt)
	}
	if got.FinishedAt != nil {
		t.Fatalf("expected FinishedAt to be nil")
	}
}

func TestWorkflowRunStore_UpdateStatus(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowRunStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowRunStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	run, err := store.Insert(ctx, wfID, RunPending)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = store.UpdateStatus(ctx, run.ID, RunRunning, nil)
	if err != nil {
		t.Fatalf("UpdateStatus failed: %v", err)
	}
}

func TestWorkflowRunStore_GetRunsForWorkflow(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowRunStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowRunStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	// Insert 3 runs
	run1, _ := store.Insert(ctx, wfID, RunSuccess)
	run2, _ := store.Insert(ctx, wfID, RunFailed)
	run3, _ := store.Insert(ctx, wfID, RunPending)

	// Manually update timestamps to ensure distinct ordering (since SQLite DEFAULT CURRENT_TIMESTAMP is second precision)
	// run3 (newest) -> now
	// run2 -> now - 1h
	// run1 -> now - 2h
	db.Exec("UPDATE workflow_runs SET started_at = datetime('now', '-2 hours') WHERE id = ?", run1.ID)
	db.Exec("UPDATE workflow_runs SET started_at = datetime('now', '-1 hours') WHERE id = ?", run2.ID)
	// run3 stays at 'now'

	// Fetch all 3
	runs, err := store.GetRunsForWorkflow(ctx, wfID, 10)
	if err != nil {
		t.Fatalf("GetRunsForWorkflow failed: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
	if runs[0].ID != run3.ID {
		t.Errorf("expected newest run first (ID %d), got ID %d", run3.ID, runs[0].ID)
	}
	if runs[1].ID != run2.ID {
		t.Errorf("expected middle run second (ID %d), got ID %d", run2.ID, runs[1].ID)
	}
	if runs[2].ID != run1.ID {
		t.Errorf("expected oldest run last (ID %d), got ID %d", run1.ID, runs[2].ID)
	}

	// Fetch limit 1
	runs, err = store.GetRunsForWorkflow(ctx, wfID, 1)
	if err != nil {
		t.Fatalf("GetRunsForWorkflow failed: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	if runs[0].ID != run3.ID {
		t.Errorf("expected newest run (ID %d), got ID %d", run3.ID, runs[0].ID)
	}
}
