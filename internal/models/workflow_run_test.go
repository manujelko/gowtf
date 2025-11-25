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
