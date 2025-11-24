package models

import (
	"context"
	"testing"
)

func TestWorkflowStore_InsertAndGetByName(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	wf := &Workflow{
		Name:     "test-workflow",
		Schedule: "* * * * *",
		Env: map[string]string{
			"TEST_VAR": "test-value",
		},
		Hash:    "test-hash",
		Enabled: true,
	}

	err = store.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetByName(ctx, "test-workflow")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}

	if got.ID != wf.ID {
		t.Fatalf("expected ID %d, got %d", wf.ID, got.ID)
	}
	if got.Name != wf.Name {
		t.Fatalf("expected Name %q, got %q", wf.Name, got.Name)
	}
	if got.Schedule != wf.Schedule {
		t.Fatalf("expected Schedule %q, got %q", wf.Schedule, got.Schedule)
	}
	if got.Env["TEST_VAR"] != wf.Env["TEST_VAR"] {
		t.Fatalf("expected Env %q, got %q", wf.Env["TEST_VAR"], got.Env["TEST_VAR"])
	}
	if got.Hash != wf.Hash {
		t.Fatalf("expected Hash %q, got %q", wf.Hash, got.Hash)
	}
	if got.Enabled != wf.Enabled {
		t.Fatalf("expected Enabled %t, got %t", wf.Enabled, got.Enabled)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatalf("expected UpdatedAt to be set")
	}
}

func TestWorkflowStore_GetByName_NotFound(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	got, err := store.GetByName(ctx, "nonexistent-workflow")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestWorkflowStore_Update(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	wf := &Workflow{
		Name:     "test-workflow",
		Schedule: "* * * * *",
		Env: map[string]string{
			"TEST_VAR": "test-value",
		},
		Hash:    "test-hash",
		Enabled: true,
	}

	err = store.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = store.Update(ctx, wf)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := store.GetByName(ctx, "test-workflow")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatalf("expected UpdatedAt to be set")
	}
}
