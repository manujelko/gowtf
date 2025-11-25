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

func TestWorkflowStore_Delete(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	wf := &Workflow{
		Name:     "test-workflow",
		Schedule: "* * * * *",
		Hash:     "test-hash",
		Enabled:  true,
	}

	err = store.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = store.Delete(ctx, wf.ID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	got, err := store.GetByName(ctx, "test-workflow")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if got != nil {
		t.Fatalf("expected workflow to be deleted, got %v", got)
	}
}

func TestWorkflowStore_InsertTx(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	wf := &Workflow{
		Name:     "test-workflow",
		Schedule: "* * * * *",
		Env: map[string]string{
			"TEST_VAR": "test-value",
		},
		Hash:    "test-hash",
		Enabled: true,
	}

	err = store.InsertTx(ctx, tx, wf)
	if err != nil {
		t.Fatalf("InsertTx failed: %v", err)
	}

	if wf.ID == 0 {
		t.Fatalf("expected ID to be set, got 0")
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	got, err := store.GetByName(ctx, "test-workflow")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if got == nil {
		t.Fatalf("expected workflow to exist")
	}
	if got.ID != wf.ID {
		t.Fatalf("expected ID %d, got %d", wf.ID, got.ID)
	}
}

func TestWorkflowStore_UpdateTx(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	wf := &Workflow{
		Name:     "test-workflow",
		Schedule: "* * * * *",
		Hash:     "test-hash",
		Enabled:  true,
	}

	err = store.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	wf.Schedule = "0 * * * *"
	wf.Hash = "updated-hash"

	err = store.UpdateTx(ctx, tx, wf)
	if err != nil {
		t.Fatalf("UpdateTx failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	got, err := store.GetByName(ctx, "test-workflow")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if got.Schedule != "0 * * * *" {
		t.Fatalf("expected Schedule %q, got %q", "0 * * * *", got.Schedule)
	}
	if got.Hash != "updated-hash" {
		t.Fatalf("expected Hash %q, got %q", "updated-hash", got.Hash)
	}
}

func TestWorkflowStore_GetByID(t *testing.T) {
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

	got, err := store.GetByID(ctx, wf.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if got == nil {
		t.Fatalf("expected workflow, got nil")
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
}

func TestWorkflowStore_GetByID_NotFound(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	got, err := store.GetByID(ctx, 999)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestWorkflowStore_GetAllEnabled(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	// Insert enabled workflow
	wf1 := &Workflow{
		Name:     "enabled-workflow",
		Schedule: "* * * * *",
		Hash:     "hash1",
		Enabled:  true,
	}
	err = store.Insert(ctx, wf1)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Insert disabled workflow
	wf2 := &Workflow{
		Name:     "disabled-workflow",
		Schedule: "* * * * *",
		Hash:     "hash2",
		Enabled:  false,
	}
	err = store.Insert(ctx, wf2)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Insert another enabled workflow
	wf3 := &Workflow{
		Name:     "another-enabled",
		Schedule: "0 * * * *",
		Hash:     "hash3",
		Enabled:  true,
	}
	err = store.Insert(ctx, wf3)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetAllEnabled(ctx)
	if err != nil {
		t.Fatalf("GetAllEnabled failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 enabled workflows, got %d", len(got))
	}

	// Check that only enabled workflows are returned
	names := make(map[string]bool)
	for _, w := range got {
		names[w.Name] = true
		if !w.Enabled {
			t.Fatalf("expected all workflows to be enabled, got %q with Enabled=false", w.Name)
		}
	}

	if !names["enabled-workflow"] {
		t.Fatalf("expected enabled-workflow to be in results")
	}
	if !names["another-enabled"] {
		t.Fatalf("expected another-enabled to be in results")
	}
	if names["disabled-workflow"] {
		t.Fatalf("expected disabled-workflow to NOT be in results")
	}
}

func TestWorkflowStore_GetAllEnabled_Empty(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowStore failed: %v", err)
	}
	ctx := context.Background()

	got, err := store.GetAllEnabled(ctx)
	if err != nil {
		t.Fatalf("GetAllEnabled failed: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 workflows, got %d", len(got))
	}
}
