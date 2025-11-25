package models

import (
	"context"
	"testing"
)

func TestWorkflowTaskStore_InsertAndGetForWorkflow(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	task := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task",
		Script:     "echo hi",
	}

	err = store.Insert(ctx, task)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 task, got %d", len(got))
	}
	if got[0].ID != task.ID {
		t.Fatalf("expected ID %d, got %d", task.ID, got[0].ID)
	}
	if got[0].WorkflowID != wfID {
		t.Fatalf("expected WorkflowID %d, got %d", wfID, got[0].WorkflowID)
	}
	if got[0].Name != task.Name {
		t.Fatalf("expected Name %q, got %q", task.Name, got[0].Name)
	}
	if got[0].Script != task.Script {
		t.Fatalf("expected Script %q, got %q", task.Script, got[0].Script)
	}
	if got[0].Retries != task.Retries {
		t.Fatalf("expected Retries %d, got %d", task.Retries, got[0].Retries)
	}
	if got[0].RetryDelay != task.RetryDelay {
		t.Fatalf("expected RetryDelay %q, got %q", task.RetryDelay, got[0].RetryDelay)
	}
	if got[0].Timeout != task.Timeout {
		t.Fatalf("expected Timeout %q, got %q", task.Timeout, got[0].Timeout)
	}
	if got[0].Condition != task.Condition {
		t.Fatalf("expected Condition %q, got %q", task.Condition, got[0].Condition)
	}
}

func TestWorkflowTaskStore_Update(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	task := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task",
		Script:     "echo hi",
	}

	err = store.Insert(ctx, task)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = store.Update(ctx, task)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := store.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 task, got %d", len(got))
	}
	if got[0].ID != task.ID {
		t.Fatalf("expected ID %d, got %d", task.ID, got[0].ID)
	}
	if got[0].Name != task.Name {
		t.Fatalf("expected Name %q, got %q", task.Name, got[0].Name)
	}
	if got[0].Script != task.Script {
		t.Fatalf("expected Script %q, got %q", task.Script, got[0].Script)
	}
	if got[0].Retries != task.Retries {
		t.Fatalf("expected Retries %d, got %d", task.Retries, got[0].Retries)
	}
	if got[0].RetryDelay != task.RetryDelay {
		t.Fatalf("expected RetryDelay %q, got %q", task.RetryDelay, got[0].RetryDelay)
	}
}

func TestWorkflowTaskStore_Delete(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	task := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task",
		Script:     "echo hi",
	}

	err = store.Insert(ctx, task)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = store.Delete(ctx, task.ID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	got, err := store.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 tasks, got %d", len(got))
	}
}

func TestWorkflowTaskStore_DeleteForWorkflow(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	task1 := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task-1",
		Script:     "echo hi",
	}
	task2 := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task-2",
		Script:     "echo hello",
	}

	err = store.Insert(ctx, task1)
	if err != nil {
		t.Fatalf("Insert task1 failed: %v", err)
	}
	err = store.Insert(ctx, task2)
	if err != nil {
		t.Fatalf("Insert task2 failed: %v", err)
	}

	err = store.DeleteForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("DeleteForWorkflow failed: %v", err)
	}

	got, err := store.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 tasks, got %d", len(got))
	}
}

func TestWorkflowTaskStore_InsertTx(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	task := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task",
		Script:     "echo hi",
		Retries:    3,
		RetryDelay: "30s",
		Timeout:    "2m",
		Condition:  "all_upstream.success",
		Env: map[string]string{
			"TEST_VAR": "test-value",
		},
	}

	err = store.InsertTx(ctx, tx, task)
	if err != nil {
		t.Fatalf("InsertTx failed: %v", err)
	}

	if task.ID == 0 {
		t.Fatalf("expected ID to be set, got 0")
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	got, err := store.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 task, got %d", len(got))
	}
	if got[0].ID != task.ID {
		t.Fatalf("expected ID %d, got %d", task.ID, got[0].ID)
	}
	if got[0].Name != task.Name {
		t.Fatalf("expected Name %q, got %q", task.Name, got[0].Name)
	}
	if got[0].Retries != task.Retries {
		t.Fatalf("expected Retries %d, got %d", task.Retries, got[0].Retries)
	}
}

func TestWorkflowTaskStore_DeleteForWorkflowTx(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	task1 := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task-1",
		Script:     "echo hi",
	}
	task2 := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task-2",
		Script:     "echo hello",
	}

	err = store.Insert(ctx, task1)
	if err != nil {
		t.Fatalf("Insert task1 failed: %v", err)
	}
	err = store.Insert(ctx, task2)
	if err != nil {
		t.Fatalf("Insert task2 failed: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	err = store.DeleteForWorkflowTx(ctx, tx, wfID)
	if err != nil {
		t.Fatalf("DeleteForWorkflowTx failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	got, err := store.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected 0 tasks, got %d", len(got))
	}
}

func TestWorkflowTaskStore_GetByNameForWorkflow(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")

	task := &WorkflowTask{
		WorkflowID: wfID,
		Name:       "test-task",
		Script:     "echo hi",
		Retries:    3,
		RetryDelay: "30s",
		Timeout:    "2m",
		Condition:  "all_upstream.success",
	}

	err = store.Insert(ctx, task)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetByNameForWorkflow(ctx, wfID, "test-task")
	if err != nil {
		t.Fatalf("GetByNameForWorkflow failed: %v", err)
	}

	if got == nil {
		t.Fatalf("expected task, got nil")
	}
	if got.ID != task.ID {
		t.Fatalf("expected ID %d, got %d", task.ID, got.ID)
	}
	if got.WorkflowID != wfID {
		t.Fatalf("expected WorkflowID %d, got %d", wfID, got.WorkflowID)
	}
	if got.Name != "test-task" {
		t.Fatalf("expected Name %q, got %q", "test-task", got.Name)
	}
	if got.Script != task.Script {
		t.Fatalf("expected Script %q, got %q", task.Script, got.Script)
	}
	if got.Retries != task.Retries {
		t.Fatalf("expected Retries %d, got %d", task.Retries, got.Retries)
	}
	if got.Condition != task.Condition {
		t.Fatalf("expected Condition %q, got %q", task.Condition, got.Condition)
	}

	// Test non-existent task
	got, err = store.GetByNameForWorkflow(ctx, wfID, "non-existent")
	if err != nil {
		t.Fatalf("GetByNameForWorkflow failed: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for non-existent task, got %v", got)
	}
}
