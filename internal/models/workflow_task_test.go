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

	wfID := insertTestWorkflow(t, db, "wf1")

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

	wfID := insertTestWorkflow(t, db, "wf1")

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

	wfID := insertTestWorkflow(t, db, "wf1")

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
