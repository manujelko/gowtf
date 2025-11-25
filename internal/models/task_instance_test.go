package models

import (
	"context"
	"testing"
)

func TestTaskInstanceStore_InsertAndGetForRun(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")
	wfRunID := InsertTestWorkflowRun(t, db, wfID)

	taskID := InsertTestTask(t, db, wfID, "test-task")

	taskInstance := &TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         TaskStatePending,
		Attempt:       1,
	}
	err = store.Insert(ctx, taskInstance)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 task instance, got %d", len(got))
	}
	if got[0].ID != taskInstance.ID {
		t.Fatalf("expected ID %d, got %d", taskInstance.ID, got[0].ID)
	}
	if got[0].WorkflowRunID != wfRunID {
		t.Fatalf("expected WorkflowRunID %d, got %d", wfRunID, got[0].WorkflowRunID)
	}
	if got[0].TaskID != taskInstance.TaskID {
		t.Fatalf("expected TaskID %d, got %d", taskInstance.TaskID, got[0].TaskID)
	}
	if got[0].State != TaskStatePending {
		t.Fatalf("expected State %v, got %v", TaskStatePending, got[0].State)
	}
	if got[0].Attempt != taskInstance.Attempt {
		t.Fatalf("expected Attempt %d, got %d", taskInstance.Attempt, got[0].Attempt)
	}
	if got[0].ExitCode != nil {
		t.Fatalf("expected ExitCode to be nil")
	}
	if got[0].StartedAt != nil {
		t.Fatalf("expected StartedAt to be nil")
	}
	if got[0].FinishedAt != nil {
		t.Fatalf("expected FinishedAt to be nil")
	}
	if got[0].StdoutPath != nil {
		t.Fatalf("expected StdoutPath to be nil")
	}
	if got[0].StderrPath != nil {
		t.Fatalf("expected StderrPath to be nil")
	}
}

func TestTaskInstanceStore_Update(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")
	wfRunID := InsertTestWorkflowRun(t, db, wfID)

	taskID := InsertTestTask(t, db, wfID, "test-task")

	taskInstance := &TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         TaskStatePending,
		Attempt:       1,
	}
	err = store.Insert(ctx, taskInstance)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = store.Update(ctx, taskInstance)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := store.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 task instance, got %d", len(got))
	}
	if got[0].ID != taskInstance.ID {
		t.Fatalf("expected ID %d, got %d", taskInstance.ID, got[0].ID)
	}
	if got[0].WorkflowRunID != wfRunID {
		t.Fatalf("expected WorkflowRunID %d, got %d", wfRunID, got[0].WorkflowRunID)
	}
	if got[0].TaskID != taskInstance.TaskID {
		t.Fatalf("expected TaskID %d, got %d", taskInstance.TaskID, got[0].TaskID)
	}
	if got[0].State != TaskStatePending {
		t.Fatalf("expected State %v, got %v", TaskStatePending, got[0].State)
	}
	if got[0].Attempt != taskInstance.Attempt {
		t.Fatalf("expected Attempt %d, got %d", taskInstance.Attempt, got[0].Attempt)
	}
	if got[0].ExitCode != nil {
		t.Fatalf("expected ExitCode to be nil")
	}
	if got[0].StartedAt != nil {
		t.Fatalf("expected StartedAt to be nil")
	}
	if got[0].FinishedAt != nil {
		t.Fatalf("expected FinishedAt to be nil")
	}
	if got[0].StdoutPath != nil {
		t.Fatalf("expected StdoutPath to be nil")
	}
	if got[0].StderrPath != nil {
		t.Fatalf("expected StderrPath to be nil")
	}
}

func TestTaskInstanceStore_GetByTaskIDAndRun(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := InsertTestWorkflow(t, db, "wf1")
	wfRunID := InsertTestWorkflowRun(t, db, wfID)
	taskID := InsertTestTask(t, db, wfID, "test-task")

	taskInstance := &TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         TaskStatePending,
		Attempt:       1,
	}
	err = store.Insert(ctx, taskInstance)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := store.GetByTaskIDAndRun(ctx, taskID, wfRunID)
	if err != nil {
		t.Fatalf("GetByTaskIDAndRun failed: %v", err)
	}

	if got == nil {
		t.Fatalf("expected task instance, got nil")
	}
	if got.ID != taskInstance.ID {
		t.Fatalf("expected ID %d, got %d", taskInstance.ID, got.ID)
	}
	if got.WorkflowRunID != wfRunID {
		t.Fatalf("expected WorkflowRunID %d, got %d", wfRunID, got.WorkflowRunID)
	}
	if got.TaskID != taskID {
		t.Fatalf("expected TaskID %d, got %d", taskID, got.TaskID)
	}
	if got.State != TaskStatePending {
		t.Fatalf("expected State %v, got %v", TaskStatePending, got.State)
	}

	// Test non-existent task instance
	got, err = store.GetByTaskIDAndRun(ctx, 999, wfRunID)
	if err != nil {
		t.Fatalf("GetByTaskIDAndRun failed: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for non-existent task instance, got %v", got)
	}
}
