package models

import (
	"context"
	"testing"
)

func TestTaskDependenciesStore_InsertAndGetForTask(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewTaskDependenciesStore(db)
	if err != nil {
		t.Fatalf("NewTaskDependenciesStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := insertTestWorkflow(t, db, "wf1")
	taskID := insertTestTask(t, db, wfID, "test-task")
	dependsOnID := insertTestTask(t, db, wfID, "test-task-2")

	err = store.Insert(ctx, taskID, dependsOnID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	got, err := store.GetForTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetForTask failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(got))
	}
	if got[0] != dependsOnID {
		t.Fatalf("expected dependency %d, got %d", dependsOnID, got[0])
	}
}

func TestTaskDependenciesStore_Delete(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewTaskDependenciesStore(db)
	if err != nil {
		t.Fatalf("NewTaskDependenciesStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := insertTestWorkflow(t, db, "wf1")
	taskID := insertTestTask(t, db, wfID, "test-task")
	dependsOnID := insertTestTask(t, db, wfID, "test-task-2")

	err = store.Insert(ctx, taskID, dependsOnID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	err = store.Delete(ctx, taskID, dependsOnID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	got, err := store.GetForTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetForTask failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 dependencies, got %d", len(got))
	}
}

func TestTaskDependenciesStore_DeleteForWorkflow(t *testing.T) {
	db := NewTestDB(t)
	store, err := NewTaskDependenciesStore(db)
	if err != nil {
		t.Fatalf("NewTaskDependenciesStore failed: %v", err)
	}
	ctx := context.Background()

	wfID := insertTestWorkflow(t, db, "wf1")
	taskID := insertTestTask(t, db, wfID, "test-task")
	dependsOnID := insertTestTask(t, db, wfID, "test-task-2")
	err = store.Insert(ctx, taskID, dependsOnID)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	err = store.DeleteForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("DeleteForWorkflow failed: %v", err)
	}
	got, err := store.GetForTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetForTask failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 dependencies, got %d", len(got))
	}
}
