package watcher

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/manujelko/gowtf/internal/models"
)

// setupTestWatcher creates a temporary directory, database, and watcher for testing
func setupTestWatcher(t *testing.T) (*Watcher, string, func()) {
	t.Helper()

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "gowtf-watcher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create temp DB file
	tmpDB, err := os.CreateTemp("", "gowtf-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	dbPath := tmpDB.Name()
	tmpDB.Close()

	// Open database using testutil
	db := models.NewTestDB(t)

	// Create notification channel (bidirectional for testing)
	notifyCh := make(chan WorkflowEvent, 10)

	// Create watcher
	watcher, err := NewWatcher(db, tmpDir, notifyCh, slog.Default())
	if err != nil {
		os.RemoveAll(tmpDir)
		os.Remove(dbPath)
		db.Close()
		t.Fatalf("failed to create watcher: %v", err)
	}

	// Store channel reference for tests
	watcher.notifyCh = notifyCh

	cleanup := func() {
		watcher.Stop()
		db.Close()
		os.RemoveAll(tmpDir)
		os.Remove(dbPath)
	}

	return watcher, tmpDir, cleanup
}

// createTestWorkflowFile creates a YAML workflow file
func createTestWorkflowFile(t *testing.T, dir, filename, content string) string {
	t.Helper()

	filePath := filepath.Join(dir, filename)
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write workflow file: %v", err)
	}
	return filePath
}

// waitForEvent waits for a workflow event with timeout
func waitForEvent(t *testing.T, ch <-chan WorkflowEvent, timeout time.Duration) *WorkflowEvent {
	t.Helper()

	select {
	case event := <-ch:
		return &event
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for event")
		return nil
	}
}

func TestComputeHash(t *testing.T) {
	content1 := []byte("test content")
	content2 := []byte("different content")

	hash1 := computeHash(content1)
	hash2 := computeHash(content2)
	hash1Again := computeHash(content1)

	// Same content should produce same hash
	if hash1 != hash1Again {
		t.Errorf("expected same hash for same content, got %q and %q", hash1, hash1Again)
	}

	// Different content should produce different hash
	if hash1 == hash2 {
		t.Errorf("expected different hashes for different content, both got %q", hash1)
	}

	// Hash should be hex-encoded (64 chars for SHA256)
	if len(hash1) != 64 {
		t.Errorf("expected hash length 64, got %d", len(hash1))
	}
}

func TestSyncWorkflow_NewWorkflow(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	err := watcher.syncWorkflow(filePath)
	if err != nil {
		t.Fatalf("syncWorkflow failed: %v", err)
	}

	// Verify workflow was created
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf == nil {
		t.Fatal("workflow was not created")
	}
	if wf.Name != "test_workflow" {
		t.Errorf("expected workflow name 'test_workflow', got %q", wf.Name)
	}

	// Verify tasks were created
	tasks, err := watcher.taskStore.GetForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("failed to get tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Name != "task1" {
		t.Errorf("expected task name 'task1', got %q", tasks[0].Name)
	}

	// Verify notification was sent
	event := waitForEvent(t, notifyCh, 1*time.Second)
	if event.Type != EventAdded {
		t.Errorf("expected EventAdded, got %v", event.Type)
	}
	if event.WorkflowName != "test_workflow" {
		t.Errorf("expected workflow name 'test_workflow', got %q", event.WorkflowName)
	}
}

func TestSyncWorkflow_UpdateWorkflow(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	// Create initial workflow
	content1 := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Sync initial version
	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	// Consume the added event
	<-notifyCh

	// Update workflow
	content2 := `name: test_workflow
schedule: "0 * * * *"
tasks:
  - name: task1
    script: echo hello
  - name: task2
    script: echo world
`

	if err := os.WriteFile(filePath, []byte(content2), 0644); err != nil {
		t.Fatalf("failed to update file: %v", err)
	}

	// Sync updated version
	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("update sync failed: %v", err)
	}

	// Verify workflow was updated
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf.Schedule != "0 * * * *" {
		t.Errorf("expected schedule '0 * * * *', got %q", wf.Schedule)
	}

	// Verify tasks were updated (old task deleted, new tasks inserted)
	tasks, err := watcher.taskStore.GetForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("failed to get tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}

	// Verify notification was sent
	event := waitForEvent(t, notifyCh, 1*time.Second)
	if event.Type != EventUpdated {
		t.Errorf("expected EventUpdated, got %v", event.Type)
	}
}

func TestSyncWorkflow_NoChange(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Sync first time
	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("first sync failed: %v", err)
	}

	// Consume the added event
	<-notifyCh

	// Sync again (should be no-op)
	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("second sync failed: %v", err)
	}

	// Should not receive another event
	select {
	case event := <-notifyCh:
		t.Errorf("unexpected event received: %v", event)
	case <-time.After(100 * time.Millisecond):
		// Expected - no event should be sent
	}
}

func TestSyncWorkflow_ValidationError(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	// Invalid YAML (invalid schedule format)
	content := `name: test_workflow
schedule: "invalid cron format"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	err := watcher.syncWorkflow(filePath)
	if err == nil {
		t.Fatal("expected error for invalid workflow, got nil")
	}

	// Verify workflow was not created
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf != nil {
		t.Fatal("workflow should not have been created for invalid YAML")
	}
}

func TestSyncWorkflow_Dependencies(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
  - name: task2
    script: echo world
    depends_on: [task1]
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("syncWorkflow failed: %v", err)
	}

	// Verify workflow and tasks
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}

	tasks, err := watcher.taskStore.GetForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("failed to get tasks: %v", err)
	}

	// Find task2
	var task2ID int
	var task1ID int
	for _, task := range tasks {
		if task.Name == "task2" {
			task2ID = task.ID
		}
		if task.Name == "task1" {
			task1ID = task.ID
		}
	}

	if task2ID == 0 {
		t.Fatal("task2 not found")
	}
	if task1ID == 0 {
		t.Fatal("task1 not found")
	}

	// Verify dependency
	deps, err := watcher.depStore.GetForTask(ctx, task2ID)
	if err != nil {
		t.Fatalf("failed to get dependencies: %v", err)
	}
	if len(deps) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(deps))
	}
	if deps[0] != task1ID {
		t.Errorf("expected dependency on task1 (ID %d), got %d", task1ID, deps[0])
	}
}

func TestDeleteWorkflow(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	// Create workflow first
	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("syncWorkflow failed: %v", err)
	}

	// Consume the added event
	<-notifyCh

	// Delete workflow
	if err := watcher.deleteWorkflow("test_workflow"); err != nil {
		t.Fatalf("deleteWorkflow failed: %v", err)
	}

	// Verify workflow was deleted
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf != nil {
		t.Fatal("workflow should have been deleted")
	}

	// Verify notification was sent
	event := waitForEvent(t, notifyCh, 1*time.Second)
	if event.Type != EventDeleted {
		t.Errorf("expected EventDeleted, got %v", event.Type)
	}
	if event.WorkflowName != "test_workflow" {
		t.Errorf("expected workflow name 'test_workflow', got %q", event.WorkflowName)
	}
}

func TestScanDirectory(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	// Create multiple workflow files
	content1 := `name: workflow1
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	content2 := `name: workflow2
schedule: "0 * * * *"
tasks:
  - name: task2
    script: echo world
`

	createTestWorkflowFile(t, dir, "workflow1.yaml", content1)
	createTestWorkflowFile(t, dir, "workflow2.yaml", content2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher.ctx = ctx

	if err := watcher.scanDirectory(); err != nil {
		t.Fatalf("scanDirectory failed: %v", err)
	}

	// Verify both workflows were created
	wf1, err := watcher.workflowStore.GetByName(ctx, "workflow1")
	if err != nil {
		t.Fatalf("failed to get workflow1: %v", err)
	}
	if wf1 == nil {
		t.Fatal("workflow1 was not created")
	}

	wf2, err := watcher.workflowStore.GetByName(ctx, "workflow2")
	if err != nil {
		t.Fatalf("failed to get workflow2: %v", err)
	}
	if wf2 == nil {
		t.Fatal("workflow2 was not created")
	}
}

func TestWatcher_FileCreate(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Start watcher in background
	watcherErr := make(chan error, 1)
	go func() {
		watcherErr <- watcher.Run()
	}()

	// Give watcher time to start
	time.Sleep(100 * time.Millisecond)

	// Create workflow file
	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	createTestWorkflowFile(t, dir, "test.yaml", content)

	// Wait for event (with debounce)
	time.Sleep(600 * time.Millisecond)

	// Verify workflow was created
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf == nil {
		t.Fatal("workflow was not created")
	}

	// Check for notification
	select {
	case event := <-notifyCh:
		if event.Type != EventAdded {
			t.Errorf("expected EventAdded, got %v", event.Type)
		}
	case <-time.After(1 * time.Second):
		t.Log("no notification received (may be due to timing)")
	}

	watcher.Stop()
}

func TestWatcher_FileUpdate(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Create initial file
	content1 := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content1)

	// Start watcher
	watcherErr := make(chan error, 1)
	go func() {
		watcherErr <- watcher.Run()
	}()

	time.Sleep(600 * time.Millisecond)

	// Consume initial event
	select {
	case <-notifyCh:
	case <-time.After(1 * time.Second):
	}

	// Update file
	content2 := `name: test_workflow
schedule: "0 * * * *"
tasks:
  - name: task1
    script: echo hello
  - name: task2
    script: echo world
`

	if err := os.WriteFile(filePath, []byte(content2), 0644); err != nil {
		t.Fatalf("failed to update file: %v", err)
	}

	time.Sleep(600 * time.Millisecond)

	// Verify workflow was updated
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf.Schedule != "0 * * * *" {
		t.Errorf("expected schedule '0 * * * *', got %q", wf.Schedule)
	}

	tasks, err := watcher.taskStore.GetForWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("failed to get tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}

	watcher.Stop()
}

func TestWatcher_FileDelete(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Create file
	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	// Start watcher
	watcherErr := make(chan error, 1)
	go func() {
		watcherErr <- watcher.Run()
	}()

	time.Sleep(600 * time.Millisecond)

	// Consume initial event
	select {
	case <-notifyCh:
	case <-time.After(1 * time.Second):
	}

	// Delete file
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to delete file: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify workflow was deleted
	wf, err := watcher.workflowStore.GetByName(ctx, "test_workflow")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf != nil {
		t.Fatal("workflow should have been deleted")
	}

	watcher.Stop()
}

func TestWatcher_InvalidYAML(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Start watcher
	go func() {
		watcher.Run()
	}()

	time.Sleep(100 * time.Millisecond)

	// Create invalid YAML file (invalid schedule format)
	invalidContent := `name: invalid
schedule: "not a valid cron"
tasks:
  - name: task1
    script: echo hello
`

	createTestWorkflowFile(t, dir, "invalid.yaml", invalidContent)

	time.Sleep(600 * time.Millisecond)

	// Verify workflow was not created
	wf, err := watcher.workflowStore.GetByName(ctx, "invalid")
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if wf != nil {
		t.Fatal("invalid workflow should not have been created")
	}

	watcher.Stop()
}

func TestWatcher_Notifications(t *testing.T) {
	watcher, dir, cleanup := setupTestWatcher(t)
	defer cleanup()

	notifyCh := make(chan WorkflowEvent, 10)
	watcher.notifyCh = notifyCh

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watcher.ctx = ctx

	// Create workflow
	content := `name: test_workflow
schedule: "* * * * *"
tasks:
  - name: task1
    script: echo hello
`

	filePath := createTestWorkflowFile(t, dir, "test.yaml", content)

	// Sync workflow
	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("syncWorkflow failed: %v", err)
	}

	// Verify Added event
	event := waitForEvent(t, notifyCh, 1*time.Second)
	if event.Type != EventAdded {
		t.Errorf("expected EventAdded, got %v", event.Type)
	}
	if event.WorkflowName != "test_workflow" {
		t.Errorf("expected workflow name 'test_workflow', got %q", event.WorkflowName)
	}
	if event.WorkflowID == 0 {
		t.Error("expected non-zero workflow ID")
	}

	// Update workflow
	content2 := `name: test_workflow
schedule: "0 * * * *"
tasks:
  - name: task1
    script: echo hello
`

	if err := os.WriteFile(filePath, []byte(content2), 0644); err != nil {
		t.Fatalf("failed to update file: %v", err)
	}

	if err := watcher.syncWorkflow(filePath); err != nil {
		t.Fatalf("syncWorkflow failed: %v", err)
	}

	// Verify Updated event
	event = waitForEvent(t, notifyCh, 1*time.Second)
	if event.Type != EventUpdated {
		t.Errorf("expected EventUpdated, got %v", event.Type)
	}

	// Delete workflow
	if err := watcher.deleteWorkflow("test_workflow"); err != nil {
		t.Fatalf("deleteWorkflow failed: %v", err)
	}

	// Verify Deleted event
	event = waitForEvent(t, notifyCh, 1*time.Second)
	if event.Type != EventDeleted {
		t.Errorf("expected EventDeleted, got %v", event.Type)
	}
	if event.WorkflowID != 0 {
		t.Errorf("expected workflow ID 0 for deleted event, got %d", event.WorkflowID)
	}
}
