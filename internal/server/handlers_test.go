package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/scheduler"
	"github.com/manujelko/gowtf/internal/watcher"
)

// Helper to setup server with test DB
func setupTestServer(t *testing.T) *Server {
	t.Helper()

	db := models.NewTestDB(t)
	srv, err := New(db, nil, nil, "./output", "", slog.Default())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return srv
}

func TestHandleHome(t *testing.T) {
	// Hack: We need to make sure we are in the project root for templates
	wd, _ := os.Getwd()
	if !strings.HasSuffix(wd, "gowtf") {
		// Attempt to move up to root
		// This is brittle but works for `go test ./internal/server/...`
		if _, err := os.Stat("../../ui/html"); err == nil {
			os.Chdir("../../")
			defer os.Chdir(wd)
		}
	}

	srv := setupTestServer(t)

	// Insert some data
	models.InsertTestWorkflow(t, srv.DB, "wf1")

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(w.Body.String(), "wf1") {
		t.Errorf("expected body to contain 'wf1'")
	}
}

func TestHandleTriggerWorkflow_Success(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan scheduler.WorkflowRunEvent, 10)

	// Create a real scheduler for testing
	sched, err := scheduler.NewScheduler(db, watcherEvents, notifyCh, slog.Default())
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	srv, err := New(db, nil, sched, "./output", "", slog.Default())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create workflow with tasks
	wfID := models.InsertTestWorkflow(t, db, "test-workflow")
	taskID := models.InsertTestTask(t, db, wfID, "test-task")

	// Trigger workflow
	req := httptest.NewRequest("POST", "/api/workflow/"+strconv.Itoa(wfID)+"/trigger", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	// Check response body
	var response map[string]int
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["workflow_run_id"] == 0 {
		t.Errorf("expected workflow_run_id in response, got %v", response)
	}

	// Verify workflow run was created
	ctx := context.Background()
	workflowRunStore, _ := models.NewWorkflowRunStore(db)
	run, err := workflowRunStore.GetByID(ctx, response["workflow_run_id"])
	if err != nil {
		t.Fatalf("failed to get workflow run: %v", err)
	}
	if run == nil {
		t.Fatal("expected workflow run to be created")
	}
	if run.WorkflowID != wfID {
		t.Errorf("expected workflow_id %d, got %d", wfID, run.WorkflowID)
	}

	// Verify task instances were created
	taskInstanceStore, _ := models.NewTaskInstanceStore(db)
	instances, err := taskInstanceStore.GetForRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("failed to get task instances: %v", err)
	}
	if len(instances) != 1 {
		t.Errorf("expected 1 task instance, got %d", len(instances))
	}
	if instances[0].TaskID != taskID {
		t.Errorf("expected task_id %d, got %d", taskID, instances[0].TaskID)
	}

	// Verify notification was sent (with timeout)
	select {
	case event := <-notifyCh:
		if event.WorkflowRunID != run.ID {
			t.Errorf("expected workflow_run_id %d, got %d", run.ID, event.WorkflowRunID)
		}
		if event.WorkflowID != wfID {
			t.Errorf("expected workflow_id %d, got %d", wfID, event.WorkflowID)
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for notification")
	}
}

func TestHandleTriggerWorkflow_NotFound(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan scheduler.WorkflowRunEvent, 10)

	sched, err := scheduler.NewScheduler(db, watcherEvents, notifyCh, slog.Default())
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	srv, err := New(db, nil, sched, "./output", "", slog.Default())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Trigger non-existent workflow
	req := httptest.NewRequest("POST", "/api/workflow/999/trigger", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d. Body: %s", w.Code, w.Body.String())
	}
}

func TestHandleTriggerWorkflow_Disabled(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan scheduler.WorkflowRunEvent, 10)

	sched, err := scheduler.NewScheduler(db, watcherEvents, notifyCh, slog.Default())
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	srv, err := New(db, nil, sched, "./output", "", slog.Default())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create disabled workflow
	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)
	wf := &models.Workflow{
		Name:     "disabled-workflow",
		Schedule: "* * * * *",
		Hash:     "hash1",
		Enabled:  false,
	}
	if err := workflowStore.Insert(ctx, wf); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Trigger disabled workflow
	req := httptest.NewRequest("POST", "/api/workflow/"+strconv.Itoa(wf.ID)+"/trigger", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d. Body: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(w.Body.String(), "disabled") {
		t.Errorf("expected error message about workflow being disabled, got: %s", w.Body.String())
	}
}

func TestHandleTriggerWorkflow_InvalidID(t *testing.T) {
	srv := setupTestServer(t)

	// Test invalid ID format
	req := httptest.NewRequest("POST", "/api/workflow/invalid/trigger", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d. Body: %s", w.Code, w.Body.String())
	}
}

func TestHandleTriggerWorkflow_WrongMethod(t *testing.T) {
	srv := setupTestServer(t)

	// Test GET instead of POST
	req := httptest.NewRequest("GET", "/api/workflow/1/trigger", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d. Body: %s", w.Code, w.Body.String())
	}
}
