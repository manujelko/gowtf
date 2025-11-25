package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/watcher"
)

func TestScheduler_NewScheduler(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	if s == nil {
		t.Fatal("expected scheduler, got nil")
	}
	if s.workflowStore == nil {
		t.Fatal("expected workflowStore, got nil")
	}
	if s.workflowRunStore == nil {
		t.Fatal("expected workflowRunStore, got nil")
	}
	if s.taskStore == nil {
		t.Fatal("expected taskStore, got nil")
	}
	if s.taskInstanceStore == nil {
		t.Fatal("expected taskInstanceStore, got nil")
	}
}

func TestScheduler_Start_LoadsEnabledWorkflows(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)

	// Create enabled workflow
	wf1 := &models.Workflow{
		Name:     "enabled-workflow",
		Schedule: "0 2 * * *", // Daily at 2 AM
		Hash:     "hash1",
		Enabled:  true,
	}
	err = workflowStore.Insert(ctx, wf1)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Create disabled workflow
	wf2 := &models.Workflow{
		Name:     "disabled-workflow",
		Schedule: "0 3 * * *",
		Hash:     "hash2",
		Enabled:  false,
	}
	err = workflowStore.Insert(ctx, wf2)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Start scheduler in background
	startCtx, cancel := context.WithCancel(context.Background())
	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- s.Start(startCtx)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Check that only enabled workflow is scheduled
	s.entryMapMu.RLock()
	enabledCount := len(s.entryMap)
	s.entryMapMu.RUnlock()

	if enabledCount != 1 {
		t.Fatalf("expected 1 scheduled workflow, got %d", enabledCount)
	}

	// Check that enabled workflow is in the map
	s.entryMapMu.RLock()
	_, exists := s.entryMap[wf1.ID]
	s.entryMapMu.RUnlock()

	if !exists {
		t.Fatal("expected enabled workflow to be scheduled")
	}

	// Cleanup
	cancel()
	s.Stop()
}

func TestScheduler_OnScheduleFire_CreatesRunAndTasks(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)
	taskStore, _ := models.NewWorkflowTaskStore(db)

	// Create workflow with tasks
	wf := &models.Workflow{
		Name:     "test-workflow",
		Schedule: "0 2 * * *",
		Hash:     "hash1",
		Enabled:  true,
	}
	err = workflowStore.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Add tasks
	task1 := &models.WorkflowTask{
		WorkflowID: wf.ID,
		Name:       "task1",
		Script:     "echo hello",
	}
	err = taskStore.Insert(ctx, task1)
	if err != nil {
		t.Fatalf("Insert task failed: %v", err)
	}

	task2 := &models.WorkflowTask{
		WorkflowID: wf.ID,
		Name:       "task2",
		Script:     "echo world",
	}
	err = taskStore.Insert(ctx, task2)
	if err != nil {
		t.Fatalf("Insert task failed: %v", err)
	}

	// Manually trigger onScheduleFire
	s.onScheduleFire(wf.ID)

	// Wait for notification
	select {
	case event := <-notifyCh:
		if event.WorkflowID != wf.ID {
			t.Fatalf("expected workflow ID %d, got %d", wf.ID, event.WorkflowID)
		}
		if event.WorkflowRunID == 0 {
			t.Fatal("expected workflow run ID to be set")
		}

		// Verify workflow run was created
		workflowRunStore, _ := models.NewWorkflowRunStore(db)
		run, err := workflowRunStore.GetByID(ctx, event.WorkflowRunID)
		if err != nil {
			t.Fatalf("GetByID failed: %v", err)
		}
		if run == nil {
			t.Fatal("expected workflow run to exist")
		}
		if run.WorkflowID != wf.ID {
			t.Fatalf("expected workflow ID %d, got %d", wf.ID, run.WorkflowID)
		}
		if run.Status != models.RunPending {
			t.Fatalf("expected status RunPending, got %v", run.Status)
		}

		// Verify task instances were created
		taskInstanceStore, _ := models.NewTaskInstanceStore(db)
		taskInstances, err := taskInstanceStore.GetForRun(ctx, event.WorkflowRunID)
		if err != nil {
			t.Fatalf("GetForRun failed: %v", err)
		}
		if len(taskInstances) != 2 {
			t.Fatalf("expected 2 task instances, got %d", len(taskInstances))
		}

		for _, ti := range taskInstances {
			if ti.State != models.TaskStatePending {
				t.Fatalf("expected task state TaskStatePending, got %v", ti.State)
			}
			if ti.Attempt != 1 {
				t.Fatalf("expected attempt 1, got %d", ti.Attempt)
			}
		}

	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for notification")
	}
}

func TestScheduler_HandleWorkflowEvent_Add(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)

	// Create enabled workflow
	wf := &models.Workflow{
		Name:     "new-workflow",
		Schedule: "0 2 * * *",
		Hash:     "hash1",
		Enabled:  true,
	}
	err = workflowStore.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Send add event
	event := watcher.WorkflowEvent{
		Type:         watcher.EventAdded,
		WorkflowName: wf.Name,
		WorkflowID:   wf.ID,
	}

	err = s.handleWorkflowEvent(ctx, event)
	if err != nil {
		t.Fatalf("handleWorkflowEvent failed: %v", err)
	}

	// Verify workflow is scheduled
	s.entryMapMu.RLock()
	_, exists := s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if !exists {
		t.Fatal("expected workflow to be scheduled")
	}

	s.Stop()
}

func TestScheduler_HandleWorkflowEvent_Add_Disabled(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)

	// Create disabled workflow
	wf := &models.Workflow{
		Name:     "disabled-workflow",
		Schedule: "0 2 * * *",
		Hash:     "hash1",
		Enabled:  false,
	}
	err = workflowStore.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Send add event
	event := watcher.WorkflowEvent{
		Type:         watcher.EventAdded,
		WorkflowName: wf.Name,
		WorkflowID:   wf.ID,
	}

	err = s.handleWorkflowEvent(ctx, event)
	if err != nil {
		t.Fatalf("handleWorkflowEvent failed: %v", err)
	}

	// Verify workflow is NOT scheduled
	s.entryMapMu.RLock()
	_, exists := s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if exists {
		t.Fatal("expected disabled workflow to NOT be scheduled")
	}

	s.Stop()
}

func TestScheduler_HandleWorkflowEvent_Update(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)

	// Create workflow
	wf := &models.Workflow{
		Name:     "test-workflow",
		Schedule: "0 2 * * *",
		Hash:     "hash1",
		Enabled:  true,
	}
	err = workflowStore.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Schedule workflow
	err = s.scheduleWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("scheduleWorkflow failed: %v", err)
	}

	// Update workflow schedule
	wf.Schedule = "0 3 * * *"
	wf.Hash = "hash2"
	err = workflowStore.Update(ctx, wf)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Send update event
	event := watcher.WorkflowEvent{
		Type:         watcher.EventUpdated,
		WorkflowName: wf.Name,
		WorkflowID:   wf.ID,
	}

	err = s.handleWorkflowEvent(ctx, event)
	if err != nil {
		t.Fatalf("handleWorkflowEvent failed: %v", err)
	}

	// Verify workflow is still scheduled (with new schedule)
	s.entryMapMu.RLock()
	_, exists := s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if !exists {
		t.Fatal("expected workflow to still be scheduled after update")
	}

	s.Stop()
}

func TestScheduler_HandleWorkflowEvent_Delete(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)

	// Create workflow
	wf := &models.Workflow{
		Name:     "test-workflow",
		Schedule: "0 2 * * *",
		Hash:     "hash1",
		Enabled:  true,
	}
	err = workflowStore.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Schedule workflow
	err = s.scheduleWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("scheduleWorkflow failed: %v", err)
	}

	// Verify it's scheduled
	s.entryMapMu.RLock()
	_, exists := s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if !exists {
		t.Fatal("expected workflow to be scheduled")
	}

	// Send delete event
	event := watcher.WorkflowEvent{
		Type:         watcher.EventDeleted,
		WorkflowName: wf.Name,
		WorkflowID:   wf.ID,
	}

	err = s.handleWorkflowEvent(ctx, event)
	if err != nil {
		t.Fatalf("handleWorkflowEvent failed: %v", err)
	}

	// Verify workflow is unscheduled
	s.entryMapMu.RLock()
	_, exists = s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if exists {
		t.Fatal("expected workflow to be unscheduled after delete")
	}

	s.Stop()
}

func TestScheduler_UnscheduleWorkflow(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()
	workflowStore, _ := models.NewWorkflowStore(db)

	// Create workflow
	wf := &models.Workflow{
		Name:     "test-workflow",
		Schedule: "0 2 * * *",
		Hash:     "hash1",
		Enabled:  true,
	}
	err = workflowStore.Insert(ctx, wf)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Schedule workflow
	err = s.scheduleWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("scheduleWorkflow failed: %v", err)
	}

	// Verify it's scheduled
	s.entryMapMu.RLock()
	_, exists := s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if !exists {
		t.Fatal("expected workflow to be scheduled")
	}

	// Unschedule
	s.unscheduleWorkflow(wf.ID)

	// Verify it's unscheduled
	s.entryMapMu.RLock()
	_, exists = s.entryMap[wf.ID]
	s.entryMapMu.RUnlock()

	if exists {
		t.Fatal("expected workflow to be unscheduled")
	}

	s.Stop()
}

func TestScheduler_ScheduleWorkflow_InvalidSchedule(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx := context.Background()

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Create workflow with invalid schedule
	wf := &models.Workflow{
		Name:     "test-workflow",
		Schedule: "invalid-cron",
		Hash:     "hash1",
		Enabled:  true,
	}

	// Schedule should fail
	err = s.scheduleWorkflow(ctx, wf)
	if err == nil {
		t.Fatal("expected scheduleWorkflow to fail with invalid schedule")
	}

	s.Stop()
}

func TestScheduler_Stop(t *testing.T) {
	db := models.NewTestDB(t)
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	notifyCh := make(chan WorkflowRunEvent, 10)

	s, err := NewScheduler(db, watcherEvents, notifyCh)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	// Initialize cron
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))
	s.cron.Start()

	// Stop should not panic
	s.Stop()

	// Stop again should be safe
	s.Stop()
}
