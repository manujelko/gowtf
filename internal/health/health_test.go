package health

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/manujelko/gowtf/internal/models"
)

func TestHealthMonitor_RegisterUnregisterTask(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	config := DefaultConfig()
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	taskInstanceID := 123
	cancelCalled := false
	cancelFunc := func() {
		cancelCalled = true
	}

	// Register task
	hm.RegisterTask(taskInstanceID, cancelFunc)

	// Verify task is registered
	hm.mu.RLock()
	_, exists := hm.activeTasks[taskInstanceID]
	hm.mu.RUnlock()

	if !exists {
		t.Fatal("Task should be registered")
	}

	// Unregister task
	hm.UnregisterTask(taskInstanceID)

	// Verify task is unregistered
	hm.mu.RLock()
	_, exists = hm.activeTasks[taskInstanceID]
	hm.mu.RUnlock()

	if exists {
		t.Fatal("Task should be unregistered")
	}

	if cancelCalled {
		t.Fatal("Cancel should not be called on unregister")
	}
}

func TestHealthMonitor_HandleHeartbeat(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	config := DefaultConfig()
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	taskInstanceID := 123
	cancelFunc := func() {}

	hm.RegisterTask(taskInstanceID, cancelFunc)

	// Send a heartbeat
	heartbeatTime := time.Now()
	msg := HeartbeatMessage{
		TaskInstanceID: taskInstanceID,
		Timestamp:      heartbeatTime,
	}

	hm.handleHeartbeat(msg)

	// Verify heartbeat time was updated
	hm.mu.RLock()
	task, exists := hm.activeTasks[taskInstanceID]
	hm.mu.RUnlock()

	if !exists {
		t.Fatal("Task should still be registered")
	}

	if task.lastHeartbeat.Before(heartbeatTime) || task.lastHeartbeat.After(heartbeatTime.Add(100*time.Millisecond)) {
		t.Fatalf("Heartbeat time should be updated. Expected around %v, got %v", heartbeatTime, task.lastHeartbeat)
	}
}

func TestHealthMonitor_StaleHeartbeatDetection(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	// Create workflow and task instance for testing
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)
	taskID := models.InsertTestTask(t, db, wfID, "test-task")

	taskInstance := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}
	err = taskInstanceStore.Insert(context.Background(), taskInstance)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Use very short intervals for testing
	config := Config{
		HeartbeatInterval: 100 * time.Millisecond,
		TimeoutThreshold:  300 * time.Millisecond, // 3x heartbeat interval
		CheckInterval:     100 * time.Millisecond,
	}
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	taskInstanceID := taskInstance.ID
	cancelCalled := make(chan struct{}, 1)
	cancelFunc := func() {
		select {
		case cancelCalled <- struct{}{}:
		default:
		}
	}

	// Register task with old heartbeat time
	hm.mu.Lock()
	hm.activeTasks[taskInstanceID] = &activeTask{
		cancelFunc:     cancelFunc,
		lastHeartbeat:  time.Now().Add(-500 * time.Millisecond), // Very old heartbeat
		taskInstanceID: taskInstanceID,
	}
	hm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	hm.Start(ctx)

	// Wait for stale check to run and verify context was cancelled
	select {
	case <-cancelCalled:
		// Cancel was called - good!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cancel function should have been called")
	}

	// Wait a bit for database update to complete
	time.Sleep(100 * time.Millisecond)

	// Verify task is unregistered
	hm.mu.RLock()
	_, exists := hm.activeTasks[taskInstanceID]
	hm.mu.RUnlock()

	if exists {
		t.Fatal("Task should be unregistered after stale detection")
	}

	// Verify task was marked as failed in database
	updated, err := taskInstanceStore.GetByID(context.Background(), taskInstanceID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if updated == nil {
		t.Fatal("Task instance should exist")
	}

	if updated.State != models.TaskStateFailed {
		t.Fatalf("Task should be marked as failed, got state: %v", updated.State)
	}

	if updated.ExitCode == nil || *updated.ExitCode != 124 {
		t.Fatalf("Task should have exit code 124, got: %v", updated.ExitCode)
	}

	if updated.FinishedAt == nil {
		t.Fatal("Task should have finished_at set")
	}

	hm.Stop()
}

func TestHealthMonitor_StaleHeartbeatNonRunningTask(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	// Create workflow and task instance for testing
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)
	taskID := models.InsertTestTask(t, db, wfID, "test-task")

	taskInstance := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         models.TaskStateSuccess, // Already completed
		Attempt:       1,
	}
	err = taskInstanceStore.Insert(context.Background(), taskInstance)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	config := Config{
		HeartbeatInterval: 100 * time.Millisecond,
		TimeoutThreshold:  300 * time.Millisecond,
		CheckInterval:     100 * time.Millisecond,
	}
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	taskInstanceID := taskInstance.ID
	cancelCalled := make(chan struct{}, 1)
	cancelFunc := func() {
		select {
		case cancelCalled <- struct{}{}:
		default:
		}
	}

	// Register task with old heartbeat time
	hm.mu.Lock()
	hm.activeTasks[taskInstanceID] = &activeTask{
		cancelFunc:     cancelFunc,
		lastHeartbeat:  time.Now().Add(-500 * time.Millisecond),
		taskInstanceID: taskInstanceID,
	}
	hm.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	hm.Start(ctx)

	// Wait for stale check to run and verify context was cancelled
	select {
	case <-cancelCalled:
		// Cancel was called - good!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cancel function should have been called")
	}

	// Verify task state was NOT changed (already in terminal state)
	updated, err := taskInstanceStore.GetByID(context.Background(), taskInstanceID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if updated.State != models.TaskStateSuccess {
		t.Fatalf("Task should remain in success state, got: %v", updated.State)
	}

	hm.Stop()
}

func TestHealthMonitor_MonitorLoop(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	config := Config{
		HeartbeatInterval: 50 * time.Millisecond,
		TimeoutThreshold:  150 * time.Millisecond,
		CheckInterval:     50 * time.Millisecond,
	}
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	taskInstanceID := 123
	cancelCalled := make(chan struct{}, 1)
	cancelFunc := func() {
		select {
		case cancelCalled <- struct{}{}:
		default:
		}
	}

	hm.RegisterTask(taskInstanceID, cancelFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hm.Start(ctx)

	// Send heartbeats for a while
	heartbeatCh := hm.HeartbeatChannel()
	for i := 0; i < 5; i++ {
		heartbeatCh <- HeartbeatMessage{
			TaskInstanceID: taskInstanceID,
			Timestamp:      time.Now(),
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Stop sending heartbeats and wait for stale detection
	select {
	case <-cancelCalled:
		// Cancel was called - good!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cancel function should have been called for stale heartbeat")
	}

	hm.Stop()
}

func TestHealthMonitor_GracefulShutdown(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	config := DefaultConfig()
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hm.Start(ctx)

	// Stop should complete without blocking
	done := make(chan struct{})
	go func() {
		hm.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Stop() should complete within reasonable time")
	}
}

func TestHealthMonitor_MultipleTasks(t *testing.T) {
	db := models.NewTestDB(t)
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	config := Config{
		HeartbeatInterval: 50 * time.Millisecond,
		TimeoutThreshold:  150 * time.Millisecond,
		CheckInterval:     50 * time.Millisecond,
	}
	hm := NewHealthMonitor(taskInstanceStore, config, slog.Default())

	// Register multiple tasks
	cancelCalls := make(map[int]chan struct{})
	for i := 1; i <= 5; i++ {
		taskID := i
		cancelCalled := make(chan struct{}, 1)
		cancelCalls[taskID] = cancelCalled
		cancelFunc := func() {
			select {
			case cancelCalled <- struct{}{}:
			default:
			}
		}
		hm.RegisterTask(taskID, cancelFunc)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	hm.Start(ctx)

	// Send heartbeats for task 1 only continuously
	heartbeatCh := hm.HeartbeatChannel()
	stopHeartbeats := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				heartbeatCh <- HeartbeatMessage{
					TaskInstanceID: 1,
					Timestamp:      time.Now(),
				}
			case <-stopHeartbeats:
				return
			}
		}
	}()

	// Wait for stale detection on tasks 2-5
	time.Sleep(200 * time.Millisecond)

	// Stop sending heartbeats for task 1
	close(stopHeartbeats)

	// Task 1 should still be active (receiving heartbeats)
	// Tasks 2-5 should be cancelled (no heartbeats)

	hm.mu.RLock()
	task1Exists := hm.activeTasks[1] != nil
	hm.mu.RUnlock()

	if !task1Exists {
		t.Error("Task 1 should still be active (receiving heartbeats)")
	}

	// Verify tasks 2-5 were cancelled
	for i := 2; i <= 5; i++ {
		select {
		case <-cancelCalls[i]:
			// Cancel was called - good!
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Task %d should have been cancelled", i)
		}
	}

	hm.Stop()
}
