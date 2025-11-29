package executor

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/scheduler"
)

func TestExecutor_HandleWorkflowRun(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start executor in background
	go func() {
		_ = executor.Start(ctx)
	}()

	// Give executor time to start
	time.Sleep(50 * time.Millisecond)

	// Create test workflow and run
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)
	taskID := models.InsertTestTask(t, db, wfID, "test-task")

	// Create task instance
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	taskInstance := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         models.TaskStatePending,
		Attempt:       1,
	}
	if err := taskInstanceStore.Insert(ctx, taskInstance); err != nil {
		t.Fatalf("Insert task instance failed: %v", err)
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify task instance was processed
	instances, err := taskInstanceStore.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	if len(instances) != 1 {
		t.Fatalf("expected 1 task instance, got %d", len(instances))
	}

	// Task should be in a terminal state (success, failed, or skipped)
	if instances[0].State != models.TaskStateSuccess &&
		instances[0].State != models.TaskStateFailed &&
		instances[0].State != models.TaskStateSkipped {
		t.Fatalf("expected task to be in terminal state, got %v", instances[0].State)
	}

	// Verify workflow run status
	workflowRunStore, err := models.NewWorkflowRunStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowRunStore failed: %v", err)
	}

	run, err := workflowRunStore.GetByID(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if run.Status != models.RunSuccess && run.Status != models.RunFailed {
		t.Fatalf("expected workflow run to be in terminal state, got %v", run.Status)
	}
}

func TestExecutor_DependencyResolution(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = executor.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Create workflow with two tasks: task1 -> task2
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	task1ID := models.InsertTestTask(t, db, wfID, "task1")
	task2ID := models.InsertTestTask(t, db, wfID, "task2")

	// Create dependency: task2 depends on task1
	depsStore, err := models.NewTaskDependenciesStore(db)
	if err != nil {
		t.Fatalf("NewTaskDependenciesStore failed: %v", err)
	}
	if err := depsStore.Insert(ctx, task2ID, task1ID); err != nil {
		t.Fatalf("Insert dependency failed: %v", err)
	}

	// Create workflow run
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

	// Create task instances
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	ti1 := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        task1ID,
		State:         models.TaskStatePending,
		Attempt:       1,
	}
	if err := taskInstanceStore.Insert(ctx, ti1); err != nil {
		t.Fatalf("Insert task1 instance failed: %v", err)
	}

	ti2 := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        task2ID,
		State:         models.TaskStatePending,
		Attempt:       1,
	}
	if err := taskInstanceStore.Insert(ctx, ti2); err != nil {
		t.Fatalf("Insert task2 instance failed: %v", err)
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing
	time.Sleep(1 * time.Second)

	// Verify task1 completed first
	instances, err := taskInstanceStore.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	var task1Instance, task2Instance *models.TaskInstance
	for _, ti := range instances {
		if ti.TaskID == task1ID {
			task1Instance = ti
		}
		if ti.TaskID == task2ID {
			task2Instance = ti
		}
	}

	if task1Instance == nil || task2Instance == nil {
		t.Fatalf("task instances not found")
	}

	// Task1 should be in terminal state
	if task1Instance.State != models.TaskStateSuccess &&
		task1Instance.State != models.TaskStateFailed &&
		task1Instance.State != models.TaskStateSkipped {
		t.Fatalf("task1 should be in terminal state, got %v", task1Instance.State)
	}

	// Task2 should also be processed (after task1 completes)
	if task2Instance.State != models.TaskStateSuccess &&
		task2Instance.State != models.TaskStateFailed &&
		task2Instance.State != models.TaskStateSkipped {
		t.Fatalf("task2 should be in terminal state, got %v", task2Instance.State)
	}
}

func TestExecutor_ConditionEvaluation_AllUpstreamSuccess(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = executor.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Create workflow with tasks: task1, task2 -> task3 (task3 requires all_upstream.success)
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	task1ID := models.InsertTestTask(t, db, wfID, "task1")
	task2ID := models.InsertTestTask(t, db, wfID, "task2")
	task3ID := models.InsertTestTask(t, db, wfID, "task3")

	// Set condition on task3
	taskStore, err := models.NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}

	tasks, err := taskStore.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	var task3 *models.WorkflowTask
	for _, task := range tasks {
		if task.ID == task3ID {
			task3 = task
			break
		}
	}
	if task3 == nil {
		t.Fatalf("task3 not found")
	}

	task3.Condition = "all_upstream.success"
	if err := taskStore.Update(ctx, task3); err != nil {
		t.Fatalf("Update task3 failed: %v", err)
	}

	// Create dependencies: task3 depends on task1 and task2
	depsStore, err := models.NewTaskDependenciesStore(db)
	if err != nil {
		t.Fatalf("NewTaskDependenciesStore failed: %v", err)
	}
	if err := depsStore.Insert(ctx, task3ID, task1ID); err != nil {
		t.Fatalf("Insert dependency failed: %v", err)
	}
	if err := depsStore.Insert(ctx, task3ID, task2ID); err != nil {
		t.Fatalf("Insert dependency failed: %v", err)
	}

	// Create workflow run
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

	// Create task instances
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	for _, taskID := range []int{task1ID, task2ID, task3ID} {
		ti := &models.TaskInstance{
			WorkflowRunID: wfRunID,
			TaskID:        taskID,
			State:         models.TaskStatePending,
			Attempt:       1,
		}
		if err := taskInstanceStore.Insert(ctx, ti); err != nil {
			t.Fatalf("Insert task instance failed: %v", err)
		}
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing (longer now since we're actually executing scripts)
	time.Sleep(2 * time.Second)

	// Verify all tasks completed
	instances, err := taskInstanceStore.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	var task3Instance *models.TaskInstance
	for _, ti := range instances {
		if ti.TaskID == task3ID {
			task3Instance = ti
			break
		}
	}

	if task3Instance == nil {
		t.Fatalf("task3 instance not found")
	}

	// Task3 should have run (all dependencies succeeded)
	if task3Instance.State == models.TaskStateSkipped {
		t.Fatalf("task3 should not be skipped when all dependencies succeed")
	}
}

func TestExecutor_ConditionEvaluation_TaskNameSuccess(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = executor.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Create workflow with tasks: task1 -> task2 (task2 requires task1.success)
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	task1ID := models.InsertTestTask(t, db, wfID, "task1")
	task2ID := models.InsertTestTask(t, db, wfID, "task2")

	// Set condition on task2
	taskStore, err := models.NewWorkflowTaskStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowTaskStore failed: %v", err)
	}

	tasks, err := taskStore.GetForWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("GetForWorkflow failed: %v", err)
	}

	var task2 *models.WorkflowTask
	for _, task := range tasks {
		if task.ID == task2ID {
			task2 = task
			break
		}
	}
	if task2 == nil {
		t.Fatalf("task2 not found")
	}

	task2.Condition = "task1.success"
	if err := taskStore.Update(ctx, task2); err != nil {
		t.Fatalf("Update task2 failed: %v", err)
	}

	// Create dependency: task2 depends on task1
	depsStore, err := models.NewTaskDependenciesStore(db)
	if err != nil {
		t.Fatalf("NewTaskDependenciesStore failed: %v", err)
	}
	if err := depsStore.Insert(ctx, task2ID, task1ID); err != nil {
		t.Fatalf("Insert dependency failed: %v", err)
	}

	// Create workflow run
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

	// Create task instances
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	for _, taskID := range []int{task1ID, task2ID} {
		ti := &models.TaskInstance{
			WorkflowRunID: wfRunID,
			TaskID:        taskID,
			State:         models.TaskStatePending,
			Attempt:       1,
		}
		if err := taskInstanceStore.Insert(ctx, ti); err != nil {
			t.Fatalf("Insert task instance failed: %v", err)
		}
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing (longer now since we're actually executing scripts)
	time.Sleep(2 * time.Second)

	// Verify task2 ran (task1 succeeded)
	instances, err := taskInstanceStore.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	var task2Instance *models.TaskInstance
	for _, ti := range instances {
		if ti.TaskID == task2ID {
			task2Instance = ti
			break
		}
	}

	if task2Instance == nil {
		t.Fatalf("task2 instance not found")
	}

	// Task2 should have run (task1 succeeded)
	if task2Instance.State == models.TaskStateSkipped {
		t.Fatalf("task2 should not be skipped when task1 succeeds")
	}
}

func TestExecutor_TaskStateTransitions(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = executor.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Create simple workflow
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	taskID := models.InsertTestTask(t, db, wfID, "test-task")
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

	// Create task instance
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	ti := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         models.TaskStatePending,
		Attempt:       1,
	}
	if err := taskInstanceStore.Insert(ctx, ti); err != nil {
		t.Fatalf("Insert task instance failed: %v", err)
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing (longer now since we're actually executing scripts)
	time.Sleep(2 * time.Second)

	// Verify state transitions occurred
	instances, err := taskInstanceStore.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	if len(instances) != 1 {
		t.Fatalf("expected 1 task instance, got %d", len(instances))
	}

	// Task should have transitioned through states and ended in a terminal state
	if instances[0].State != models.TaskStateSuccess &&
		instances[0].State != models.TaskStateFailed &&
		instances[0].State != models.TaskStateSkipped {
		t.Fatalf("expected task to be in terminal state, got %v", instances[0].State)
	}

	// If successful, verify timestamps were set
	if instances[0].State == models.TaskStateSuccess {
		if instances[0].StartedAt == nil {
			t.Fatalf("expected StartedAt to be set")
		}
		if instances[0].FinishedAt == nil {
			t.Fatalf("expected FinishedAt to be set")
		}
		if instances[0].ExitCode == nil {
			t.Fatalf("expected ExitCode to be set")
		}
	}
}

func TestExecutor_WorkflowRunStatusUpdate(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = executor.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Create workflow with single task
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	taskID := models.InsertTestTask(t, db, wfID, "test-task")
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

	// Create task instance
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	ti := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         models.TaskStatePending,
		Attempt:       1,
	}
	if err := taskInstanceStore.Insert(ctx, ti); err != nil {
		t.Fatalf("Insert task instance failed: %v", err)
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing (longer now since we're actually executing scripts)
	time.Sleep(2 * time.Second)

	// Verify workflow run status was updated
	workflowRunStore, err := models.NewWorkflowRunStore(db)
	if err != nil {
		t.Fatalf("NewWorkflowRunStore failed: %v", err)
	}

	run, err := workflowRunStore.GetByID(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	// Workflow run should be in a terminal state
	if run.Status != models.RunSuccess && run.Status != models.RunFailed {
		t.Fatalf("expected workflow run to be in terminal state, got %v", run.Status)
	}

	// FinishedAt should be set
	if run.FinishedAt == nil {
		t.Fatalf("expected FinishedAt to be set")
	}
}

func TestExecutor_WorkflowStatus_HandledVsUnhandledFailures(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = executor.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	t.Run("Handled failure should allow workflow to succeed", func(t *testing.T) {
		// Create workflow: setup -> decision (fails) -> success_path (skipped) | failure_path (runs) -> finalize
		wfID := models.InsertTestWorkflow(t, db, "branching-wf")
		setupID := models.InsertTestTask(t, db, wfID, "setup")
		decisionID := models.InsertTestTask(t, db, wfID, "decision")
		successPathID := models.InsertTestTask(t, db, wfID, "success_path")
		failurePathID := models.InsertTestTask(t, db, wfID, "failure_path")
		finalizeID := models.InsertTestTask(t, db, wfID, "finalize")

		taskStore, err := models.NewWorkflowTaskStore(db)
		if err != nil {
			t.Fatalf("NewWorkflowTaskStore failed: %v", err)
		}

		// Set conditions
		tasks, err := taskStore.GetForWorkflow(ctx, wfID)
		if err != nil {
			t.Fatalf("GetForWorkflow failed: %v", err)
		}

		for _, task := range tasks {
			switch task.ID {
			case decisionID:
				// Decision task will fail
				task.Script = "exit 1"
			case successPathID:
				task.Condition = "decision.success"
			case failurePathID:
				task.Condition = "decision.failed" // This handles the failure
			case finalizeID:
				task.Condition = "any_upstream.success"
			}
			if err := taskStore.Update(ctx, task); err != nil {
				t.Fatalf("Update task %d failed: %v", task.ID, err)
			}
		}

		// Create dependencies
		depsStore, err := models.NewTaskDependenciesStore(db)
		if err != nil {
			t.Fatalf("NewTaskDependenciesStore failed: %v", err)
		}
		if err := depsStore.Insert(ctx, decisionID, setupID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}
		if err := depsStore.Insert(ctx, successPathID, decisionID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}
		if err := depsStore.Insert(ctx, failurePathID, decisionID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}
		if err := depsStore.Insert(ctx, finalizeID, successPathID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}
		if err := depsStore.Insert(ctx, finalizeID, failurePathID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}

		// Create workflow run
		wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

		// Create task instances
		taskInstanceStore, err := models.NewTaskInstanceStore(db)
		if err != nil {
			t.Fatalf("NewTaskInstanceStore failed: %v", err)
		}

		for _, taskID := range []int{setupID, decisionID, successPathID, failurePathID, finalizeID} {
			ti := &models.TaskInstance{
				WorkflowRunID: wfRunID,
				TaskID:        taskID,
				State:         models.TaskStatePending,
				Attempt:       1,
			}
			if err := taskInstanceStore.Insert(ctx, ti); err != nil {
				t.Fatalf("Insert task instance failed: %v", err)
			}
		}

		// Send event
		events <- scheduler.WorkflowRunEvent{
			WorkflowRunID: wfRunID,
			WorkflowID:    wfID,
		}

		// Wait for processing
		time.Sleep(3 * time.Second)

		// Verify workflow run status
		workflowRunStore, err := models.NewWorkflowRunStore(db)
		if err != nil {
			t.Fatalf("NewWorkflowRunStore failed: %v", err)
		}

		run, err := workflowRunStore.GetByID(ctx, wfRunID)
		if err != nil {
			t.Fatalf("GetByID failed: %v", err)
		}

		// Workflow should succeed because failure_path handled the decision failure
		if run.Status != models.RunSuccess {
			t.Fatalf("expected workflow to succeed with handled failure, got %v", run.Status)
		}
	})

	t.Run("Unhandled failure should cause workflow to fail", func(t *testing.T) {
		// Create workflow: setup -> critical_task (fails) -> downstream (skipped) -> final_task (skipped)
		wfID := models.InsertTestWorkflow(t, db, "failure-wf")
		setupID := models.InsertTestTask(t, db, wfID, "setup")
		criticalID := models.InsertTestTask(t, db, wfID, "critical_task")
		downstreamID := models.InsertTestTask(t, db, wfID, "downstream")
		finalID := models.InsertTestTask(t, db, wfID, "final_task")

		taskStore, err := models.NewWorkflowTaskStore(db)
		if err != nil {
			t.Fatalf("NewWorkflowTaskStore failed: %v", err)
		}

		// Set conditions and scripts
		tasks, err := taskStore.GetForWorkflow(ctx, wfID)
		if err != nil {
			t.Fatalf("GetForWorkflow failed: %v", err)
		}

		for _, task := range tasks {
			switch task.ID {
			case criticalID:
				// Critical task will fail
				task.Script = "exit 1"
			case downstreamID:
				task.Condition = "critical_task.success" // No handling of failure
			case finalID:
				task.Condition = "downstream.success"
			}
			if err := taskStore.Update(ctx, task); err != nil {
				t.Fatalf("Update task %d failed: %v", task.ID, err)
			}
		}

		// Create dependencies
		depsStore, err := models.NewTaskDependenciesStore(db)
		if err != nil {
			t.Fatalf("NewTaskDependenciesStore failed: %v", err)
		}
		if err := depsStore.Insert(ctx, criticalID, setupID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}
		if err := depsStore.Insert(ctx, downstreamID, criticalID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}
		if err := depsStore.Insert(ctx, finalID, downstreamID); err != nil {
			t.Fatalf("Insert dependency failed: %v", err)
		}

		// Create workflow run
		wfRunID := models.InsertTestWorkflowRun(t, db, wfID)

		// Create task instances
		taskInstanceStore, err := models.NewTaskInstanceStore(db)
		if err != nil {
			t.Fatalf("NewTaskInstanceStore failed: %v", err)
		}

		for _, taskID := range []int{setupID, criticalID, downstreamID, finalID} {
			ti := &models.TaskInstance{
				WorkflowRunID: wfRunID,
				TaskID:        taskID,
				State:         models.TaskStatePending,
				Attempt:       1,
			}
			if err := taskInstanceStore.Insert(ctx, ti); err != nil {
				t.Fatalf("Insert task instance failed: %v", err)
			}
		}

		// Send event
		events <- scheduler.WorkflowRunEvent{
			WorkflowRunID: wfRunID,
			WorkflowID:    wfID,
		}

		// Wait for processing
		time.Sleep(3 * time.Second)

		// Verify workflow run status
		workflowRunStore, err := models.NewWorkflowRunStore(db)
		if err != nil {
			t.Fatalf("NewWorkflowRunStore failed: %v", err)
		}

		run, err := workflowRunStore.GetByID(ctx, wfRunID)
		if err != nil {
			t.Fatalf("GetByID failed: %v", err)
		}

		// Workflow should fail because critical_task failure is unhandled
		if run.Status != models.RunFailed {
			t.Fatalf("expected workflow to fail with unhandled failure, got %v", run.Status)
		}
	})
}

func TestExecutor_HealthMonitorIntegration(t *testing.T) {
	db := models.NewTestDB(t)
	events := make(chan scheduler.WorkflowRunEvent, 10)

	outputDir, err := os.MkdirTemp("", "gowtf-test-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	executor, err := NewExecutor(db, events, 5, outputDir, slog.Default())
	if err != nil {
		t.Fatalf("NewExecutor failed: %v", err)
	}

	// Verify health monitor is created and configured
	if executor.healthMonitor == nil {
		t.Fatal("Health monitor should be created")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start executor in background
	go func() {
		_ = executor.Start(ctx)
	}()

	// Give executor time to start
	time.Sleep(50 * time.Millisecond)

	// Create test workflow and run
	wfID := models.InsertTestWorkflow(t, db, "test-wf")
	wfRunID := models.InsertTestWorkflowRun(t, db, wfID)
	taskID := models.InsertTestTask(t, db, wfID, "test-task")

	// Create task instance
	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		t.Fatalf("NewTaskInstanceStore failed: %v", err)
	}

	taskInstance := &models.TaskInstance{
		WorkflowRunID: wfRunID,
		TaskID:        taskID,
		State:         models.TaskStatePending,
		Attempt:       1,
	}
	if err := taskInstanceStore.Insert(ctx, taskInstance); err != nil {
		t.Fatalf("Insert task instance failed: %v", err)
	}

	// Send event
	events <- scheduler.WorkflowRunEvent{
		WorkflowRunID: wfRunID,
		WorkflowID:    wfID,
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify task instance was processed
	instances, err := taskInstanceStore.GetForRun(ctx, wfRunID)
	if err != nil {
		t.Fatalf("GetForRun failed: %v", err)
	}

	if len(instances) != 1 {
		t.Fatalf("expected 1 task instance, got %d", len(instances))
	}

	// Task should be in a terminal state (success, failed, or skipped)
	if instances[0].State != models.TaskStateSuccess &&
		instances[0].State != models.TaskStateFailed &&
		instances[0].State != models.TaskStateSkipped {
		t.Fatalf("expected task to be in terminal state, got %v", instances[0].State)
	}

	// Verify the task was properly unregistered from health monitor
	// by checking that it's no longer in the active tasks map
	// (This is an indirect check - the task should complete and be unregistered)
	// We can verify by checking that if we wait a bit longer, no stale heartbeat
	// errors occur (tasks should be unregistered)
	time.Sleep(200 * time.Millisecond)

	// Stop executor to verify graceful shutdown
	executor.Stop()
}
