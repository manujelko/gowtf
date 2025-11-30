package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/manujelko/gowtf/internal/health"
	"github.com/manujelko/gowtf/internal/models"
)

// testLogger returns a logger for testing
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError, // Only log errors in tests
	}))
}

func TestNewWorkerPool(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(5, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	if pool == nil {
		t.Fatal("NewWorkerPool returned nil")
	}

	if pool.size != 5 {
		t.Errorf("Expected pool size 5, got %d", pool.size)
	}

	if pool.outputDir != outputDir {
		t.Errorf("Expected output dir %s, got %s", outputDir, pool.outputDir)
	}
}

func TestNewWorkerPool_InvalidSize(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	_, err = NewWorkerPool(0, outputDir, testLogger(t))
	if err == nil {
		t.Fatal("Expected error for size 0")
	}

	_, err = NewWorkerPool(-1, outputDir, testLogger(t))
	if err == nil {
		t.Fatal("Expected error for negative size")
	}
}

func TestNewWorkerPool_EmptyOutputDir(t *testing.T) {
	_, err := NewWorkerPool(5, "", testLogger(t))
	if err == nil {
		t.Fatal("Expected error for empty output dir")
	}
}

func TestWorkerPool_StartStop(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(3, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	pool.Stop()

	// Try to start again - should fail
	if err := pool.Start(ctx); err == nil {
		t.Fatal("Expected error when starting already started pool")
	}
}

func TestWorkerPool_ExecuteTask(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	// Create a test task
	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo 'Hello, World!'",
		Env:    make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Wait for result
	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Fatalf("Task execution failed: %v", result.Error)
		}

		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}

		if result.TaskInstanceID != taskInstance.ID {
			t.Errorf("Expected task instance ID %d, got %d", taskInstance.ID, result.TaskInstanceID)
		}

		if result.WorkflowRunID != taskInstance.WorkflowRunID {
			t.Errorf("Expected workflow run ID %d, got %d", taskInstance.WorkflowRunID, result.WorkflowRunID)
		}

		// Verify output files exist
		if result.StdoutPath == "" {
			t.Error("Expected stdout path to be set")
		} else {
			if _, err := os.Stat(result.StdoutPath); os.IsNotExist(err) {
				t.Errorf("Stdout file does not exist: %s", result.StdoutPath)
			}
		}

		if result.StderrPath == "" {
			t.Error("Expected stderr path to be set")
		} else {
			if _, err := os.Stat(result.StderrPath); os.IsNotExist(err) {
				t.Errorf("Stderr file does not exist: %s", result.StderrPath)
			}
		}

		// Verify stdout content
		stdoutContent, err := os.ReadFile(result.StdoutPath)
		if err != nil {
			t.Fatalf("Failed to read stdout file: %v", err)
		}

		expectedOutput := "Hello, World!\n"
		if string(stdoutContent) != expectedOutput {
			t.Errorf("Expected stdout %q, got %q", expectedOutput, string(stdoutContent))
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_ExecuteTaskWithFailure(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	taskInstance := &models.TaskInstance{
		ID:            2,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "exit 42", // Exit with code 42
		Env:    make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		if result.ExitCode != 42 {
			t.Errorf("Expected exit code 42, got %d", result.ExitCode)
		}

		if result.Error != nil {
			// Error is expected for non-zero exit codes
			t.Logf("Task error (expected): %v", result.Error)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_ExecuteTaskWithTimeout(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	taskInstance := &models.TaskInstance{
		ID:            3,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:      10,
		Name:    "test-task",
		Script:  "sleep 10", // Sleep for 10 seconds
		Timeout: "100ms",    // But timeout after 100ms
		Env:     make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		if result.Error == nil {
			t.Fatal("Expected error for timeout")
		}

		if result.ExitCode != 124 {
			t.Errorf("Expected timeout exit code 124, got %d", result.ExitCode)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_ConcurrentExecution(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(3, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	// Submit multiple tasks
	numTasks := 5
	for i := 0; i < numTasks; i++ {
		taskInstance := &models.TaskInstance{
			ID:            i + 1,
			WorkflowRunID: 100,
			TaskID:        10,
			State:         models.TaskStateRunning,
			Attempt:       1,
		}

		task := &models.WorkflowTask{
			ID:     10,
			Name:   fmt.Sprintf("task-%d", i),
			Script: "echo 'Task " + string(rune('A'+i)) + "'",
			Env:    make(map[string]string),
		}

		job := TaskJob{
			WorkflowEnv:  make(map[string]string),
			TaskInstance: taskInstance,
			Task:         task,
			Context:      ctx,
		}

		if err := pool.Submit(job); err != nil {
			t.Fatalf("Submit failed for task %d: %v", i, err)
		}
	}

	// Collect all results
	results := make([]TaskResult, 0, numTasks)
	timeout := time.After(10 * time.Second)

	for i := 0; i < numTasks; i++ {
		select {
		case result := <-pool.Results():
			results = append(results, result)
		case <-timeout:
			t.Fatalf("Timeout waiting for result %d", i)
		}
	}

	if len(results) != numTasks {
		t.Errorf("Expected %d results, got %d", numTasks, len(results))
	}

	// Verify all tasks completed successfully
	for _, result := range results {
		if result.Error != nil {
			t.Errorf("Task %d failed: %v", result.TaskInstanceID, result.Error)
		}
		if result.ExitCode != 0 {
			t.Errorf("Task %d exited with code %d", result.TaskInstanceID, result.ExitCode)
		}
	}
}

func TestWorkerPool_EnvironmentVariables(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo $TEST_VAR",
		Env: map[string]string{
			"TEST_VAR": "Hello from env",
		},
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Fatalf("Task execution failed: %v", result.Error)
		}

		stdoutContent, err := os.ReadFile(result.StdoutPath)
		if err != nil {
			t.Fatalf("Failed to read stdout file: %v", err)
		}

		expectedOutput := "Hello from env\n"
		if string(stdoutContent) != expectedOutput {
			t.Errorf("Expected stdout %q, got %q", expectedOutput, string(stdoutContent))
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_EnvironmentVariableResolution(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	// Set environment variable for resolution
	os.Setenv("TEST_ENV_VAR", "resolved-value")
	defer os.Unsetenv("TEST_ENV_VAR")

	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo $RESOLVED_VAR",
		Env: map[string]string{
			"RESOLVED_VAR": "$TEST_ENV_VAR",
		},
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Fatalf("Task execution failed: %v", result.Error)
		}

		stdoutContent, err := os.ReadFile(result.StdoutPath)
		if err != nil {
			t.Fatalf("Failed to read stdout file: %v", err)
		}

		expectedOutput := "resolved-value\n"
		if string(stdoutContent) != expectedOutput {
			t.Errorf("Expected stdout %q, got %q", expectedOutput, string(stdoutContent))
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_EnvironmentVariableWithDefault(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	// Don't set the environment variable - should use default
	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo $VAR_WITH_DEFAULT",
		Env: map[string]string{
			"VAR_WITH_DEFAULT": "$MISSING_VAR:default-value",
		},
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Fatalf("Task execution failed: %v", result.Error)
		}

		stdoutContent, err := os.ReadFile(result.StdoutPath)
		if err != nil {
			t.Fatalf("Failed to read stdout file: %v", err)
		}

		expectedOutput := "default-value\n"
		if string(stdoutContent) != expectedOutput {
			t.Errorf("Expected stdout %q, got %q", expectedOutput, string(stdoutContent))
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_OutputFileStructure(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	workflowRunID := 123
	taskInstanceID := 456
	workflowName := "test-workflow"
	runStartTime := time.Date(2025, 11, 26, 13, 11, 23, 0, time.UTC)
	taskName := "test-task"

	taskInstance := &models.TaskInstance{
		ID:            taskInstanceID,
		WorkflowRunID: workflowRunID,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   taskName,
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: workflowName,
		RunStartedAt: runStartTime,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		// Verify file paths follow expected structure: workflow-name/YYYY-MM-DD/HH-MM-SS/task-name/stdout.log
		expectedStdoutPath := filepath.Join(outputDir, workflowName, "2025-11-26", "13-11-23", taskName, "stdout.log")
		expectedStderrPath := filepath.Join(outputDir, workflowName, "2025-11-26", "13-11-23", taskName, "stderr.log")

		if result.StdoutPath != expectedStdoutPath {
			t.Errorf("Expected stdout path %s, got %s", expectedStdoutPath, result.StdoutPath)
		}

		if result.StderrPath != expectedStderrPath {
			t.Errorf("Expected stderr path %s, got %s", expectedStderrPath, result.StderrPath)
		}

		// Verify directory was created
		taskDir := filepath.Join(outputDir, workflowName, "2025-11-26", "13-11-23", taskName)
		if _, err := os.Stat(taskDir); os.IsNotExist(err) {
			t.Errorf("Task directory does not exist: %s", taskDir)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

func TestWorkerPool_SubmitBeforeStart(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx := context.Background()

	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	err = pool.Submit(job)
	if err == nil {
		t.Fatal("Expected error when submitting before start")
	}
}

func TestWorkerPool_SubmitAfterStop(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	pool.Stop()

	// Wait a bit for shutdown
	time.Sleep(10 * time.Millisecond)

	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	// Submit should fail after stop
	err = pool.Submit(job)
	if err == nil {
		t.Error("Expected error when submitting after stop, got nil")
	}
}

func TestWorkerPool_HeartbeatSending(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	// Create a heartbeat channel
	heartbeatCh := make(chan health.HeartbeatMessage, 10)
	heartbeatInterval := 50 * time.Millisecond
	pool.SetHeartbeatChannel(heartbeatCh, heartbeatInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	taskInstanceID := 123
	taskInstance := &models.TaskInstance{
		ID:            taskInstanceID,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	// Create a task that runs for a while to allow multiple heartbeats
	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "sleep 0.5", // Sleep for 500ms
		Env:    make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Collect heartbeats
	var heartbeats []health.HeartbeatMessage
	heartbeatTimeout := time.After(600 * time.Millisecond)
	heartbeatDone := make(chan struct{})

	go func() {
		defer close(heartbeatDone)
		for {
			select {
			case hb := <-heartbeatCh:
				heartbeats = append(heartbeats, hb)
				if hb.TaskInstanceID != taskInstanceID {
					t.Errorf("Expected task instance ID %d, got %d", taskInstanceID, hb.TaskInstanceID)
				}
			case <-heartbeatTimeout:
				return
			}
		}
	}()

	// Wait for task to complete
	select {
	case result := <-pool.Results():
		if result.Error != nil && result.ExitCode != 0 {
			// Task might have been cancelled, which is fine for this test
			t.Logf("Task exited with code %d: %v", result.ExitCode, result.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}

	// Wait for heartbeat collection to finish
	<-heartbeatDone

	// We should have received at least a few heartbeats during the 500ms sleep
	// (at 50ms interval, we should get at least 8-10 heartbeats)
	if len(heartbeats) < 3 {
		t.Errorf("Expected at least 3 heartbeats, got %d", len(heartbeats))
	}

	// Verify heartbeats have valid timestamps
	for i, hb := range heartbeats {
		if hb.Timestamp.IsZero() {
			t.Errorf("Heartbeat %d has zero timestamp", i)
		}
	}

	// Verify heartbeats are roughly in order
	for i := 1; i < len(heartbeats); i++ {
		if heartbeats[i].Timestamp.Before(heartbeats[i-1].Timestamp) {
			t.Errorf("Heartbeat timestamps should be in order")
		}
	}
}

func TestWorkerPool_HeartbeatWithoutChannel(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir, testLogger(t))
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	// Don't set heartbeat channel - should work fine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	taskInstance := &models.TaskInstance{
		ID:            1,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Name:   "test-task",
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	now := time.Now()
	job := TaskJob{
		WorkflowEnv:  make(map[string]string),
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: "test-workflow",
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Task should complete successfully without heartbeat channel
	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Fatalf("Task execution failed: %v", result.Error)
		}
		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}
}

// capturedLogger returns a logger that writes to a buffer for testing
func capturedLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	return logger, &buf
}

func TestWorkerPool_CommandExecutionLogging(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create a logger that captures output
	logger, logBuffer := capturedLogger()

	pool, err := NewWorkerPool(1, outputDir, logger)
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pool.Stop()

	taskInstance := &models.TaskInstance{
		ID:            42,
		WorkflowRunID: 100,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	testScript := "echo 'Hello, World!'"
	task := &models.WorkflowTask{
		ID:      10,
		Name:    "test-task",
		Script:  testScript,
		Timeout: "5m",
		Env: map[string]string{
			"SECRET_KEY":   "secret-value",
			"PUBLIC_TOKEN": "public-token",
		},
	}

	workflowName := "test-workflow"
	now := time.Now()
	job := TaskJob{
		WorkflowEnv: map[string]string{
			"WORKFLOW_VAR": "workflow-value",
		},
		TaskInstance: taskInstance,
		Task:         task,
		WorkflowName: workflowName,
		RunStartedAt: now,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Wait for task to complete
	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Fatalf("Task execution failed: %v", result.Error)
		}
		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task result")
	}

	// Verify logs were written
	logOutput := logBuffer.String()
	if logOutput == "" {
		t.Fatal("Expected log output, but got empty string")
	}

	// Verify required fields are present in log
	checks := []struct {
		field string
		value string
	}{
		{"workflow", workflowName},
		{"task", task.Name},
		{"task_instance_id", "42"},
		{"script", testScript},
		{"timeout", "5m"},
	}

	for _, check := range checks {
		if !strings.Contains(logOutput, check.field) {
			t.Errorf("Log output missing field %q", check.field)
		}
		if !strings.Contains(logOutput, check.value) {
			t.Errorf("Log output missing value %q for field %q", check.value, check.field)
		}
	}

	// Verify environment variable VALUES are NOT logged (security)
	secretValue := "secret-value"
	publicTokenValue := "public-token"
	workflowValue := "workflow-value"

	if strings.Contains(logOutput, secretValue) {
		t.Errorf("Log output contains secret value %q (security risk)", secretValue)
	}
	if strings.Contains(logOutput, publicTokenValue) {
		t.Errorf("Log output contains env value %q (should not log env values)", publicTokenValue)
	}
	if strings.Contains(logOutput, workflowValue) {
		t.Errorf("Log output contains workflow env value %q (should not log env values)", workflowValue)
	}

	// Verify environment variable KEYS are logged (for audit trail)
	expectedKeys := []string{"SECRET_KEY", "PUBLIC_TOKEN", "WORKFLOW_VAR"}
	for _, key := range expectedKeys {
		if !strings.Contains(logOutput, key) {
			t.Errorf("Log output missing env key %q (should log keys for audit trail)", key)
		}
	}

	// Verify "Executing command" message is present
	if !strings.Contains(logOutput, "Executing command") {
		t.Error("Log output missing 'Executing command' message")
	}
}
