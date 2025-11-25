package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/manujelko/gowtf/internal/models"
)

func TestNewWorkerPool(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(5, outputDir)
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

	_, err = NewWorkerPool(0, outputDir)
	if err == nil {
		t.Fatal("Expected error for size 0")
	}

	_, err = NewWorkerPool(-1, outputDir)
	if err == nil {
		t.Fatal("Expected error for negative size")
	}
}

func TestNewWorkerPool_EmptyOutputDir(t *testing.T) {
	_, err := NewWorkerPool(5, "")
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

	pool, err := NewWorkerPool(3, outputDir)
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

	pool, err := NewWorkerPool(1, outputDir)
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
		Script: "echo 'Hello, World!'",
		Env:    make(map[string]string),
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
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

	pool, err := NewWorkerPool(1, outputDir)
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
		Script: "exit 42", // Exit with code 42
		Env:    make(map[string]string),
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
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

	pool, err := NewWorkerPool(1, outputDir)
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
		Script:  "sleep 10", // Sleep for 10 seconds
		Timeout: "100ms",    // But timeout after 100ms
		Env:     make(map[string]string),
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
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

	pool, err := NewWorkerPool(3, outputDir)
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
			Script: "echo 'Task " + string(rune('A'+i)) + "'",
			Env:    make(map[string]string),
		}

		job := TaskJob{
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

	pool, err := NewWorkerPool(1, outputDir)
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
		Script: "echo $TEST_VAR",
		Env: map[string]string{
			"TEST_VAR": "Hello from env",
		},
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
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

func TestWorkerPool_OutputFileStructure(t *testing.T) {
	outputDir, err := os.MkdirTemp("", "gowtf-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	pool, err := NewWorkerPool(1, outputDir)
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

	taskInstance := &models.TaskInstance{
		ID:            taskInstanceID,
		WorkflowRunID: workflowRunID,
		TaskID:        10,
		State:         models.TaskStateRunning,
		Attempt:       1,
	}

	task := &models.WorkflowTask{
		ID:     10,
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
		Context:      ctx,
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case result := <-pool.Results():
		// Verify file paths follow expected structure
		expectedStdoutPath := filepath.Join(outputDir, "123", "456.stdout")
		expectedStderrPath := filepath.Join(outputDir, "123", "456.stderr")

		if result.StdoutPath != expectedStdoutPath {
			t.Errorf("Expected stdout path %s, got %s", expectedStdoutPath, result.StdoutPath)
		}

		if result.StderrPath != expectedStderrPath {
			t.Errorf("Expected stderr path %s, got %s", expectedStderrPath, result.StderrPath)
		}

		// Verify directory was created
		runDir := filepath.Join(outputDir, "123")
		if _, err := os.Stat(runDir); os.IsNotExist(err) {
			t.Errorf("Run directory does not exist: %s", runDir)
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

	pool, err := NewWorkerPool(1, outputDir)
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
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
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

	pool, err := NewWorkerPool(1, outputDir)
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
		Script: "echo 'test'",
		Env:    make(map[string]string),
	}

	job := TaskJob{
		TaskInstance: taskInstance,
		Task:         task,
		Context:      ctx,
	}

	// Submit should fail after stop
	err = pool.Submit(job)
	if err == nil {
		t.Error("Expected error when submitting after stop, got nil")
	}
}
