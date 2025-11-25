package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/manujelko/gowtf/internal/models"
)

// TaskJob represents a task to be executed by a worker
type TaskJob struct {
	TaskInstance *models.TaskInstance
	Task         *models.WorkflowTask
	OutputDir    string
	Context      context.Context
}

// TaskResult represents the result of a task execution
type TaskResult struct {
	TaskInstanceID int
	WorkflowRunID  int
	ExitCode       int
	StdoutPath     string
	StderrPath     string
	Error          error
}

// WorkerPool manages a pool of workers that execute tasks
type WorkerPool struct {
	size      int
	outputDir string

	jobChan    chan TaskJob
	resultChan chan TaskResult

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	started bool
	stopped bool
	mu      sync.RWMutex
}

// NewWorkerPool creates a new worker pool with the specified size and output directory
func NewWorkerPool(size int, outputDir string) (*WorkerPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("worker pool size must be greater than 0")
	}

	if outputDir == "" {
		return nil, fmt.Errorf("output directory cannot be empty")
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		size:       size,
		outputDir:  outputDir,
		jobChan:    make(chan TaskJob, size*2), // Buffer to allow some queuing
		resultChan: make(chan TaskResult, size*2),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start starts the worker pool and begins processing tasks
func (wp *WorkerPool) Start(ctx context.Context) error {
	wp.mu.Lock()
	if wp.started {
		wp.mu.Unlock()
		return fmt.Errorf("worker pool already started")
	}
	wp.started = true
	wp.mu.Unlock()

	// Start worker goroutines
	for i := 0; i < wp.size; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}

	return nil
}

// Stop stops the worker pool gracefully
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	if !wp.started || wp.stopped {
		wp.mu.Unlock()
		return
	}
	wp.stopped = true
	wp.mu.Unlock()

	wp.cancel()
	close(wp.jobChan)
	wp.wg.Wait()
	close(wp.resultChan)
}

// Submit submits a task job to the worker pool
func (wp *WorkerPool) Submit(job TaskJob) error {
	wp.mu.RLock()
	if !wp.started {
		wp.mu.RUnlock()
		return fmt.Errorf("worker pool not started")
	}
	if wp.stopped {
		wp.mu.RUnlock()
		return fmt.Errorf("worker pool is stopped")
	}
	wp.mu.RUnlock()

	select {
	case wp.jobChan <- job:
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	case <-job.Context.Done():
		return job.Context.Err()
	}
}

// Results returns the channel for receiving task results
func (wp *WorkerPool) Results() <-chan TaskResult {
	return wp.resultChan
}

// worker is the main worker goroutine that processes tasks
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}
			result := wp.executeTask(ctx, job)
			select {
			case wp.resultChan <- result:
			case <-ctx.Done():
				return
			case <-wp.ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		case <-wp.ctx.Done():
			return
		}
	}
}

// executeTask executes a task and returns the result
func (wp *WorkerPool) executeTask(ctx context.Context, job TaskJob) TaskResult {
	result := TaskResult{
		TaskInstanceID: job.TaskInstance.ID,
		WorkflowRunID:  job.TaskInstance.WorkflowRunID,
		ExitCode:       -1,
	}

	// Create output directory for this workflow run
	runDir := filepath.Join(wp.outputDir, fmt.Sprintf("%d", job.TaskInstance.WorkflowRunID))
	if err := os.MkdirAll(runDir, 0755); err != nil {
		result.Error = fmt.Errorf("failed to create run directory: %w", err)
		return result
	}

	// Create output file paths
	stdoutPath := filepath.Join(runDir, fmt.Sprintf("%d.stdout", job.TaskInstance.ID))
	stderrPath := filepath.Join(runDir, fmt.Sprintf("%d.stderr", job.TaskInstance.ID))

	result.StdoutPath = stdoutPath
	result.StderrPath = stderrPath

	// Create output files
	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		result.Error = fmt.Errorf("failed to create stdout file: %w", err)
		return result
	}
	defer stdoutFile.Close()

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		result.Error = fmt.Errorf("failed to create stderr file: %w", err)
		return result
	}
	defer stderrFile.Close()

	// Parse timeout
	timeout := 1 * time.Hour // Default timeout
	if job.Task.Timeout != "" {
		parsedTimeout, err := time.ParseDuration(job.Task.Timeout)
		if err != nil {
			result.Error = fmt.Errorf("failed to parse timeout: %w", err)
			return result
		}
		timeout = parsedTimeout
	}

	// Create context with timeout
	execCtx, cancel := context.WithTimeout(job.Context, timeout)
	defer cancel()

	// Build environment variables
	env := os.Environ()
	for k, v := range job.Task.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Create command
	cmd := exec.CommandContext(execCtx, "sh", "-c", job.Task.Script)
	cmd.Env = env
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	// Execute command
	err = cmd.Run()
	if err != nil {
		// Check if it was a timeout
		if execCtx.Err() == context.DeadlineExceeded {
			result.Error = fmt.Errorf("task execution timed out after %v", timeout)
			result.ExitCode = 124 // Standard timeout exit code
		} else {
			// Try to get exit code from the error
			if exitError, ok := err.(*exec.ExitError); ok {
				result.ExitCode = exitError.ExitCode()
			} else {
				result.Error = fmt.Errorf("failed to execute task: %w", err)
				result.ExitCode = 1
			}
		}
		return result
	}

	// Success
	result.ExitCode = 0
	return result
}
