package executor

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/scheduler"
)

// Executor orchestrates workflow task execution
type Executor struct {
	db                *sql.DB
	workflowStore     *models.WorkflowStore
	workflowRunStore  *models.WorkflowRunStore
	taskStore         *models.WorkflowTaskStore
	taskInstanceStore *models.TaskInstanceStore
	depsStore         *models.TaskDependenciesStore

	events <-chan scheduler.WorkflowRunEvent

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExecutor creates a new executor instance
func NewExecutor(db *sql.DB, events <-chan scheduler.WorkflowRunEvent) (*Executor, error) {
	workflowStore, err := models.NewWorkflowStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow store: %w", err)
	}

	workflowRunStore, err := models.NewWorkflowRunStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow run store: %w", err)
	}

	taskStore, err := models.NewWorkflowTaskStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create task store: %w", err)
	}

	taskInstanceStore, err := models.NewTaskInstanceStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create task instance store: %w", err)
	}

	depsStore, err := models.NewTaskDependenciesStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create dependencies store: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Executor{
		db:                db,
		workflowStore:     workflowStore,
		workflowRunStore:  workflowRunStore,
		taskStore:         taskStore,
		taskInstanceStore: taskInstanceStore,
		depsStore:         depsStore,
		events:            events,
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// Start starts the executor event loop
func (e *Executor) Start(ctx context.Context) error {
	log.Printf("Executor: Started")

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case event, ok := <-e.events:
				if !ok {
					return
				}
				if err := e.handleWorkflowRun(ctx, event); err != nil {
					log.Printf("Executor: Failed to handle workflow run %d: %v", event.WorkflowRunID, err)
				}
			case <-ctx.Done():
				return
			case <-e.ctx.Done():
				return
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// Stop stops the executor gracefully
func (e *Executor) Stop() {
	e.cancel()
	e.wg.Wait()
	log.Printf("Executor: Stopped")
}

// handleWorkflowRun processes a workflow run event
func (e *Executor) handleWorkflowRun(ctx context.Context, event scheduler.WorkflowRunEvent) error {
	log.Printf("Executor: Processing workflow run %d for workflow %d", event.WorkflowRunID, event.WorkflowID)

	// Process in a goroutine to allow concurrent workflow runs
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if err := e.processWorkflowRun(ctx, event.WorkflowRunID, event.WorkflowID); err != nil {
			log.Printf("Executor: Error processing workflow run %d: %v", event.WorkflowRunID, err)
		}
	}()

	return nil
}

// processWorkflowRun orchestrates task execution for a workflow run
func (e *Executor) processWorkflowRun(ctx context.Context, workflowRunID, workflowID int) error {
	// Update workflow run status to running
	if err := e.workflowRunStore.UpdateStatus(ctx, workflowRunID, models.RunRunning, nil); err != nil {
		return fmt.Errorf("failed to update workflow run status: %w", err)
	}

	// Load all tasks for the workflow
	tasks, err := e.taskStore.GetForWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to get tasks: %w", err)
	}

	// Build task name map for condition evaluation
	tasksByName := make(map[string]*models.WorkflowTask)
	for _, task := range tasks {
		tasksByName[task.Name] = task
	}

	// Load all task instances for this run
	taskInstances, err := e.taskInstanceStore.GetForRun(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("failed to get task instances: %w", err)
	}

	// Build instance map for quick lookup
	instanceMap := make(map[int]*models.TaskInstance)
	for _, ti := range taskInstances {
		instanceMap[ti.TaskID] = ti
	}

	// Main execution loop
	for {
		// Reload task instances to get latest state
		taskInstances, err = e.taskInstanceStore.GetForRun(ctx, workflowRunID)
		if err != nil {
			return fmt.Errorf("failed to reload task instances: %w", err)
		}

		// Update instance map
		instanceMap = make(map[int]*models.TaskInstance)
		for _, ti := range taskInstances {
			instanceMap[ti.TaskID] = ti
		}

		// Find ready tasks (dependencies satisfied)
		readyTasks, err := e.getReadyTasks(ctx, taskInstances, tasks, workflowRunID)
		if err != nil {
			return fmt.Errorf("failed to get ready tasks: %w", err)
		}

		if len(readyTasks) == 0 {
			// Check if all tasks are complete
			allComplete := true
			for _, ti := range taskInstances {
				if ti.State != models.TaskStateSuccess &&
					ti.State != models.TaskStateFailed &&
					ti.State != models.TaskStateSkipped {
					allComplete = false
					break
				}
			}

			if allComplete {
				// Update workflow run status
				allSuccess := true
				for _, ti := range taskInstances {
					if ti.State != models.TaskStateSuccess {
						allSuccess = false
						break
					}
				}

				finishedAt := time.Now()
				var status models.WorkflowRunStatus
				if allSuccess {
					status = models.RunSuccess
				} else {
					status = models.RunFailed
				}

				if err := e.workflowRunStore.UpdateStatus(ctx, workflowRunID, status, &finishedAt); err != nil {
					return fmt.Errorf("failed to update workflow run status: %w", err)
				}

				log.Printf("Executor: Workflow run %d completed with status %s", workflowRunID, status.String())
				return nil
			}

			// No ready tasks but not all complete - wait a bit and retry
			// This handles the case where tasks are waiting for external events
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		// Process ready tasks
		for _, ti := range readyTasks {
			// Find task by ID
			var task *models.WorkflowTask
			for _, t := range tasks {
				if t.ID == ti.TaskID {
					task = t
					break
				}
			}
			if task == nil {
				log.Printf("Executor: Task %d not found, skipping", ti.TaskID)
				continue
			}

			// Evaluate condition
			shouldRun, err := e.evaluateCondition(ctx, task.Condition, task, taskInstances, tasksByName, workflowID)
			if err != nil {
				log.Printf("Executor: Failed to evaluate condition for task %d: %v", ti.TaskID, err)
				// Mark as failed due to condition evaluation error
				if err := e.markTaskFailed(ctx, ti, 1); err != nil {
					log.Printf("Executor: Failed to mark task %d as failed: %v", ti.TaskID, err)
				}
				continue
			}

			if !shouldRun {
				// Condition not met, skip task
				if err := e.markTaskSkipped(ctx, ti); err != nil {
					log.Printf("Executor: Failed to mark task %d as skipped: %v", ti.TaskID, err)
				} else {
					log.Printf("Executor: Task %d (%s) skipped due to condition", ti.TaskID, task.Name)
				}
				continue
			}

			// Mark as queued
			if err := e.markTaskQueued(ctx, ti); err != nil {
				log.Printf("Executor: Failed to mark task %d as queued: %v", ti.TaskID, err)
				continue
			}

			// Mark as running
			if err := e.markTaskRunning(ctx, ti); err != nil {
				log.Printf("Executor: Failed to mark task %d as running: %v", ti.TaskID, err)
				continue
			}

			log.Printf("Executor: Task %d (%s) marked as running (worker pool integration pending)", ti.TaskID, task.Name)

			// TODO: Send to worker pool for actual execution
			// For now, we'll simulate immediate success for testing
			// In a real implementation, this would:
			// 1. Send task to worker pool
			// 2. Wait for completion event
			// 3. Update task state based on result

			// Simulate task execution (for testing purposes)
			// In production, this would be handled by the worker pool
			e.wg.Add(1)
			go func(taskInst *models.TaskInstance, taskDef *models.WorkflowTask) {
				defer e.wg.Done()
				// Simulate immediate success (exit code 0)
				// In real implementation, worker would execute and return result
				if err := e.markTaskSuccess(ctx, taskInst, 0); err != nil {
					log.Printf("Executor: Failed to mark task %d as success: %v", taskInst.TaskID, err)
				} else {
					log.Printf("Executor: Task %d (%s) completed successfully", taskInst.TaskID, taskDef.Name)
				}
			}(ti, task)
		}

		// Small delay before next iteration
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			// Continue to next iteration
		}
	}
}

// buildTaskGraph builds an adjacency list representation of the task dependency graph
// Returns a map from taskID to list of taskIDs that depend on it (reverse dependencies)
func buildTaskGraph(tasks []*models.WorkflowTask, deps []*models.TaskDependency) map[int][]int {
	graph := make(map[int][]int)

	// Initialize graph with all tasks
	for _, task := range tasks {
		graph[task.ID] = []int{}
	}

	// Build reverse dependencies: for each dependency, add the dependent task to the dependency's list
	for _, dep := range deps {
		graph[dep.DependsOn] = append(graph[dep.DependsOn], dep.TaskID)
	}

	return graph
}

// getReadyTasks finds tasks that are ready to run (all dependencies satisfied)
func (e *Executor) getReadyTasks(ctx context.Context, taskInstances []*models.TaskInstance, tasks []*models.WorkflowTask, workflowRunID int) ([]*models.TaskInstance, error) {
	// Build a map of taskID -> taskInstance for quick lookup
	instanceMap := make(map[int]*models.TaskInstance)
	for _, ti := range taskInstances {
		instanceMap[ti.TaskID] = ti
	}

	// Build a map of taskID -> task for quick lookup
	taskMap := make(map[int]*models.WorkflowTask)
	for _, task := range tasks {
		taskMap[task.ID] = task
	}

	var ready []*models.TaskInstance

	for _, ti := range taskInstances {
		// Skip tasks that are not pending
		if ti.State != models.TaskStatePending {
			continue
		}

		// Get dependencies for this task
		deps, err := e.depsStore.GetForTask(ctx, ti.TaskID)
		if err != nil {
			return nil, fmt.Errorf("failed to get dependencies for task %d: %w", ti.TaskID, err)
		}

		// Check if all dependencies are satisfied
		allSatisfied := true
		for _, depTaskID := range deps {
			depInstance, exists := instanceMap[depTaskID]
			if !exists {
				return nil, fmt.Errorf("dependency task instance %d not found for task %d", depTaskID, ti.TaskID)
			}

			// Dependency is satisfied if it's in a terminal state (success, failed, or skipped)
			if depInstance.State != models.TaskStateSuccess &&
				depInstance.State != models.TaskStateFailed &&
				depInstance.State != models.TaskStateSkipped {
				allSatisfied = false
				break
			}
		}

		if allSatisfied {
			ready = append(ready, ti)
		}
	}

	return ready, nil
}

// evaluateCondition evaluates a task condition based on upstream task states
func (e *Executor) evaluateCondition(ctx context.Context, condition string, task *models.WorkflowTask, taskInstances []*models.TaskInstance, tasksByName map[string]*models.WorkflowTask, workflowID int) (bool, error) {
	// Empty condition means always run
	if condition == "" {
		return true, nil
	}

	// Build instance map for quick lookup
	instanceMap := make(map[int]*models.TaskInstance)
	for _, ti := range taskInstances {
		instanceMap[ti.TaskID] = ti
	}

	// Parse condition
	// Conditions can be:
	// - "all_upstream.success" - all dependency tasks succeeded
	// - "any_upstream.failed" - any dependency task failed
	// - "task_name.success" - specific task succeeded
	// - "task_name.failed" - specific task failed

	if condition == "all_upstream.success" {
		// Get dependencies for this task
		deps, err := e.depsStore.GetForTask(ctx, task.ID)
		if err != nil {
			return false, fmt.Errorf("failed to get dependencies: %w", err)
		}

		if len(deps) == 0 {
			// No dependencies, condition is satisfied
			return true, nil
		}

		// All dependencies must have succeeded
		for _, depTaskID := range deps {
			depInstance, exists := instanceMap[depTaskID]
			if !exists {
				return false, fmt.Errorf("dependency task instance %d not found", depTaskID)
			}
			if depInstance.State != models.TaskStateSuccess {
				return false, nil
			}
		}
		return true, nil
	}

	if condition == "any_upstream.failed" {
		// Get dependencies for this task
		deps, err := e.depsStore.GetForTask(ctx, task.ID)
		if err != nil {
			return false, fmt.Errorf("failed to get dependencies: %w", err)
		}

		// At least one dependency must have failed
		for _, depTaskID := range deps {
			depInstance, exists := instanceMap[depTaskID]
			if !exists {
				return false, fmt.Errorf("dependency task instance %d not found", depTaskID)
			}
			if depInstance.State == models.TaskStateFailed {
				return true, nil
			}
		}
		return false, nil
	}

	// Check for task_name.success or task_name.failed
	parts := strings.Split(condition, ".")
	if len(parts) == 2 {
		taskName := parts[0]
		state := parts[1]

		// Resolve task by name
		depTask, err := e.taskStore.GetByNameForWorkflow(ctx, workflowID, taskName)
		if err != nil {
			return false, fmt.Errorf("failed to get task by name %q: %w", taskName, err)
		}
		if depTask == nil {
			return false, fmt.Errorf("task %q not found", taskName)
		}

		depInstance, exists := instanceMap[depTask.ID]
		if !exists {
			return false, fmt.Errorf("task instance for task %q not found", taskName)
		}

		if state == "success" {
			return depInstance.State == models.TaskStateSuccess, nil
		}
		if state == "failed" {
			return depInstance.State == models.TaskStateFailed, nil
		}
	}

	return false, fmt.Errorf("unknown condition format: %q", condition)
}

// markTaskQueued marks a task instance as queued
func (e *Executor) markTaskQueued(ctx context.Context, taskInstance *models.TaskInstance) error {
	taskInstance.State = models.TaskStateQueued
	return e.taskInstanceStore.Update(ctx, taskInstance)
}

// markTaskRunning marks a task instance as running
func (e *Executor) markTaskRunning(ctx context.Context, taskInstance *models.TaskInstance) error {
	now := time.Now()
	taskInstance.State = models.TaskStateRunning
	taskInstance.StartedAt = &now
	return e.taskInstanceStore.Update(ctx, taskInstance)
}

// markTaskSuccess marks a task instance as successful
func (e *Executor) markTaskSuccess(ctx context.Context, taskInstance *models.TaskInstance, exitCode int) error {
	now := time.Now()
	taskInstance.State = models.TaskStateSuccess
	taskInstance.ExitCode = &exitCode
	taskInstance.FinishedAt = &now
	return e.taskInstanceStore.Update(ctx, taskInstance)
}

// markTaskFailed marks a task instance as failed
func (e *Executor) markTaskFailed(ctx context.Context, taskInstance *models.TaskInstance, exitCode int) error {
	now := time.Now()
	taskInstance.State = models.TaskStateFailed
	taskInstance.ExitCode = &exitCode
	taskInstance.FinishedAt = &now
	return e.taskInstanceStore.Update(ctx, taskInstance)
}

// markTaskSkipped marks a task instance as skipped
func (e *Executor) markTaskSkipped(ctx context.Context, taskInstance *models.TaskInstance) error {
	now := time.Now()
	taskInstance.State = models.TaskStateSkipped
	taskInstance.FinishedAt = &now
	return e.taskInstanceStore.Update(ctx, taskInstance)
}
