package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/watcher"
)

// retryDBOperationWithTx retries a transaction operation with exponential backoff
func retryDBOperationWithTx(ctx context.Context, db *sql.DB, maxRetries int, fn func(*sql.Tx) error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			if isSQLiteBusy(err) && i < maxRetries-1 {
				backoff := time.Duration(1<<uint(i)) * 10 * time.Millisecond
				time.Sleep(backoff)
				lastErr = err
				continue
			}
			return err
		}

		err = fn(tx)
		if err != nil {
			tx.Rollback()
			if isSQLiteBusy(err) && i < maxRetries-1 {
				backoff := time.Duration(1<<uint(i)) * 10 * time.Millisecond
				time.Sleep(backoff)
				lastErr = err
				continue
			}
			return err
		}

		err = tx.Commit()
		if err != nil {
			if isSQLiteBusy(err) && i < maxRetries-1 {
				backoff := time.Duration(1<<uint(i)) * 10 * time.Millisecond
				time.Sleep(backoff)
				lastErr = err
				continue
			}
			return err
		}

		return nil
	}
	return lastErr
}

// isSQLiteBusy checks if an error is a SQLite busy/locked error
func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "database is locked (5)")
}

// createTaskInstanceInTx creates a task instance within a transaction
func (s *Scheduler) createTaskInstanceInTx(ctx context.Context, tx *sql.Tx, ti *models.TaskInstance) error {
	return s.taskInstanceStore.InsertTx(ctx, tx, ti)
}

// WorkflowRunEvent represents a new workflow run that needs to be executed
type WorkflowRunEvent struct {
	WorkflowRunID int
	WorkflowID    int
}

// Scheduler manages workflow scheduling based on cron expressions
type Scheduler struct {
	db                *sql.DB
	workflowStore     *models.WorkflowStore
	workflowRunStore  *models.WorkflowRunStore
	taskStore         *models.WorkflowTaskStore
	taskInstanceStore *models.TaskInstanceStore

	watcherEvents <-chan watcher.WorkflowEvent
	notifyCh      chan<- WorkflowRunEvent
	logger        *slog.Logger

	cron       *cron.Cron
	entryMap   map[int]cron.EntryID // workflowID -> entryID
	entryMapMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewScheduler creates a new scheduler instance
func NewScheduler(db *sql.DB, watcherEvents <-chan watcher.WorkflowEvent, notifyCh chan<- WorkflowRunEvent, logger *slog.Logger) (*Scheduler, error) {
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

	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		db:                db,
		workflowStore:     workflowStore,
		workflowRunStore:  workflowRunStore,
		taskStore:         taskStore,
		taskInstanceStore: taskInstanceStore,
		watcherEvents:     watcherEvents,
		notifyCh:          notifyCh,
		logger:            logger,
		entryMap:          make(map[int]cron.EntryID),
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// Start starts the scheduler
func (s *Scheduler) Start(ctx context.Context) error {
	// Create cron instance with standard parser (5-field cron)
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s.cron = cron.New(cron.WithParser(parser))

	// Load all enabled workflows on startup
	workflows, err := s.workflowStore.GetAllEnabled(ctx)
	if err != nil {
		return fmt.Errorf("failed to load enabled workflows: %w", err)
	}

	s.logger.Info("Loading enabled workflows", "count", len(workflows))

	// Schedule each workflow
	for _, wf := range workflows {
		if err := s.scheduleWorkflow(ctx, wf); err != nil {
			s.logger.Error("Failed to schedule workflow",
				"workflow_id", wf.ID,
				"workflow_name", wf.Name,
				"error", err)
			// Continue with other workflows
		}
	}

	// Start the cron scheduler
	s.cron.Start()
	s.logger.Info("Scheduler started")

	// Start goroutine to listen for watcher events
	go s.handleWatcherEvents(ctx)

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// handleWatcherEvents processes workflow events from the watcher
func (s *Scheduler) handleWatcherEvents(ctx context.Context) {
	for {
		select {
		case event, ok := <-s.watcherEvents:
			if !ok {
				return
			}
			if err := s.handleWorkflowEvent(ctx, event); err != nil {
				s.logger.Error("Failed to handle workflow event",
					"event_type", event.Type,
					"workflow_id", event.WorkflowID,
					"workflow_name", event.WorkflowName,
					"error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// handleWorkflowEvent handles a workflow event (add/update/delete)
func (s *Scheduler) handleWorkflowEvent(ctx context.Context, event watcher.WorkflowEvent) error {
	switch event.Type {
	case watcher.EventAdded:
		// Load workflow by ID
		wf, err := s.workflowStore.GetByID(ctx, event.WorkflowID)
		if err != nil {
			return fmt.Errorf("failed to get workflow: %w", err)
		}
		if wf == nil {
			s.logger.Warn("Workflow not found for add event",
				"workflow_id", event.WorkflowID)
			return nil
		}
		// Only schedule if enabled
		if wf.Enabled {
			return s.scheduleWorkflow(ctx, wf)
		}

	case watcher.EventUpdated:
		// Unschedule old entry first
		s.unscheduleWorkflow(event.WorkflowID)
		// Load updated workflow
		wf, err := s.workflowStore.GetByID(ctx, event.WorkflowID)
		if err != nil {
			return fmt.Errorf("failed to get workflow: %w", err)
		}
		if wf == nil {
			s.logger.Warn("Workflow not found for update event",
				"workflow_id", event.WorkflowID)
			return nil
		}
		// Re-schedule if enabled
		if wf.Enabled {
			return s.scheduleWorkflow(ctx, wf)
		}

	case watcher.EventDeleted:
		s.unscheduleWorkflow(event.WorkflowID)
	}

	return nil
}

// HandleEnabledStateChange handles a workflow enabled state change
// This is called when a workflow is enabled/disabled via the UI
func (s *Scheduler) HandleEnabledStateChange(ctx context.Context, workflowID int) error {
	// Treat enabled state change as an update event
	event := watcher.WorkflowEvent{
		Type:       watcher.EventUpdated,
		WorkflowID: workflowID,
	}
	return s.handleWorkflowEvent(ctx, event)
}

// scheduleWorkflow adds a workflow to the cron scheduler
func (s *Scheduler) scheduleWorkflow(ctx context.Context, wf *models.Workflow) error {
	if s.cron == nil {
		return fmt.Errorf("cron scheduler not initialized")
	}

	// Skip workflows without a schedule (manual-only workflows)
	if wf.Schedule == "" {
		s.logger.Info("Skipping workflow without schedule (manual-only)",
			"workflow_id", wf.ID,
			"workflow_name", wf.Name)
		return nil
	}

	// Create callback that captures workflow ID
	workflowID := wf.ID
	callback := func() {
		s.onScheduleFire(workflowID)
	}

	// Add to cron scheduler (it will parse the schedule internally)
	entryID, err := s.cron.AddFunc(wf.Schedule, callback)
	if err != nil {
		return fmt.Errorf("failed to add cron entry: %w", err)
	}

	// Store entry ID for later removal
	s.entryMapMu.Lock()
	s.entryMap[workflowID] = entryID
	s.entryMapMu.Unlock()

	s.logger.Info("Scheduled workflow",
		"workflow_id", workflowID,
		"workflow_name", wf.Name,
		"schedule", wf.Schedule)
	return nil
}

// unscheduleWorkflow removes a workflow from the cron scheduler
func (s *Scheduler) unscheduleWorkflow(workflowID int) {
	if s.cron == nil {
		return
	}

	s.entryMapMu.Lock()
	entryID, exists := s.entryMap[workflowID]
	if exists {
		delete(s.entryMap, workflowID)
	}
	s.entryMapMu.Unlock()

	if exists {
		s.cron.Remove(entryID)
		// Try to get workflow name for logging
		ctx := context.Background()
		wf, err := s.workflowStore.GetByID(ctx, workflowID)
		if err == nil && wf != nil {
			s.logger.Info("Unscheduled workflow",
				"workflow_id", workflowID,
				"workflow_name", wf.Name)
		} else {
			s.logger.Info("Unscheduled workflow",
				"workflow_id", workflowID)
		}
	}
}

// triggerWorkflowRun creates a workflow run and task instances, then notifies the executor
// This is the core logic shared between scheduled and manual triggers
func (s *Scheduler) triggerWorkflowRun(ctx context.Context, workflowID int) (*models.WorkflowRun, error) {
	// Fetch workflow name for logging
	workflow, err := s.workflowStore.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	if workflow == nil {
		return nil, fmt.Errorf("workflow not found")
	}
	workflowName := workflow.Name

	// Create workflow run with retry logic for database locks
	var workflowRun *models.WorkflowRun
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		workflowRun, err = s.workflowRunStore.Insert(ctx, workflowID, models.RunPending)
		if err == nil {
			break
		}
		// If it's a lock error and we have retries left, wait and retry
		if i < maxRetries-1 {
			s.logger.Warn("Database locked while creating workflow run, retrying",
				"workflow_id", workflowID,
				"workflow_name", workflowName,
				"retry", i+1)
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			continue
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow run after retries: %w", err)
	}

	s.logger.Info("Created workflow run",
		"workflow_run_id", workflowRun.ID,
		"workflow_id", workflowID,
		"workflow_name", workflowName)

	// Get all tasks for the workflow
	tasks, err := s.taskStore.GetForWorkflow(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks for workflow: %w", err)
	}

	// Create task instances for all tasks in a transaction to reduce contention
	// Retry the entire transaction if we get a busy error
	var createdCount int
	err = retryDBOperationWithTx(ctx, s.db, 5, func(tx *sql.Tx) error {
		createdCount = 0
		for _, task := range tasks {
			taskInstance := &models.TaskInstance{
				WorkflowRunID: workflowRun.ID,
				TaskID:        task.ID,
				State:         models.TaskStatePending,
				Attempt:       1,
			}

			if err := s.createTaskInstanceInTx(ctx, tx, taskInstance); err != nil {
				return fmt.Errorf("failed to create task instance for task %q (ID: %d): %w", task.Name, task.ID, err)
			}
			createdCount++
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create task instances for workflow: %w", err)
	}

	s.logger.Info("Created task instances for workflow run",
		"workflow_run_id", workflowRun.ID,
		"workflow_id", workflowID,
		"workflow_name", workflowName,
		"task_count", createdCount)

	// Notify executor
	if s.notifyCh != nil {
		select {
		case s.notifyCh <- WorkflowRunEvent{
			WorkflowRunID: workflowRun.ID,
			WorkflowID:    workflowID,
		}:
		case <-s.ctx.Done():
			s.logger.Warn("Context cancelled while sending notification",
				"workflow_id", workflowID,
				"workflow_name", workflowName)
		case <-ctx.Done():
			s.logger.Warn("Request context cancelled while sending notification",
				"workflow_id", workflowID,
				"workflow_name", workflowName)
		}
	}

	return workflowRun, nil
}

// onScheduleFire is called when a workflow's schedule fires
func (s *Scheduler) onScheduleFire(workflowID int) {
	// Run in a goroutine to avoid blocking the cron scheduler
	// This allows multiple workflows to fire concurrently
	go func() {
		ctx := context.Background()
		_, err := s.triggerWorkflowRun(ctx, workflowID)
		if err != nil {
			s.logger.Error("Failed to trigger workflow run",
				"workflow_id", workflowID,
				"error", err)
		}
	}()
}

// TriggerWorkflow manually triggers a workflow run
// This method validates the workflow exists and is enabled, then creates a workflow run
func (s *Scheduler) TriggerWorkflow(ctx context.Context, workflowID int) (*models.WorkflowRun, error) {
	// Fetch workflow to validate it exists and is enabled
	workflow, err := s.workflowStore.GetByID(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	if workflow == nil {
		return nil, fmt.Errorf("workflow not found")
	}
	if !workflow.Enabled {
		return nil, fmt.Errorf("workflow is disabled")
	}

	// Trigger the workflow run
	return s.triggerWorkflowRun(ctx, workflowID)
}

// Stop stops the scheduler gracefully
func (s *Scheduler) Stop() {
	s.cancel()

	if s.cron != nil {
		// Stop cron scheduler (blocks until running jobs complete)
		ctx := s.cron.Stop()
		<-ctx.Done()
		s.logger.Info("Scheduler stopped")
	}
}
