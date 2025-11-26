package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/watcher"
)

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

	cron       *cron.Cron
	entryMap   map[int]cron.EntryID // workflowID -> entryID
	entryMapMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewScheduler creates a new scheduler instance
func NewScheduler(db *sql.DB, watcherEvents <-chan watcher.WorkflowEvent, notifyCh chan<- WorkflowRunEvent) (*Scheduler, error) {
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

	log.Printf("Scheduler: Loading %d enabled workflows", len(workflows))

	// Schedule each workflow
	for _, wf := range workflows {
		if err := s.scheduleWorkflow(ctx, wf); err != nil {
			log.Printf("Scheduler: Failed to schedule workflow %q (ID: %d): %v", wf.Name, wf.ID, err)
			// Continue with other workflows
		}
	}

	// Start the cron scheduler
	s.cron.Start()
	log.Printf("Scheduler: Started")

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
				log.Printf("Scheduler: Failed to handle workflow event %v: %v", event, err)
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
			log.Printf("Scheduler: Workflow ID %d not found for add event", event.WorkflowID)
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
			log.Printf("Scheduler: Workflow ID %d not found for update event", event.WorkflowID)
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

	log.Printf("Scheduler: Scheduled workflow %q (ID: %d) with schedule %q", wf.Name, workflowID, wf.Schedule)
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
			log.Printf("Scheduler: Unscheduled workflow %q (ID: %d)", wf.Name, workflowID)
		} else {
			log.Printf("Scheduler: Unscheduled workflow ID %d", workflowID)
		}
	}
}

// onScheduleFire is called when a workflow's schedule fires
func (s *Scheduler) onScheduleFire(workflowID int) {
	// Run in a goroutine to avoid blocking the cron scheduler
	// This allows multiple workflows to fire concurrently
	go func() {
		ctx := context.Background()

		// Fetch workflow name for logging
		workflow, err := s.workflowStore.GetByID(ctx, workflowID)
		if err != nil {
			log.Printf("Scheduler: Failed to get workflow ID %d: %v", workflowID, err)
			return
		}
		if workflow == nil {
			log.Printf("Scheduler: Workflow ID %d not found", workflowID)
			return
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
				log.Printf("Scheduler: Database locked while creating workflow run for %q (ID: %d), retrying...", workflowName, workflowID)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
		}
		if err != nil {
			log.Printf("Scheduler: Failed to create workflow run for %q (ID: %d) after %d retries: %v", workflowName, workflowID, maxRetries, err)
			return
		}

		log.Printf("Scheduler: Created workflow run %d for workflow %q (ID: %d)", workflowRun.ID, workflowName, workflowID)

		// Get all tasks for the workflow
		tasks, err := s.taskStore.GetForWorkflow(ctx, workflowID)
		if err != nil {
			log.Printf("Scheduler: Failed to get tasks for workflow %q (ID: %d): %v", workflowName, workflowID, err)
			return
		}

		// Create task instances for all tasks
		for _, task := range tasks {
			taskInstance := &models.TaskInstance{
				WorkflowRunID: workflowRun.ID,
				TaskID:        task.ID,
				State:         models.TaskStatePending,
				Attempt:       1,
			}

			if err := s.taskInstanceStore.Insert(ctx, taskInstance); err != nil {
				log.Printf("Scheduler: Failed to create task instance for task %q (ID: %d) in workflow %q: %v", task.Name, task.ID, workflowName, err)
				// Continue with other tasks
				continue
			}
		}

		log.Printf("Scheduler: Created %d task instances for workflow run %d (workflow: %q)", len(tasks), workflowRun.ID, workflowName)

		// Notify executor
		if s.notifyCh != nil {
			select {
			case s.notifyCh <- WorkflowRunEvent{
				WorkflowRunID: workflowRun.ID,
				WorkflowID:    workflowID,
			}:
			case <-s.ctx.Done():
				log.Printf("Scheduler: Context cancelled while sending notification for workflow %q", workflowName)
			}
		}
	}()
}

// Stop stops the scheduler gracefully
func (s *Scheduler) Stop() {
	s.cancel()

	if s.cron != nil {
		// Stop cron scheduler (blocks until running jobs complete)
		ctx := s.cron.Stop()
		<-ctx.Done()
		log.Printf("Scheduler: Stopped")
	}
}
