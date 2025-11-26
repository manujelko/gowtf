package watcher

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/manujelko/gowtf/internal/models"
	"github.com/manujelko/gowtf/internal/workflow"
)

// WorkflowEventType represents the type of workflow event
type WorkflowEventType int

const (
	// EventAdded indicates a workflow was added
	EventAdded WorkflowEventType = iota
	// EventUpdated indicates a workflow was updated
	EventUpdated
	// EventDeleted indicates a workflow was deleted
	EventDeleted
)

// WorkflowEvent represents a workflow change event
type WorkflowEvent struct {
	Type         WorkflowEventType
	WorkflowName string
	WorkflowID   int
}

// Watcher monitors a directory for workflow YAML files and syncs them to the database
type Watcher struct {
	db             *sql.DB
	watchDir       string
	workflowStore  *models.WorkflowStore
	taskStore      *models.WorkflowTaskStore
	depStore       *models.TaskDependenciesStore
	notifyCh       chan<- WorkflowEvent
	fsWatcher      *fsnotify.Watcher
	fsWatcherMu    sync.Mutex // protects fsWatcher
	ctx            context.Context
	cancel         context.CancelFunc
	knownFiles     map[string]string // filepath -> hash
	fileToWorkflow map[string]string // filepath -> workflow name
	fileErrors     map[string]string // filepath -> error message
	mu             sync.RWMutex
	debounceTimer  *time.Timer
	pendingFiles   map[string]struct{}
	debounceMu     sync.Mutex
}

// NewWatcher creates a new watcher instance
func NewWatcher(db *sql.DB, watchDir string, notifyCh chan<- WorkflowEvent) (*Watcher, error) {
	workflowStore, err := models.NewWorkflowStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow store: %w", err)
	}

	taskStore, err := models.NewWorkflowTaskStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create task store: %w", err)
	}

	depStore, err := models.NewTaskDependenciesStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create dependency store: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Watcher{
		db:             db,
		watchDir:       watchDir,
		workflowStore:  workflowStore,
		taskStore:      taskStore,
		depStore:       depStore,
		notifyCh:       notifyCh,
		ctx:            ctx,
		cancel:         cancel,
		knownFiles:     make(map[string]string),
		fileToWorkflow: make(map[string]string),
		fileErrors:     make(map[string]string),
		pendingFiles:   make(map[string]struct{}),
	}, nil
}

// Stop stops the watcher and cleans up resources
func (w *Watcher) Stop() {
	w.cancel()

	// Stop debounce timer with proper locking
	w.debounceMu.Lock()
	if w.debounceTimer != nil {
		w.debounceTimer.Stop()
		w.debounceTimer = nil
	}
	w.debounceMu.Unlock()

	// Close file watcher with proper locking
	w.fsWatcherMu.Lock()
	fsWatcher := w.fsWatcher
	w.fsWatcher = nil
	w.fsWatcherMu.Unlock()

	if fsWatcher != nil {
		fsWatcher.Close()
	}
}

// computeHash computes a SHA256 hash of the content
func computeHash(content []byte) string {
	hash := sha256.Sum256(content)
	return hex.EncodeToString(hash[:])
}

// syncWorkflow loads, validates, and syncs a workflow file to the database
func (w *Watcher) syncWorkflow(filePath string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	newHash := computeHash(content)

	// Check if file hash changed
	w.mu.RLock()
	oldHash, exists := w.knownFiles[filePath]
	w.mu.RUnlock()

	if exists && oldHash == newHash {
		// No change, skip
		return nil
	}

	// Load and validate workflow
	wf, err := workflow.Load(filePath)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to load workflow: %v", err)
		log.Printf("Failed to load workflow from %s: %v", filePath, err)
		// Store error for UI display
		w.mu.Lock()
		w.fileErrors[filePath] = errMsg
		w.mu.Unlock()
		return fmt.Errorf("validation error: %w", err)
	}

	// Clear any previous error for this file
	w.mu.Lock()
	delete(w.fileErrors, filePath)
	w.mu.Unlock()

	// Check if workflow exists (outside transaction for now)
	existingWf, err := w.workflowStore.GetByName(w.ctx, wf.Name)
	if err != nil {
		return fmt.Errorf("failed to check existing workflow: %w", err)
	}

	var eventType WorkflowEventType
	var workflowID int

	// Start transaction
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if existingWf == nil {
		// New workflow
		eventType = EventAdded
		dbWf := &models.Workflow{
			Name:     wf.Name,
			Schedule: wf.Schedule,
			Env:      wf.Env,
			Hash:     newHash,
			Enabled:  false, // Disabled by default
		}

		if err := w.workflowStore.InsertTx(w.ctx, tx, dbWf); err != nil {
			return fmt.Errorf("failed to insert workflow: %w", err)
		}
		workflowID = dbWf.ID
	} else {
		// Update existing workflow
		eventType = EventUpdated
		workflowID = existingWf.ID

		existingWf.Schedule = wf.Schedule
		existingWf.Env = wf.Env
		existingWf.Hash = newHash

		if err := w.workflowStore.UpdateTx(w.ctx, tx, existingWf); err != nil {
			return fmt.Errorf("failed to update workflow: %w", err)
		}

		// Delete existing dependencies
		if err := w.depStore.DeleteForWorkflowTx(w.ctx, tx, workflowID); err != nil {
			return fmt.Errorf("failed to delete dependencies: %w", err)
		}

		// Delete existing tasks
		if err := w.taskStore.DeleteForWorkflowTx(w.ctx, tx, workflowID); err != nil {
			return fmt.Errorf("failed to delete tasks: %w", err)
		}
	}

	// Insert new tasks
	taskMap := make(map[string]int) // task name -> task ID

	for _, task := range wf.Tasks {
		dbTask := &models.WorkflowTask{
			WorkflowID: workflowID,
			Name:       task.Name,
			Script:     task.Script,
			Retries:    task.Retries,
			RetryDelay: task.RetryDelay,
			Timeout:    task.Timeout,
			Condition:  task.Condition,
			Env:        task.Env,
		}

		if err := w.taskStore.InsertTx(w.ctx, tx, dbTask); err != nil {
			return fmt.Errorf("failed to insert task: %w", err)
		}

		taskMap[task.Name] = dbTask.ID
	}

	// Insert dependencies
	for _, task := range wf.Tasks {
		taskID, ok := taskMap[task.Name]
		if !ok {
			continue
		}

		for _, depName := range task.DependsOn {
			depID, ok := taskMap[depName]
			if !ok {
				return fmt.Errorf("dependency %q not found for task %q", depName, task.Name)
			}

			if err := w.depStore.InsertTx(w.ctx, tx, taskID, depID); err != nil {
				return fmt.Errorf("failed to insert dependency: %w", err)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update known files and workflow mapping
	w.mu.Lock()
	w.knownFiles[filePath] = newHash
	w.fileToWorkflow[filePath] = wf.Name
	w.mu.Unlock()

	// Send notification
	if w.notifyCh != nil {
		select {
		case w.notifyCh <- WorkflowEvent{
			Type:         eventType,
			WorkflowName: wf.Name,
			WorkflowID:   workflowID,
		}:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	return nil
}

// deleteWorkflow removes a workflow from the database
func (w *Watcher) deleteWorkflow(workflowName string) error {
	existingWf, err := w.workflowStore.GetByName(w.ctx, workflowName)
	if err != nil {
		return fmt.Errorf("failed to get workflow: %w", err)
	}

	if existingWf == nil {
		// Already deleted or never existed
		return nil
	}

	// Delete workflow (cascade will delete tasks and dependencies)
	if err := w.workflowStore.Delete(w.ctx, existingWf.ID); err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}

	// Remove from known files and clear errors
	w.mu.Lock()
	for path, name := range w.fileToWorkflow {
		if name == workflowName {
			delete(w.knownFiles, path)
			delete(w.fileToWorkflow, path)
			delete(w.fileErrors, path)
			break
		}
	}
	w.mu.Unlock()

	// Send notification
	if w.notifyCh != nil {
		select {
		case w.notifyCh <- WorkflowEvent{
			Type:         EventDeleted,
			WorkflowName: workflowName,
			WorkflowID:   0,
		}:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	return nil
}

// scanDirectory scans the watch directory for YAML files and processes them
func (w *Watcher) scanDirectory() error {
	entries, err := os.ReadDir(w.watchDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}

		filePath := filepath.Join(w.watchDir, name)
		if err := w.syncWorkflow(filePath); err != nil {
			log.Printf("Failed to sync workflow %s: %v", filePath, err)
			// Continue processing other files
		}
	}

	return nil
}

// processPendingFiles processes all pending files after debounce period
func (w *Watcher) processPendingFiles() {
	w.debounceMu.Lock()
	pending := make([]string, 0, len(w.pendingFiles))
	for file := range w.pendingFiles {
		pending = append(pending, file)
	}
	w.pendingFiles = make(map[string]struct{})
	w.debounceMu.Unlock()

	for _, file := range pending {
		// Check if file still exists
		if _, err := os.Stat(file); os.IsNotExist(err) {
			// File was deleted, look up workflow name from mapping
			w.mu.RLock()
			workflowName, exists := w.fileToWorkflow[file]
			w.mu.RUnlock()
			if exists {
				if err := w.deleteWorkflow(workflowName); err != nil {
					log.Printf("Failed to delete workflow %s: %v", workflowName, err)
				}
			} else {
				// File was deleted but we don't have it in our mapping
				// This can happen if the file was deleted before we could sync it
				// Just log and continue - we can't determine the workflow name
				log.Printf("File %s was deleted but workflow name is unknown (file was never synced)", file)
			}
			continue
		}

		// File exists, load it and sync (this will get the workflow name from YAML)
		if err := w.syncWorkflow(file); err != nil {
			log.Printf("Failed to sync workflow %s: %v", file, err)
		}
	}
}

// scheduleDebounce schedules processing of pending files after debounce period
func (w *Watcher) scheduleDebounce() {
	w.debounceMu.Lock()
	defer w.debounceMu.Unlock()

	// Check if context is already cancelled
	select {
	case <-w.ctx.Done():
		return
	default:
	}

	if w.debounceTimer != nil {
		w.debounceTimer.Stop()
	}

	w.debounceTimer = time.AfterFunc(500*time.Millisecond, w.processPendingFiles)
}

// Run starts the watcher and processes file system events
func (w *Watcher) Run() error {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	w.fsWatcherMu.Lock()
	w.fsWatcher = fsWatcher
	w.fsWatcherMu.Unlock()

	// Watch the directory
	if err := fsWatcher.Add(w.watchDir); err != nil {
		return fmt.Errorf("failed to watch directory: %w", err)
	}

	// Perform initial scan
	if err := w.scanDirectory(); err != nil {
		return fmt.Errorf("failed to scan directory: %w", err)
	}

	// Process events
	go func() {
		for {
			select {
			case event, ok := <-fsWatcher.Events:
				if !ok {
					return
				}

				// Only process YAML files
				if !strings.HasSuffix(event.Name, ".yaml") && !strings.HasSuffix(event.Name, ".yml") {
					continue
				}

				// Handle different event types
				switch {
				case event.Op&fsnotify.Create == fsnotify.Create:
					fallthrough
				case event.Op&fsnotify.Write == fsnotify.Write:
					w.debounceMu.Lock()
					w.pendingFiles[event.Name] = struct{}{}
					w.debounceMu.Unlock()
					w.scheduleDebounce()

				case event.Op&fsnotify.Remove == fsnotify.Remove:
					// File was deleted, look up workflow name from mapping
					w.mu.RLock()
					workflowName, exists := w.fileToWorkflow[event.Name]
					w.mu.RUnlock()
					if exists {
						if err := w.deleteWorkflow(workflowName); err != nil {
							log.Printf("Failed to delete workflow %s: %v", workflowName, err)
						}
					} else {
						// File was deleted but we don't have it in our mapping
						// This can happen if the file was deleted before we could sync it
						log.Printf("File %s was deleted but workflow name is unknown (file was never synced)", event.Name)
					}

				case event.Op&fsnotify.Rename == fsnotify.Rename:
					// Rename can mean:
					// 1. File was renamed/deleted (file no longer exists)
					// 2. File was created via rename (common with editors - write to temp then rename)
					// Check if file still exists to determine which case
					if _, err := os.Stat(event.Name); os.IsNotExist(err) {
						// File doesn't exist - it was deleted/renamed away
						w.mu.RLock()
						workflowName, exists := w.fileToWorkflow[event.Name]
						w.mu.RUnlock()
						if exists {
							if err := w.deleteWorkflow(workflowName); err != nil {
								log.Printf("Failed to delete workflow %s: %v", workflowName, err)
							}
						} else {
							log.Printf("File %s was renamed/deleted but workflow name is unknown (file was never synced)", event.Name)
						}
					} else {
						// File exists - it was created/renamed into place, treat as Create/Write
						w.debounceMu.Lock()
						w.pendingFiles[event.Name] = struct{}{}
						w.debounceMu.Unlock()
						w.scheduleDebounce()
					}
				}

			case err, ok := <-fsWatcher.Errors:
				if !ok {
					return
				}
				log.Printf("File watcher error: %v", err)

			case <-w.ctx.Done():
				return
			}
		}
	}()

	// Wait for context cancellation
	<-w.ctx.Done()
	return nil
}

// GetFileErrors returns a map of file paths to error messages
func (w *Watcher) GetFileErrors() map[string]string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Return a copy to avoid race conditions
	errors := make(map[string]string)
	for k, v := range w.fileErrors {
		errors[k] = v
	}
	return errors
}

// GetErrorsByWorkflowName returns a map of workflow names to error messages
func (w *Watcher) GetErrorsByWorkflowName() map[string]string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Build reverse map: workflow name -> error
	errorsByWorkflow := make(map[string]string)
	for filePath, errMsg := range w.fileErrors {
		// Try to find workflow name from fileToWorkflow map
		if workflowName, exists := w.fileToWorkflow[filePath]; exists {
			errorsByWorkflow[workflowName] = errMsg
		} else {
			// Fallback: try to extract from filename
			baseName := filepath.Base(filePath)
			nameWithoutExt := strings.TrimSuffix(strings.TrimSuffix(baseName, ".yaml"), ".yml")
			errorsByWorkflow[nameWithoutExt] = errMsg
		}
	}
	return errorsByWorkflow
}
