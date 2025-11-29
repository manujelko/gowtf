package health

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/manujelko/gowtf/internal/models"
)

// HeartbeatMessage represents a heartbeat from a worker
type HeartbeatMessage struct {
	TaskInstanceID int
	Timestamp      time.Time
}

// activeTask tracks a task instance being monitored
type activeTask struct {
	cancelFunc     context.CancelFunc
	lastHeartbeat  time.Time
	taskInstanceID int
}

// Config holds configuration for the health monitor
type Config struct {
	HeartbeatInterval time.Duration // How often workers should send heartbeats
	TimeoutThreshold  time.Duration // Time to wait before considering a worker dead
	CheckInterval     time.Duration // How often to check for stale heartbeats
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval: 5 * time.Second,
		TimeoutThreshold:  15 * time.Second, // 3x heartbeat interval
		CheckInterval:     5 * time.Second,  // Check every heartbeat interval
	}
}

// HealthMonitor tracks worker heartbeats and handles stale worker detection
type HealthMonitor struct {
	taskInstanceStore *models.TaskInstanceStore
	heartbeatCh       chan HeartbeatMessage
	activeTasks       map[int]*activeTask
	mu                sync.RWMutex
	config            Config
	logger            *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewHealthMonitor creates a new health monitor instance
func NewHealthMonitor(taskInstanceStore *models.TaskInstanceStore, config Config, logger *slog.Logger) *HealthMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	// Buffer heartbeats to avoid blocking workers
	heartbeatBuffer := 100

	return &HealthMonitor{
		taskInstanceStore: taskInstanceStore,
		heartbeatCh:       make(chan HeartbeatMessage, heartbeatBuffer),
		activeTasks:       make(map[int]*activeTask),
		config:            config,
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// HeartbeatChannel returns the channel for sending heartbeat messages
func (hm *HealthMonitor) HeartbeatChannel() chan<- HeartbeatMessage {
	return hm.heartbeatCh
}

// RegisterTask registers a task instance for monitoring
func (hm *HealthMonitor) RegisterTask(taskInstanceID int, cancelFunc context.CancelFunc) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.activeTasks[taskInstanceID] = &activeTask{
		cancelFunc:     cancelFunc,
		lastHeartbeat:  time.Now(),
		taskInstanceID: taskInstanceID,
	}
	hm.logger.Info("Registered task instance for monitoring",
		"task_instance_id", taskInstanceID)
}

// UnregisterTask unregisters a task instance from monitoring
func (hm *HealthMonitor) UnregisterTask(taskInstanceID int) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, exists := hm.activeTasks[taskInstanceID]; exists {
		delete(hm.activeTasks, taskInstanceID)
		hm.logger.Info("Unregistered task instance",
			"task_instance_id", taskInstanceID)
	}
}

// Start starts the health monitor's monitoring loop
func (hm *HealthMonitor) Start(ctx context.Context) {
	hm.wg.Add(1)
	go func() {
		defer hm.wg.Done()
		hm.monitor(ctx)
	}()
	hm.logger.Info("HealthMonitor started")
}

// Stop stops the health monitor gracefully
func (hm *HealthMonitor) Stop() {
	hm.cancel()
	hm.wg.Wait()
	close(hm.heartbeatCh)
	hm.logger.Info("HealthMonitor stopped")
}

// monitor is the main monitoring loop
func (hm *HealthMonitor) monitor(ctx context.Context) {
	ticker := time.NewTicker(hm.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case heartbeat, ok := <-hm.heartbeatCh:
			if !ok {
				// Channel closed, exit
				return
			}
			hm.handleHeartbeat(heartbeat)

		case <-ticker.C:
			hm.checkStaleHeartbeats(ctx)

		case <-ctx.Done():
			return
		case <-hm.ctx.Done():
			return
		}
	}
}

// handleHeartbeat updates the last heartbeat time for a task instance
func (hm *HealthMonitor) handleHeartbeat(msg HeartbeatMessage) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if task, exists := hm.activeTasks[msg.TaskInstanceID]; exists {
		task.lastHeartbeat = msg.Timestamp
	}
}

// checkStaleHeartbeats checks for tasks that haven't sent heartbeats in a while
func (hm *HealthMonitor) checkStaleHeartbeats(ctx context.Context) {
	hm.mu.Lock()
	now := time.Now()
	var staleTasks []*activeTask

	for taskInstanceID, task := range hm.activeTasks {
		timeSinceLastHeartbeat := now.Sub(task.lastHeartbeat)
		if timeSinceLastHeartbeat > hm.config.TimeoutThreshold {
			staleTasks = append(staleTasks, task)
			hm.logger.Warn("Task instance is stale",
				"task_instance_id", taskInstanceID,
				"time_since_last_heartbeat", timeSinceLastHeartbeat)
		}
	}
	hm.mu.Unlock()

	// Handle stale tasks outside the lock to avoid holding it during DB operations
	for _, task := range staleTasks {
		hm.handleStaleTask(ctx, task)
	}
}

// handleStaleTask cancels the context and marks the task as failed
func (hm *HealthMonitor) handleStaleTask(ctx context.Context, task *activeTask) {
	hm.mu.Lock()
	// Remove from active tasks to prevent duplicate handling
	delete(hm.activeTasks, task.taskInstanceID)
	cancelFunc := task.cancelFunc
	hm.mu.Unlock()

	// Cancel the context to kill the subprocess
	hm.logger.Info("Cancelling context for stale task instance",
		"task_instance_id", task.taskInstanceID)
	cancelFunc()

	// Mark task as failed in the database
	taskInstance, err := hm.taskInstanceStore.GetByID(ctx, task.taskInstanceID)
	if err != nil {
		hm.logger.Error("Failed to get task instance",
			"task_instance_id", task.taskInstanceID,
			"error", err)
		return
	}

	if taskInstance == nil {
		hm.logger.Warn("Task instance not found in database",
			"task_instance_id", task.taskInstanceID)
		return
	}

	// Only mark as failed if it's still in a running state
	if taskInstance.State == models.TaskStateRunning {
		now := time.Now()
		exitCode := 124 // Standard timeout exit code
		taskInstance.State = models.TaskStateFailed
		taskInstance.ExitCode = &exitCode
		taskInstance.FinishedAt = &now

		if err := hm.taskInstanceStore.Update(ctx, taskInstance); err != nil {
			hm.logger.Error("Failed to mark task instance as failed",
				"task_instance_id", task.taskInstanceID,
				"error", err)
			return
		}

		hm.logger.Info("Marked task instance as failed due to stale heartbeat",
			"task_instance_id", task.taskInstanceID)
	}
}
