package cleanup

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/manujelko/gowtf/internal/models"
)

// Cleanup handles periodic cleanup of old workflow runs based on retention policies
type Cleanup struct {
	db               *sql.DB
	workflowStore    *models.WorkflowStore
	workflowRunStore *models.WorkflowRunStore
	retentionDays    int
	keepMin          int
	logger           *slog.Logger
	interval         time.Duration
}

// New creates a new cleanup component
func New(db *sql.DB, retentionDays int, keepMin int, logger *slog.Logger) (*Cleanup, error) {
	workflowStore, err := models.NewWorkflowStore(db)
	if err != nil {
		return nil, err
	}

	workflowRunStore, err := models.NewWorkflowRunStore(db)
	if err != nil {
		return nil, err
	}

	return &Cleanup{
		db:               db,
		workflowStore:    workflowStore,
		workflowRunStore: workflowRunStore,
		retentionDays:    retentionDays,
		keepMin:          keepMin,
		logger:           logger,
		interval:         6 * time.Hour, // Run every 6 hours
	}, nil
}

// Start runs the cleanup process periodically in the background
func (c *Cleanup) Start(ctx context.Context) error {
	// Run cleanup immediately on start
	if err := c.runCleanup(ctx); err != nil {
		c.logger.Error("Initial cleanup failed", "error", err)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Cleanup stopped")
			return nil
		case <-ticker.C:
			if err := c.runCleanup(ctx); err != nil {
				c.logger.Error("Cleanup failed", "error", err)
			}
		}
	}
}

// runCleanup performs the cleanup for all workflows
func (c *Cleanup) runCleanup(ctx context.Context) error {
	// Skip cleanup if retention is disabled
	if c.retentionDays == 0 {
		return nil
	}

	c.logger.Info("Starting cleanup", "retention_days", c.retentionDays, "keep_min", c.keepMin)

	// Get all workflows
	workflows, err := c.workflowStore.GetAll(ctx)
	if err != nil {
		return err
	}

	totalDeleted := 0
	for _, workflow := range workflows {
		deleted, err := c.cleanupWorkflow(ctx, workflow.ID)
		if err != nil {
			c.logger.Error("Failed to cleanup workflow", "workflow_id", workflow.ID, "workflow_name", workflow.Name, "error", err)
			continue
		}
		if deleted > 0 {
			c.logger.Info("Cleaned up workflow runs", "workflow_id", workflow.ID, "workflow_name", workflow.Name, "deleted", deleted)
			totalDeleted += deleted
		}
	}

	c.logger.Info("Cleanup completed", "total_deleted", totalDeleted)
	return nil
}

// cleanupWorkflow cleans up old runs for a specific workflow
func (c *Cleanup) cleanupWorkflow(ctx context.Context, workflowID int) (int, error) {
	// Get all runs for this workflow (ordered by started_at DESC, newest first)
	runs, err := c.workflowRunStore.GetAllRunsForWorkflow(ctx, workflowID)
	if err != nil {
		return 0, err
	}

	if len(runs) == 0 {
		return 0, nil
	}

	// Calculate the cutoff time
	cutoffTime := time.Now().AddDate(0, 0, -c.retentionDays)

	// Determine which runs to delete
	var runsToDelete []int
	for i, run := range runs {
		// Always keep the N most recent runs (keep-min)
		if i < c.keepMin {
			continue
		}

		// Only delete finished runs (success or failed)
		// Skip pending or running runs
		if run.Status != models.RunSuccess && run.Status != models.RunFailed {
			continue
		}

		// Use finished_at if available, otherwise use started_at
		timeToCheck := run.StartedAt
		if run.FinishedAt != nil {
			timeToCheck = *run.FinishedAt
		}

		// Delete if older than retention period
		if timeToCheck.Before(cutoffTime) {
			runsToDelete = append(runsToDelete, run.ID)
		}
	}

	if len(runsToDelete) == 0 {
		return 0, nil
	}

	// Delete the runs (transaction is handled inside DeleteRuns)
	err = c.workflowRunStore.DeleteRuns(ctx, runsToDelete)
	if err != nil {
		return 0, err
	}

	return len(runsToDelete), nil
}
