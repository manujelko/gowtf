package models

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"
)

type WorkflowRunStatus int

const (
	RunPending WorkflowRunStatus = iota
	RunRunning
	RunSuccess
	RunFailed
)

var workflowRunStatusNames = map[WorkflowRunStatus]string{
	RunPending: "pending",
	RunRunning: "running",
	RunSuccess: "success",
	RunFailed:  "failed",
}

func (s WorkflowRunStatus) String() string {
	name, ok := workflowRunStatusNames[s]
	if !ok {
		panic("unknown WorkflowRunStatus value")
	}
	return name
}

func ParseWorkflowRunStatus(str string) (WorkflowRunStatus, error) {
	for k, v := range workflowRunStatusNames {
		if v == str {
			return k, nil
		}
	}
	return 0, fmt.Errorf("unknown WorkflowRunStatus: %q", str)
}

type WorkflowRun struct {
	ID         int
	WorkflowID int
	Status     WorkflowRunStatus
	StartedAt  time.Time
	FinishedAt *time.Time
}

//go:embed queries/workflow_runs/*.sql
var workflowRunQueries embed.FS

type WorkflowRunStore struct {
	DB              *sql.DB
	insertQ         string
	updateStatusQ   string
	getByIDQ        string
	getLatestForWfQ string
	getRunsForWfQ   string
}

func NewWorkflowRunStore(db *sql.DB) (*WorkflowRunStore, error) {
	insertQ, err := workflowRunQueries.ReadFile("queries/workflow_runs/insert_workflow_run.sql")
	if err != nil {
		return nil, err
	}
	updateStatusQ, err := workflowRunQueries.ReadFile("queries/workflow_runs/update_workflow_run_status.sql")
	if err != nil {
		return nil, err
	}
	getByIDQ, err := workflowRunQueries.ReadFile("queries/workflow_runs/get_workflow_run.sql")
	if err != nil {
		return nil, err
	}
	getLatestForWfQ, err := workflowRunQueries.ReadFile("queries/workflow_runs/get_latest_for_workflow.sql")
	if err != nil {
		return nil, err
	}
	getRunsForWfQ, err := workflowRunQueries.ReadFile("queries/workflow_runs/get_runs_for_workflow.sql")
	if err != nil {
		return nil, err
	}

	return &WorkflowRunStore{
		DB:              db,
		insertQ:         string(insertQ),
		updateStatusQ:   string(updateStatusQ),
		getByIDQ:        string(getByIDQ),
		getLatestForWfQ: string(getLatestForWfQ),
		getRunsForWfQ:   string(getRunsForWfQ),
	}, nil
}

func (s *WorkflowRunStore) Insert(ctx context.Context, workflowID int, status WorkflowRunStatus) (*WorkflowRun, error) {
	var res sql.Result
	var id64 int64
	err := retryDBOperation(5, func() error {
		var execErr error
		res, execErr = s.DB.ExecContext(ctx, s.insertQ, workflowID, status.String())
		if execErr != nil {
			return execErr
		}

		id64, execErr = res.LastInsertId()
		return execErr
	})
	if err != nil {
		return nil, err
	}

	return s.GetByID(ctx, int(id64))
}

func (s *WorkflowRunStore) GetByID(ctx context.Context, id int) (*WorkflowRun, error) {
	var (
		wr        WorkflowRun
		statusStr string
		finished  sql.NullTime
	)

	err := s.DB.QueryRowContext(ctx, s.getByIDQ, id).Scan(
		&wr.ID,
		&wr.WorkflowID,
		&statusStr,
		&wr.StartedAt,
		&finished,
	)
	if err != nil {
		return nil, err
	}

	status, err := ParseWorkflowRunStatus(statusStr)
	if err != nil {
		return nil, err
	}
	wr.Status = status

	if finished.Valid {
		t := finished.Time
		wr.FinishedAt = &t
	}

	return &wr, nil
}

func (s *WorkflowRunStore) GetLatestForWorkflow(ctx context.Context, workflowID int) (*WorkflowRun, error) {
	var (
		wr        WorkflowRun
		statusStr string
		finished  sql.NullTime
	)

	err := s.DB.QueryRowContext(ctx, s.getLatestForWfQ, workflowID).Scan(
		&wr.ID,
		&wr.WorkflowID,
		&statusStr,
		&wr.StartedAt,
		&finished,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	status, err := ParseWorkflowRunStatus(statusStr)
	if err != nil {
		return nil, err
	}
	wr.Status = status

	if finished.Valid {
		t := finished.Time
		wr.FinishedAt = &t
	}

	return &wr, nil
}

func (s *WorkflowRunStore) GetRunsForWorkflow(ctx context.Context, workflowID int, limit int) ([]*WorkflowRun, error) {
	rows, err := s.DB.QueryContext(ctx, s.getRunsForWfQ, workflowID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*WorkflowRun
	for rows.Next() {
		var (
			wr        WorkflowRun
			statusStr string
			finished  sql.NullTime
		)

		err := rows.Scan(
			&wr.ID,
			&wr.WorkflowID,
			&statusStr,
			&wr.StartedAt,
			&finished,
		)
		if err != nil {
			return nil, err
		}

		status, err := ParseWorkflowRunStatus(statusStr)
		if err != nil {
			return nil, err
		}
		wr.Status = status

		if finished.Valid {
			t := finished.Time
			wr.FinishedAt = &t
		}

		runs = append(runs, &wr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func (s *WorkflowRunStore) UpdateStatus(ctx context.Context, id int, status WorkflowRunStatus, finishedAt *time.Time) error {
	var finishedAny any
	if finishedAt != nil {
		finishedAny = *finishedAt
	} else {
		finishedAny = nil
	}

	return retryDBOperation(5, func() error {
		_, err := s.DB.ExecContext(ctx, s.updateStatusQ, status.String(), finishedAny, id)
		return err
	})
}
