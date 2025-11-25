package models

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"
)

type TaskInstanceState int

const (
	TaskStatePending TaskInstanceState = iota
	TaskStateQueued
	TaskStateRunning
	TaskStateSkipped
	TaskStateSuccess
	TaskStateFailed
)

var taskInstanceStateNames = map[TaskInstanceState]string{
	TaskStatePending: "pending",
	TaskStateQueued:  "queued",
	TaskStateRunning: "running",
	TaskStateSkipped: "skipped",
	TaskStateSuccess: "success",
	TaskStateFailed:  "failed",
}

func (s TaskInstanceState) String() string {
	name, ok := taskInstanceStateNames[s]
	if !ok {
		panic("unknown TaskInstanceState value")
	}
	return name
}

func ParseTaskInstanceState(str string) (TaskInstanceState, error) {
	for k, v := range taskInstanceStateNames {
		if v == str {
			return k, nil
		}
	}
	return 0, fmt.Errorf("unknown TaskInstanceState: %q", str)
}

type TaskInstance struct {
	ID            int
	WorkflowRunID int
	TaskID        int
	State         TaskInstanceState
	Attempt       int
	ExitCode      *int
	StartedAt     *time.Time
	FinishedAt    *time.Time
	StdoutPath    *string
	StderrPath    *string
}

//go:embed queries/task_instances/*.sql
var taskInstanceQueries embed.FS

type TaskInstanceStore struct {
	DB               *sql.DB
	insertQ          string
	updateQ          string
	getForRunQ       string
	getByTaskAndRunQ string
	getByIDQ         string
}

func NewTaskInstanceStore(db *sql.DB) (*TaskInstanceStore, error) {
	insertQ, err := taskInstanceQueries.ReadFile("queries/task_instances/insert_task_instance.sql")
	if err != nil {
		return nil, err
	}
	updateQ, err := taskInstanceQueries.ReadFile("queries/task_instances/update_task_instance.sql")
	if err != nil {
		return nil, err
	}
	getForRunQ, err := taskInstanceQueries.ReadFile("queries/task_instances/get_for_run.sql")
	if err != nil {
		return nil, err
	}
	getByTaskAndRunQ, err := taskInstanceQueries.ReadFile("queries/task_instances/get_by_task_and_run.sql")
	if err != nil {
		return nil, err
	}
	getByIDQ, err := taskInstanceQueries.ReadFile("queries/task_instances/get_by_id.sql")
	if err != nil {
		return nil, err
	}

	return &TaskInstanceStore{
		DB:               db,
		insertQ:          string(insertQ),
		updateQ:          string(updateQ),
		getForRunQ:       string(getForRunQ),
		getByTaskAndRunQ: string(getByTaskAndRunQ),
		getByIDQ:         string(getByIDQ),
	}, nil
}

func (s *TaskInstanceStore) Insert(ctx context.Context, ti *TaskInstance) error {
	var (
		exitCode   interface{}
		startedAt  interface{}
		finishedAt interface{}
		stdoutPath interface{}
		stderrPath interface{}
	)

	if ti.ExitCode != nil {
		exitCode = *ti.ExitCode
	}
	if ti.StartedAt != nil {
		startedAt = *ti.StartedAt
	}
	if ti.FinishedAt != nil {
		finishedAt = *ti.FinishedAt
	}
	if ti.StdoutPath != nil {
		stdoutPath = *ti.StdoutPath
	}
	if ti.StderrPath != nil {
		stderrPath = *ti.StderrPath
	}

	res, err := s.DB.ExecContext(
		ctx,
		s.insertQ,
		ti.WorkflowRunID,
		ti.TaskID,
		ti.State.String(),
		ti.Attempt,
		exitCode,
		startedAt,
		finishedAt,
		stdoutPath,
		stderrPath,
	)
	if err != nil {
		return err
	}

	id64, err := res.LastInsertId()
	if err != nil {
		return err
	}
	ti.ID = int(id64)
	return nil
}

func (s *TaskInstanceStore) Update(ctx context.Context, ti *TaskInstance) error {
	var (
		exitCode   any
		startedAt  any
		finishedAt any
		stdoutPath any
		stderrPath any
	)

	if ti.ExitCode != nil {
		exitCode = *ti.ExitCode
	}
	if ti.StartedAt != nil {
		startedAt = *ti.StartedAt
	}
	if ti.FinishedAt != nil {
		finishedAt = *ti.FinishedAt
	}
	if ti.StdoutPath != nil {
		stdoutPath = *ti.StdoutPath
	}
	if ti.StderrPath != nil {
		stderrPath = *ti.StderrPath
	}

	_, err := s.DB.ExecContext(
		ctx,
		s.updateQ,
		ti.State.String(),
		ti.Attempt,
		exitCode,
		startedAt,
		finishedAt,
		stdoutPath,
		stderrPath,
		ti.ID,
	)
	return err
}

func (s *TaskInstanceStore) GetForRun(ctx context.Context, workflowRunID int) ([]*TaskInstance, error) {
	rows, err := s.DB.QueryContext(ctx, s.getForRunQ, workflowRunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*TaskInstance

	for rows.Next() {
		var (
			ti         TaskInstance
			stateStr   string
			exitCode   sql.NullInt64
			startedAt  sql.NullTime
			finishedAt sql.NullTime
			stdoutPath sql.NullString
			stderrPath sql.NullString
		)

		if err := rows.Scan(
			&ti.ID,
			&ti.WorkflowRunID,
			&ti.TaskID,
			&stateStr,
			&ti.Attempt,
			&exitCode,
			&startedAt,
			&finishedAt,
			&stdoutPath,
			&stderrPath,
		); err != nil {
			return nil, err
		}

		state, err := ParseTaskInstanceState(stateStr)
		if err != nil {
			return nil, err
		}
		ti.State = state

		if exitCode.Valid {
			v := int(exitCode.Int64)
			ti.ExitCode = &v
		}
		if startedAt.Valid {
			t := startedAt.Time
			ti.StartedAt = &t
		}
		if finishedAt.Valid {
			t := finishedAt.Time
			ti.FinishedAt = &t
		}
		if stdoutPath.Valid {
			s := stdoutPath.String
			ti.StdoutPath = &s
		}
		if stderrPath.Valid {
			s := stderrPath.String
			ti.StderrPath = &s
		}

		result = append(result, &ti)
	}

	return result, nil
}

func (s *TaskInstanceStore) GetByTaskIDAndRun(ctx context.Context, taskID, workflowRunID int) (*TaskInstance, error) {
	var (
		ti         TaskInstance
		stateStr   string
		exitCode   sql.NullInt64
		startedAt  sql.NullTime
		finishedAt sql.NullTime
		stdoutPath sql.NullString
		stderrPath sql.NullString
	)

	err := s.DB.QueryRowContext(ctx, s.getByTaskAndRunQ, taskID, workflowRunID).Scan(
		&ti.ID,
		&ti.WorkflowRunID,
		&ti.TaskID,
		&stateStr,
		&ti.Attempt,
		&exitCode,
		&startedAt,
		&finishedAt,
		&stdoutPath,
		&stderrPath,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	state, err := ParseTaskInstanceState(stateStr)
	if err != nil {
		return nil, err
	}
	ti.State = state

	if exitCode.Valid {
		v := int(exitCode.Int64)
		ti.ExitCode = &v
	}
	if startedAt.Valid {
		t := startedAt.Time
		ti.StartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		ti.FinishedAt = &t
	}
	if stdoutPath.Valid {
		s := stdoutPath.String
		ti.StdoutPath = &s
	}
	if stderrPath.Valid {
		s := stderrPath.String
		ti.StderrPath = &s
	}

	return &ti, nil
}

func (s *TaskInstanceStore) GetByID(ctx context.Context, id int) (*TaskInstance, error) {
	var (
		ti         TaskInstance
		stateStr   string
		exitCode   sql.NullInt64
		startedAt  sql.NullTime
		finishedAt sql.NullTime
		stdoutPath sql.NullString
		stderrPath sql.NullString
	)

	err := s.DB.QueryRowContext(ctx, s.getByIDQ, id).Scan(
		&ti.ID,
		&ti.WorkflowRunID,
		&ti.TaskID,
		&stateStr,
		&ti.Attempt,
		&exitCode,
		&startedAt,
		&finishedAt,
		&stdoutPath,
		&stderrPath,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	state, err := ParseTaskInstanceState(stateStr)
	if err != nil {
		return nil, err
	}
	ti.State = state

	if exitCode.Valid {
		v := int(exitCode.Int64)
		ti.ExitCode = &v
	}
	if startedAt.Valid {
		t := startedAt.Time
		ti.StartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		ti.FinishedAt = &t
	}
	if stdoutPath.Valid {
		s := stdoutPath.String
		ti.StdoutPath = &s
	}
	if stderrPath.Valid {
		s := stderrPath.String
		ti.StderrPath = &s
	}

	return &ti, nil
}
