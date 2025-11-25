package models

import (
	"context"
	"database/sql"
	"embed"
)

//go:embed queries/tasks/*.sql
var taskQueries embed.FS

type WorkflowTask struct {
	ID         int
	WorkflowID int
	Name       string
	Script     string
	Retries    int
	RetryDelay string
	Timeout    string
	Condition  string
	Env        map[string]string
}

type WorkflowTaskStore struct {
	DB              *sql.DB
	insertQ         string
	getForWfQ       string
	getByNameForWfQ string
	updateQ         string
	deleteQ         string
	deleteForWfQ    string
}

func NewWorkflowTaskStore(db *sql.DB) (*WorkflowTaskStore, error) {
	insertQ, err := taskQueries.ReadFile("queries/tasks/insert_task.sql")
	if err != nil {
		return nil, err
	}
	getQ, err := taskQueries.ReadFile("queries/tasks/get_tasks_for_workflow.sql")
	if err != nil {
		return nil, err
	}
	getByNameQ, err := taskQueries.ReadFile("queries/tasks/get_by_name_for_workflow.sql")
	if err != nil {
		return nil, err
	}
	updateQ, err := taskQueries.ReadFile("queries/tasks/update_task.sql")
	if err != nil {
		return nil, err
	}
	deleteQ, err := taskQueries.ReadFile("queries/tasks/delete_task.sql")
	if err != nil {
		return nil, err
	}
	deleteForWfQ, err := taskQueries.ReadFile("queries/tasks/delete_all_for_workflow.sql")
	if err != nil {
		return nil, err
	}

	return &WorkflowTaskStore{
		DB:              db,
		insertQ:         string(insertQ),
		getForWfQ:       string(getQ),
		getByNameForWfQ: string(getByNameQ),
		updateQ:         string(updateQ),
		deleteQ:         string(deleteQ),
		deleteForWfQ:    string(deleteForWfQ),
	}, nil
}

func (s *WorkflowTaskStore) Insert(ctx context.Context, t *WorkflowTask) error {
	envJSON, err := encodeEnv(t.Env)
	if err != nil {
		return err
	}

	res, err := s.DB.ExecContext(ctx, s.insertQ,
		t.WorkflowID,
		t.Name,
		t.Script,
		t.Retries,
		t.RetryDelay,
		t.Timeout,
		t.Condition,
		envJSON,
	)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	t.ID = int(id)
	return nil
}

func (s *WorkflowTaskStore) GetForWorkflow(ctx context.Context, workflowID int) ([]*WorkflowTask, error) {
	rows, err := s.DB.QueryContext(ctx, s.getForWfQ, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*WorkflowTask

	for rows.Next() {
		var (
			t          WorkflowTask
			retryDelay sql.NullString
			timeout    sql.NullString
			condition  sql.NullString
			envJSON    sql.NullString
		)

		err := rows.Scan(
			&t.ID,
			&t.WorkflowID,
			&t.Name,
			&t.Script,
			&t.Retries,
			&retryDelay,
			&timeout,
			&condition,
			&envJSON,
		)
		if err != nil {
			return nil, err
		}

		if retryDelay.Valid {
			t.RetryDelay = retryDelay.String
		}
		if timeout.Valid {
			t.Timeout = timeout.String
		}
		if condition.Valid {
			t.Condition = condition.String
		}

		t.Env, err = decodeEnv(envJSON)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, &t)
	}

	return tasks, nil
}

func (s *WorkflowTaskStore) GetByNameForWorkflow(ctx context.Context, workflowID int, taskName string) (*WorkflowTask, error) {
	var (
		t          WorkflowTask
		retryDelay sql.NullString
		timeout    sql.NullString
		condition  sql.NullString
		envJSON    sql.NullString
	)

	err := s.DB.QueryRowContext(ctx, s.getByNameForWfQ, workflowID, taskName).Scan(
		&t.ID,
		&t.WorkflowID,
		&t.Name,
		&t.Script,
		&t.Retries,
		&retryDelay,
		&timeout,
		&condition,
		&envJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if retryDelay.Valid {
		t.RetryDelay = retryDelay.String
	}
	if timeout.Valid {
		t.Timeout = timeout.String
	}
	if condition.Valid {
		t.Condition = condition.String
	}

	t.Env, err = decodeEnv(envJSON)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (s *WorkflowTaskStore) Update(ctx context.Context, t *WorkflowTask) error {
	envJSON, err := encodeEnv(t.Env)
	if err != nil {
		return err
	}

	_, err = s.DB.ExecContext(ctx, s.updateQ,
		t.Name,
		t.Script,
		t.Retries,
		t.RetryDelay,
		t.Timeout,
		t.Condition,
		envJSON,
		t.ID,
	)
	return err
}

func (s *WorkflowTaskStore) Delete(ctx context.Context, id int) error {
	_, err := s.DB.ExecContext(ctx, s.deleteQ, id)
	return err
}

func (s *WorkflowTaskStore) DeleteForWorkflow(ctx context.Context, workflowID int) error {
	_, err := s.DB.ExecContext(ctx, s.deleteForWfQ, workflowID)
	return err
}

// InsertTx inserts a task within a transaction
func (s *WorkflowTaskStore) InsertTx(ctx context.Context, tx *sql.Tx, t *WorkflowTask) error {
	envJSON, err := encodeEnv(t.Env)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, s.insertQ,
		t.WorkflowID,
		t.Name,
		t.Script,
		t.Retries,
		t.RetryDelay,
		t.Timeout,
		t.Condition,
		envJSON,
	)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	t.ID = int(id)
	return nil
}

// DeleteForWorkflowTx deletes all tasks for a workflow within a transaction
func (s *WorkflowTaskStore) DeleteForWorkflowTx(ctx context.Context, tx *sql.Tx, workflowID int) error {
	_, err := tx.ExecContext(ctx, s.deleteForWfQ, workflowID)
	return err
}
