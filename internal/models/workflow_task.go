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
	DB        *sql.DB
	insertQ   string
	getForWfQ string
	updateQ   string
	deleteQ   string
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
	updateQ, err := taskQueries.ReadFile("queries/tasks/update_task.sql")
	if err != nil {
		return nil, err
	}
	deleteQ, err := taskQueries.ReadFile("queries/tasks/delete_task.sql")
	if err != nil {
		return nil, err
	}

	return &WorkflowTaskStore{
		DB:        db,
		insertQ:   string(insertQ),
		getForWfQ: string(getQ),
		updateQ:   string(updateQ),
		deleteQ:   string(deleteQ),
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
			t       WorkflowTask
			envJSON sql.NullString
		)

		err := rows.Scan(
			&t.ID,
			&t.WorkflowID,
			&t.Name,
			&t.Script,
			&t.Retries,
			&t.RetryDelay,
			&t.Timeout,
			&t.Condition,
			&envJSON,
		)
		if err != nil {
			return nil, err
		}

		t.Env, err = decodeEnv(envJSON)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, &t)
	}

	return tasks, nil
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
