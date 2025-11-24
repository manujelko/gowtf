package models

import (
	"context"
	"database/sql"
	"embed"
)

//go:embed queries/dependencies/*.sql
var depQueries embed.FS

type TaskDependency struct {
	TaskID    int
	DependsOn int
}

type TaskDependenciesStore struct {
	DB           *sql.DB
	insertQ      string
	getForTaskQ  string
	deleteQ      string
	deleteForWfQ string
}

func NewTaskDependenciesStore(db *sql.DB) (*TaskDependenciesStore, error) {
	insertQ, _ := depQueries.ReadFile("queries/dependencies/insert_dependency.sql")
	getQ, _ := depQueries.ReadFile("queries/dependencies/get_dependencies_for_task.sql")
	delQ, _ := depQueries.ReadFile("queries/dependencies/delete_dependency.sql")
	delWfQ, _ := depQueries.ReadFile("queries/dependencies/delete_all_for_workflow.sql")

	return &TaskDependenciesStore{
		DB:           db,
		insertQ:      string(insertQ),
		getForTaskQ:  string(getQ),
		deleteQ:      string(delQ),
		deleteForWfQ: string(delWfQ),
	}, nil
}

func (s *TaskDependenciesStore) Insert(ctx context.Context, taskID, dependsOn int) error {
	_, err := s.DB.ExecContext(ctx, s.insertQ, taskID, dependsOn)
	return err
}

func (s *TaskDependenciesStore) GetForTask(ctx context.Context, taskID int) ([]int, error) {
	rows, err := s.DB.QueryContext(ctx, s.getForTaskQ, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		deps = append(deps, id)
	}

	return deps, nil
}

func (s *TaskDependenciesStore) Delete(ctx context.Context, taskID, dependsOn int) error {
	_, err := s.DB.ExecContext(ctx, s.deleteQ, taskID, dependsOn)
	return err
}

func (s *TaskDependenciesStore) DeleteForWorkflow(ctx context.Context, workflowID int) error {
	_, err := s.DB.ExecContext(ctx, s.deleteForWfQ, workflowID)
	return err
}
