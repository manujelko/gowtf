package models

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"time"
)

//go:embed queries/*.sql
var workflowQueries embed.FS

type Workflow struct {
	ID        int
	Name      string
	Schedule  string
	Env       map[string]string
	Hash      string
	Enabled   bool
	UpdatedAt time.Time
}

type WorkflowStore struct {
	DB      *sql.DB
	insertQ string
	getByQ  string
	updateQ string
	deleteQ string
}

func NewWorkflowStore(db *sql.DB) (*WorkflowStore, error) {
	insertQ, err := workflowQueries.ReadFile("queries/insert_workflow.sql")
	if err != nil {
		return nil, err
	}
	getByQ, err := workflowQueries.ReadFile("queries/get_workflow_by_name.sql")
	if err != nil {
		return nil, err
	}
	updateQ, err := workflowQueries.ReadFile("queries/update_workflow.sql")
	if err != nil {
		return nil, err
	}
	deleteQ, err := workflowQueries.ReadFile("queries/delete_workflow.sql")
	if err != nil {
		return nil, err
	}

	return &WorkflowStore{
		DB:      db,
		insertQ: string(insertQ),
		getByQ:  string(getByQ),
		updateQ: string(updateQ),
		deleteQ: string(deleteQ),
	}, nil
}

func (s *WorkflowStore) Insert(ctx context.Context, w *Workflow) error {
	envJSON, err := encodeEnv(w.Env)
	if err != nil {
		return err
	}

	res, err := s.DB.ExecContext(
		ctx,
		s.insertQ,
		w.Name,
		w.Schedule,
		envJSON,
		w.Hash,
		boolToInt(w.Enabled),
	)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	w.ID = int(id)
	return nil
}

func (s *WorkflowStore) GetByName(ctx context.Context, name string) (*Workflow, error) {
	var (
		w       Workflow
		envJSON sql.NullString
		enabled int
	)

	err := s.DB.QueryRowContext(ctx, s.getByQ, name).Scan(
		&w.ID,
		&w.Name,
		&w.Schedule,
		&envJSON,
		&w.Hash,
		&enabled,
		&w.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	w.Enabled = enabled == 1
	w.Env, err = decodeEnv(envJSON)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (s *WorkflowStore) Update(ctx context.Context, w *Workflow) error {
	if w.ID == 0 {
		return errors.New("cannot update workflow without ID")
	}

	envJSON, err := encodeEnv(w.Env)
	if err != nil {
		return err
	}

	_, err = s.DB.ExecContext(
		ctx,
		s.updateQ,
		w.Name,
		w.Schedule,
		envJSON,
		w.Hash,
		boolToInt(w.Enabled),
		w.ID,
	)
	return err
}

func (s *WorkflowStore) Delete(ctx context.Context, id int) error {
	_, err := s.DB.ExecContext(ctx, s.deleteQ, id)
	return err
}

// InsertTx inserts a workflow within a transaction
func (s *WorkflowStore) InsertTx(ctx context.Context, tx *sql.Tx, w *Workflow) error {
	envJSON, err := encodeEnv(w.Env)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(
		ctx,
		s.insertQ,
		w.Name,
		w.Schedule,
		envJSON,
		w.Hash,
		boolToInt(w.Enabled),
	)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return err
	}

	w.ID = int(id)
	return nil
}

// UpdateTx updates a workflow within a transaction
func (s *WorkflowStore) UpdateTx(ctx context.Context, tx *sql.Tx, w *Workflow) error {
	if w.ID == 0 {
		return errors.New("cannot update workflow without ID")
	}

	envJSON, err := encodeEnv(w.Env)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(
		ctx,
		s.updateQ,
		w.Name,
		w.Schedule,
		envJSON,
		w.Hash,
		boolToInt(w.Enabled),
		w.ID,
	)
	return err
}
