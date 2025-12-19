-- Add branch column to workflow_tasks table
ALTER TABLE workflow_tasks ADD COLUMN branch INTEGER NOT NULL DEFAULT 0;


