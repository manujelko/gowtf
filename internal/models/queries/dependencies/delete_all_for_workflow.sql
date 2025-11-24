DELETE FROM task_dependencies
WHERE task_id IN (
    SELECT id FROM workflow_tasks WHERE workflow_id = ?
);
