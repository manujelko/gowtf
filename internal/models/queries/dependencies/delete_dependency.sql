DELETE FROM task_dependencies
WHERE task_id = ? AND depends_on_id = ?;
