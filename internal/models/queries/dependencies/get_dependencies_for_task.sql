SELECT depends_on_id
FROM task_dependencies
WHERE task_id = ?
ORDER BY depends_on_id;
