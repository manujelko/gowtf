SELECT id, workflow_id, name, script, retries, retry_delay, timeout, condition, env
FROM workflow_tasks
WHERE workflow_id = ? AND name = ?;











