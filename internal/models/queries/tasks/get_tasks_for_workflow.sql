SELECT id, workflow_id, name, script, retries, retry_delay, timeout, condition, env, branch
FROM workflow_tasks
WHERE workflow_id = ?
ORDER BY id;
