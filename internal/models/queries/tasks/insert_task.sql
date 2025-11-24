INSERT INTO workflow_tasks (
    workflow_id, name, script, retries, retry_delay, timeout, condition, env
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

