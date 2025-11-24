UPDATE workflow_tasks
SET name = ?, script = ?, retries = ?, retry_delay = ?, timeout = ?, condition = ?, env = ?
WHERE id = ?;
