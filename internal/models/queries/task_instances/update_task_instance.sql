UPDATE task_instances
SET
    state = ?,
    attempt = ?,
    exit_code = ?,
    started_at = ?,
    finished_at = ?,
    stdout_path = ?,
    stderr_path = ?
WHERE ID = ?;
