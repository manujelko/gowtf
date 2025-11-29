SELECT
    id,
    workflow_run_id,
    task_id,
    state,
    attempt,
    exit_code,
    started_at,
    finished_at,
    stdout_path,
    stderr_path
FROM task_instances
WHERE task_id = ? AND workflow_run_id = ?;















