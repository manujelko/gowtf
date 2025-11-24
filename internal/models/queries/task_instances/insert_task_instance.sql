INSERT INTO task_instances (
    workflow_run_id,
    task_id,
    state,
    attempt,
    exit_code,
    started_at,
    finished_at,
    stdout_path,
    stderr_path
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
