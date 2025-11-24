SELECT id, workflow_id, status, started_at, finished_at
FROM workflow_runs
WHERE id = ?;
