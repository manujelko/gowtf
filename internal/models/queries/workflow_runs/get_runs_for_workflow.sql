SELECT id, workflow_id, status, started_at, finished_at
FROM workflow_runs
WHERE workflow_id = ?
ORDER BY started_at DESC
LIMIT ?;


