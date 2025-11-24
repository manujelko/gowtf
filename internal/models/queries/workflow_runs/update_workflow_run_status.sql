UPDATE workflow_runs
SET status = ?, finished_at = ?
WHERE id = ?;
