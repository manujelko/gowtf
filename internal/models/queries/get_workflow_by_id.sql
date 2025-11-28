SELECT id, name, schedule, env, hash, enabled, updated_at
FROM workflows
WHERE id = ?
LIMIT 1;












