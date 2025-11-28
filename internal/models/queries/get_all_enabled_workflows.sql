SELECT id, name, schedule, env, hash, enabled, updated_at
FROM workflows
WHERE enabled = 1
ORDER BY name;












