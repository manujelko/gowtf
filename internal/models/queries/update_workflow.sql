UPDATE workflows
SET name = ?, schedule = ?, env = ?, hash = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
WHERE id = ?;
