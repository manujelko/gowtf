-- Track applied migrations
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);

-- Workflows definition (one row per YAML file)
CREATE TABLE IF NOT EXISTS workflows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    schedule TEXT NOT NULL,
    env TEXT,            -- stored as JSON
    hash TEXT NOT NULL,  -- workflow definition hash
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_workflows_enabled ON workflows(enabled);

-- Tasks belonging to a workflow
CREATE TABLE IF NOT EXISTS workflow_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    script TEXT NOT NULL,
    retries INTEGER NOT NULL DEFAULT 0,
    retry_delay TEXT,
    timeout TEXT,
    condition TEXT,
    env TEXT,  -- JSON
    UNIQUE(workflow_id, name)
);

-- Dependencies between tasks
CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id INTEGER NOT NULL REFERENCES workflow_tasks(id) ON DELETE CASCADE,
    depends_on_id INTEGER NOT NULL REFERENCES workflow_tasks(id) ON DELETE CASCADE,
    PRIMARY KEY(task_id, depends_on_id)
);

-- Workflow run instances (scheduler creates these)
CREATE TABLE IF NOT EXISTS workflow_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id INTEGER NOT NULL REFERENCES workflows(id),
    status TEXT NOT NULL,  -- pending, running, success, failed
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME
);

-- Task instances (executor manages these)
CREATE TABLE IF NOT EXISTS task_instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_run_id INTEGER NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_id INTEGER NOT NULL REFERENCES workflow_tasks(id),
    state TEXT NOT NULL, -- pending, queued, running, skipped, success
    attempt INTEGER NOT NULL DEFAULT 1,
    exit_code INTEGER,
    started_at DATETIME,
    finished_at DATETIME,
    stdout_path TEXT,
    stderr_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_instances_run ON task_instances(workflow_run_id);
