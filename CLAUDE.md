# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build -o gowtf ./cmd/gowtf

# Run (from repo root — needs ./workflows and ./output dirs)
./gowtf --watch-dir ./workflows --output-dir ./output

# Tests (all packages, with race detector)
go test -v -race ./...

# Run a single test
go test -v -run TestName ./internal/package/

# Format
go fmt ./...

# Vet
go vet ./...
```

## Architecture

`gowtf` is a single-binary workflow orchestration server. Five goroutines run concurrently and communicate via channels:

```
YAML files → Watcher → (watcherEvents chan) → Scheduler → (schedulerEvents chan) → Executor → WorkerPool
                                                                                        ↑
                                                                              HTTP Server (manual triggers)
```

**Watcher** (`internal/watcher/`) — uses `fsnotify` to detect YAML changes in `--watch-dir`. On change, parses the YAML, computes a SHA-256 hash, and upserts the workflow + tasks into SQLite. Emits `WorkflowEvent` (add/update/delete) on the `watcherEvents` channel. Tracks file→workflow mapping and parse errors for the UI.

**Scheduler** (`internal/scheduler/`) — listens to `watcherEvents` and manages a `robfig/cron` instance. When a cron fires (or a manual trigger is received via `TriggerWorkflow`), it creates a `WorkflowRun` row and one `TaskInstance` row per task (all in a retried transaction to handle SQLite busy), then sends a `WorkflowRunEvent` on `schedulerEvents`.

**Executor** (`internal/executor/`) — receives `WorkflowRunEvent`, spawns a goroutine per run. Each run loops: find pending tasks whose dependencies are terminal, evaluate conditions, skip or submit to the WorkerPool. Handles branching (reads stdout of branch tasks to determine which downstream tasks run), retries, and failure propagation. Updates task instance states as it goes.

**WorkerPool** (`internal/worker/`) — fixed-size goroutine pool. Executes tasks as `sh -c <script>` with merged workflow+task env. Captures stdout/stderr to files in `--output-dir`. Sends heartbeats to the health monitor. Returns `TaskResult` on a results channel.

**HealthMonitor** (`internal/health/`) — detects stuck/heartbeat-missing task instances and cancels their contexts.

**Server** (`internal/server/`) — serves HTML pages and a small REST API. Uses `html/template` with templates embedded at compile time from `ui/html/`. Middleware stack (innermost to outermost): requestID → logging → rateLimit → apiKey.

## Data layer

- SQLite via `modernc.org/sqlite` (pure Go, no CGo). WAL mode enabled for concurrent reads.
- Schema migrations live in `internal/db/migrations/` as numbered `.sql` files, applied in order at startup.
- Each model package (e.g. `internal/models/workflows.go`) loads its SQL queries at init time from embedded `.sql` files in `internal/models/queries/`. Stores are plain structs with a `*sql.DB` — no ORM.

## Workflow definition

Parsed by `internal/workflow/workflow.go` from YAML. Key fields:

- `branch: true` on a task — marks it as a branching task; its stdout (one task name per line) controls which downstream tasks execute.
- `condition` — trigger rule evaluated before a task runs; see `evaluateCondition` in `internal/executor/executor.go` for the full set (`all_upstream.success`, `any_upstream.success`, `task_name.success`, etc.).
- `retries` / `retry_delay` / `timeout` — handled by the executor/worker respectively.
- Env vars support `{{ var_name }}` interpolation and `{{ var | default:value }}` syntax (see `internal/workflow/env.go`).

## API routes

| Method | Path | Auth required |
|--------|------|---------------|
| GET | `/{$}` (dashboard) | No |
| GET | `/workflow/{id}` | No |
| GET | `/run/{id}` | No |
| POST | `/api/workflow/{id}/toggle` | Yes (if `--api-key` set) |
| POST | `/api/workflow/{id}/trigger` | Yes |
| GET | `/api/task-instance/{id}/logs` | Yes |
| GET | `/api/docs` | Yes |
| GET | `/health`, `/ready` | No |

API key passed via `X-API-Key` header or `?api_key=` query param.
