# gowtf

`gowtf` is a simple orchestration framework. The acronym stands for either:

1. Go with the flow when things are working as expected.
2. Go what the f*ck when things go wrong.

## Architecture

1. **Watcher**

Responsibilities:

* Monitor a directory for workflow YAML changes.
* Parse YAML into workflow objects.
* Validate tasks + dependencies + conditions.
* Compute a hash/version for the workflow definition.
* Write workflow metadata + tasks + dependencies into SQLite.
* Notify the scheduler when a workflow is added/udpated/deleted.

Important:

* The watcher does not schedule runs.
* It simply keeps the DB in sync with the filesystem.
* It also ensures the UI has the exact workflow definition.

2. **Scheduler**

Responsibilities:

* Load workflow definitions from SQLite on startup.
* Maintain in-memory workflow DAG structs.
* Create timers/tickers from workflow schedules (cron).
* When a schedule fires:
    * Insert a new row into `workflow_runs`
    * Insert rows for `task_instances` (set to pending)
    * Notify the executor: "A new workflow run is ready"

Important:

* Scheduler handles *when to run* a workflow.
* It does NOT handle task-level orchestration.
* It does NOT manage conditionals.
* It does NOT run tasks.
* It does NOT track task states beyond initial creation.

3. **Executor**

This is the most important component.

Responsibilities:

* Maintain a queue of runnable tasks.
* Receive on a channel from scheduler to start executing work.
* Check dependencies for each task.
* Evaluate conditions based on upstream tasks' exit codes.
* Transition task state from pending to queued.
* Send runnable tasks to the worker pool.
* List for worker completion events.
* Mark tasks as skipped when conditions fail.
* Manage backpressure and concurrency limits.

Important:

* The executor is the only component that understand the graph logic.
* It is the only place that evaluates exit codes and conditions.
* It is the only place that decides task ordering.
* It waits for worker results and decides the next steps.

This is your orchestrator's brain.

4. **Worker Pool**

Responsibilities:

* Receive tasks from executor.
* Run shell commands
* Capture stdout, stderr and exit code.
* Write task results into SQLite.
* Transition task state from queued to running, success and failed.
* Send completion messages back to executor.
* Send heartbeat to health monitor.
* Writes logs to disk.

Important:

* Workers do NOT know ANYTHING about dependencies.
* Workers do NOT know what runs next.
* Workers do NOT evaluate conditions.
* Workers do NOT orchestrate.

5. **Health Monitor**

Responsibilities:

* Track heartbeat messages from each worker gorotuine.
* If a worker stops sending heartbeats:
    * Mark task as failed or cancelled.
    * Executor handles retry logic.
* Cancels worker contexts (kills subprocess).

Important:

* You cancel contexts, which kills subprocesses.

6. **Server**

Responsibilities:

* Serve UI.
* Read workflow definitions from SQLite.
* Read workflow runs from SQLite.
* Read task instances from SQLite.
* Read task dependencies from SQLite.
* Show graph structure using stored dependency tables.
* Show logs from disk.
* Allow enabling/disabling workflows.
* Allow triggering manual runs.
* Provide REST endpoints for external systems.

Important:

* UI must not rely on in-memory workflow objects.
* SQLite is the single source of truth for the UI.
