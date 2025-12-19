# Go With The Flow

`gowtf` is a simple workflow orchestration framework that allows you to define workflows as YAML files, schedule them with cron expressions, and execute tasks with dependencies, conditions, retries, and more. It includes a web UI for monitoring and managing your workflows.

## Installation

### Using Go Install

The easiest way to install `gowtf` is using `go install`:

```bash
go install github.com/manujelko/gowtf/cmd/gowtf@latest
```

Make sure your `$GOPATH/bin` or `$GOBIN` is in your `$PATH` to run the `gowtf` command directly.

### Building from Source

If you prefer to build from source:

```bash
git clone https://github.com/manujelko/gowtf.git
cd gowtf
go build -o gowtf ./cmd/gowtf
```

## Quick Start

1. **Create a workflows directory** (or use the default `./workflows`):

```bash
mkdir workflows
```

2. **Create your first workflow** (`workflows/hello.yaml`):

```yaml
name: hello
schedule: "*/5 * * * *"  # Run every 5 minutes

tasks:
  - name: greet
    script: |
      echo "Hello from gowtf!"
      echo "Current time: $(date)"
  
  - name: process
    depends_on: [greet]
    script: |
      echo "Processing data..."
      sleep 2
      echo "Done!"
```

3. **Start gowtf**:

```bash
gowtf
```

4. **Open the web UI**:

Navigate to `http://localhost:8080` in your browser to see your workflows, monitor runs, and view logs.

## Command-Line Options

```bash
gowtf [options]
```

### Available Flags

- `--db`: Database file path (default: `./gowtf.db`)
- `--watch-dir`: Directory to watch for workflow YAML files (default: `./workflows`)
- `--output-dir`: Directory for task output/logs (default: `./output`)
- `--workers`: Worker pool size (default: `4`)
- `--http-addr`: HTTP server address (default: `:8080`)
- `--api-key`: API key for protecting API endpoints (optional, but recommended for production)
- `--rate-limit`: Rate limit for API endpoints in requests per minute (default: `60`, set to `0` to disable)
- `--retention-days`: Number of days to retain workflow runs (default: `30`, set to `0` to disable cleanup)
- `--retention-keep-min`: Always keep at least N most recent runs per workflow (default: `10`)

### Example with Custom Options

```bash
gowtf \
  --watch-dir /path/to/workflows \
  --output-dir /var/log/gowtf \
  --workers 8 \
  --http-addr :9090 \
  --api-key my-secret-key \
  --retention-days 60
```

## Workflow Definition

Workflows are defined in YAML files. Each workflow file should contain:

- `name`: Unique workflow name
- `schedule`: Cron expression (optional - empty string means manual-only)
- `env`: Environment variables (optional, workflow-level)
- `tasks`: List of tasks to execute

### Basic Workflow Structure

```yaml
name: my_workflow
schedule: "0 * * * *"  # Every hour at minute 0

env:
  ENVIRONMENT: "production"
  API_URL: "https://api.example.com"

tasks:
  - name: task1
    script: |
      echo "Task 1 running"
      # Your shell commands here
  
  - name: task2
    depends_on: [task1]
    script: |
      echo "Task 2 running after task1"
```

## Workflow Examples

### Example 1: Simple Sequential Tasks

```yaml
name: data_pipeline
schedule: "0 2 * * *"  # Daily at 2 AM

tasks:
  - name: extract
    script: |
      echo "Extracting data..."
      # Your extraction logic
  
  - name: transform
    depends_on: [extract]
    script: |
      echo "Transforming data..."
      # Your transformation logic
  
  - name: load
    depends_on: [transform]
    script: |
      echo "Loading data..."
      # Your loading logic
```

### Example 2: Parallel Execution

```yaml
name: parallel_processing
schedule: "*/10 * * * *"

tasks:
  - name: setup
    script: |
      echo "Setting up environment..."
  
  - name: process_a
    depends_on: [setup]
    script: |
      echo "Processing A..."
      sleep 5
  
  - name: process_b
    depends_on: [setup]
    script: |
      echo "Processing B..."
      sleep 5
  
  - name: merge
    depends_on: [process_a, process_b]
    script: |
      echo "Merging results..."
```

### Example 3: Dynamic Branching

Branching allows you to dynamically choose which path to take based on runtime conditions. Branching tasks output task names to stdout, and only those tasks (and their downstream dependencies) will execute.

```yaml
name: branching_workflow
schedule: "0 * * * *"

tasks:
  - name: initialize
    script: |
      echo "Initializing workflow..."
  
  # Branching task: outputs which path to take
  - name: make_decision
    depends_on: [initialize]
    branch: true  # Mark as branching task
    script: |
      echo "Making decision..."
      # Output task names to stdout (one per line)
      # Only tasks listed here will execute
      if [ "$(date +%H)" -lt 12 ]; then
        echo "morning_path_task1"
      else
        echo "afternoon_path_task1"
      fi
      # Task must exit with code 0 (success)
      exit 0
  
  # Path A: Morning path
  - name: morning_path_task1
    depends_on: [make_decision]
    script: |
      echo "Executing morning path..."
  
  - name: morning_path_task2
    depends_on: [morning_path_task1]
    script: |
      echo "Morning path complete"
  
  # Path B: Afternoon path
  - name: afternoon_path_task1
    depends_on: [make_decision]
    script: |
      echo "Executing afternoon path..."
  
  - name: afternoon_path_task2
    depends_on: [afternoon_path_task1]
    script: |
      echo "Afternoon path complete"
  
  # Merge point: Run if either path completes
  - name: finalize
    depends_on: [morning_path_task2, afternoon_path_task2]
    condition: any_upstream.success
    script: |
      echo "Workflow complete!"
```

**Key points about branching:**
- Branching tasks **always succeed** (exit code 0) - branching is controlled by output, not exit code
- Output task names to stdout, one per line
- Tasks not in the output are automatically skipped (along with their entire downstream branch)
- You can output multiple task names for parallel branches
- Use trigger rules (conditions) on merge points to handle multiple branches

### Example 4: Conditional Execution (Legacy)

You can still use conditions based on task success/failure:

```yaml
name: conditional_workflow
schedule: "0 * * * *"

tasks:
  - name: check_status
    script: |
      # This task might succeed or fail
      if [ "$RANDOM" -gt 16383 ]; then
        echo "Status check passed"
        exit 0
      else
        echo "Status check failed"
        exit 1
      fi
  
  - name: approved_path
    depends_on: [check_status]
    condition: check_status.success
    script: |
      echo "Processing approved status..."
  
  - name: rejected_path
    depends_on: [check_status]
    condition: check_status.failed
    script: |
      echo "Processing rejected status..."
  
  - name: finalize
    depends_on: [approved_path, rejected_path]
    condition: any_upstream.success
    script: |
      echo "Finalizing workflow..."
```

### Example 5: Task with Retries

```yaml
name: retry_demo
schedule: "*/15 * * * *"

tasks:
  - name: unreliable_task
    retries: 3
    retry_delay: 10s
    timeout: 30s
    script: |
      echo "Attempting unreliable operation..."
      # This might fail, but will retry up to 3 times
      if [ "$RANDOM" -gt 20000 ]; then
        echo "Success!"
        exit 0
      else
        echo "Failed, will retry..."
        exit 1
      fi
  
  - name: continue_after_retry
    depends_on: [unreliable_task]
    script: |
      echo "Continuing after unreliable task completed..."
```

### Example 6: Manual-Only Workflow

```yaml
name: manual_backup
schedule: ""  # Empty schedule = manual-only

tasks:
  - name: backup_database
    script: |
      echo "Backing up database..."
      # Your backup logic
  
  - name: verify_backup
    depends_on: [backup_database]
    script: |
      echo "Verifying backup..."
      # Your verification logic
```

### Example 7: Environment Variables

```yaml
name: env_demo
schedule: "0 * * * *"

env:
  WORKFLOW_ENV: "production"
  SHARED_KEY: "{{ shared_key }}"

tasks:
  - name: task_with_env
    env:
      TASK_ENV: "task-level"
      API_KEY: "{{ api_key }}"
    script: |
      echo "Workflow env: $WORKFLOW_ENV"
      echo "Task env: $TASK_ENV"
      echo "Shared key: $SHARED_KEY"
      echo "API key: $API_KEY"
```

## Tutorial: Building Your First Workflow

Let's build a complete workflow step by step.

### Step 1: Create a Simple Workflow

Create `workflows/tutorial.yaml`:

```yaml
name: tutorial
schedule: ""  # We'll start with manual-only

tasks:
  - name: step1
    script: |
      echo "Step 1: Gathering data"
      date > /tmp/tutorial_data.txt
      echo "Data gathered successfully"
```

### Step 2: Add Dependencies

Add a second task that depends on the first:

```yaml
name: tutorial
schedule: ""

tasks:
  - name: step1
    script: |
      echo "Step 1: Gathering data"
      date > /tmp/tutorial_data.txt
      echo "Data gathered successfully"
  
  - name: step2
    depends_on: [step1]
    script: |
      echo "Step 2: Processing data"
      cat /tmp/tutorial_data.txt
      echo "Processing complete"
```

### Step 3: Add Conditional Logic

Add a task that only runs if step2 succeeds:

```yaml
name: tutorial
schedule: ""

tasks:
  - name: step1
    script: |
      echo "Step 1: Gathering data"
      date > /tmp/tutorial_data.txt
      echo "Data gathered successfully"
  
  - name: step2
    depends_on: [step1]
    script: |
      echo "Step 2: Processing data"
      cat /tmp/tutorial_data.txt
      echo "Processing complete"
  
  - name: step3
    depends_on: [step2]
    condition: step2.success
    script: |
      echo "Step 3: Finalizing (only if step2 succeeded)"
      echo "Tutorial workflow complete!"
```

### Step 4: Add Scheduling

Change the schedule to run automatically:

```yaml
name: tutorial
schedule: "*/10 * * * *"  # Every 10 minutes

tasks:
  # ... tasks remain the same
```

### Step 5: Enable the Workflow

1. Start gowtf: `gowtf`
2. Open `http://localhost:8080`
3. Find your workflow in the list
4. Click the toggle button to enable it

The workflow will now run automatically according to its schedule!

## Task Configuration

Each task can have the following properties:

### Required Fields

- `name`: Unique task name within the workflow
- `script`: Shell script to execute (runs in `sh -c`)

### Optional Fields

- `depends_on`: List of task names that must complete before this task runs
- `branch`: Set to `true` to mark this as a branching task. Branching tasks output task names to stdout to determine which downstream tasks execute. Branching tasks cannot have conditions.
- `condition`: Trigger rule to evaluate before running (also called "trigger rules"):
  - `all_upstream.success`: All dependencies succeeded
  - `any_upstream.success`: At least one dependency succeeded (ignores skipped tasks)
  - `all_done`: All dependencies are in terminal state (success, failed, or skipped)
  - `none_failed`: No dependencies failed (success or skipped are OK)
  - `all_success_or_skipped`: All dependencies either succeeded or were skipped
  - `any_upstream.failed`: At least one dependency failed
  - `task_name.success`: Specific task succeeded
  - `task_name.failed`: Specific task failed
- `retries`: Number of retry attempts (default: 0)
- `retry_delay`: Delay between retries (e.g., `10s`, `1m`, default: `30s`)
- `timeout`: Maximum execution time (e.g., `30s`, `5m`, `1h`, default: `1h`)
- `env`: Task-level environment variables (overrides workflow-level)

## Environment Variables

### Workflow-Level Environment Variables

Set environment variables at the workflow level:

```yaml
name: my_workflow
env:
  API_URL: "https://api.example.com"
  ENVIRONMENT: "production"
```

### Task-Level Environment Variables

Task-level variables override workflow-level:

```yaml
tasks:
  - name: my_task
    env:
      API_KEY: "secret-key"
      DEBUG: "true"
    script: |
      echo "API URL: $API_URL"  # From workflow level
      echo "API Key: $API_KEY"  # From task level
```

### Environment Variable References

You can reference other environment variables using `{{ variable_name }}`:

```yaml
env:
  BASE_URL: "https://api.example.com"
  API_URL: "{{ BASE_URL }}/v1"
  DEFAULT_TIMEOUT: "30s"

tasks:
  - name: task1
    env:
      TIMEOUT: "{{ DEFAULT_TIMEOUT }}"
    script: |
      curl -m "$TIMEOUT" "$API_URL/endpoint"
```

### Default Values

Provide default values if a variable is not set:

```yaml
env:
  TIMEOUT: "{{ timeout | default:30s }}"
```

## Cron Schedule Format

The schedule uses standard 5-field cron format:

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

### Common Examples

- `"*/5 * * * *"` - Every 5 minutes
- `"0 * * * *"` - Every hour at minute 0
- `"0 0 * * *"` - Daily at midnight
- `"0 0 * * 0"` - Weekly on Sunday at midnight
- `"0 9 * * 1-5"` - Weekdays at 9 AM
- `"0 0 1 * *"` - First day of every month at midnight
- `""` - Manual-only (no automatic scheduling)

## Web UI

The web UI provides:

- **Dashboard**: Overview of all workflows with their status
- **Workflow Details**: View workflow structure, dependencies, and recent runs
- **Run Graph**: Visual representation of task execution and dependencies
- **Logs**: View stdout and stderr for each task instance
- **Controls**: Enable/disable workflows and trigger manual runs

Access the UI at `http://localhost:8080` (or your configured `--http-addr`).

## API Authentication

When the `--api-key` flag is set, all API endpoints (routes starting with `/api/`) require authentication.

### Using HTTP Header

```bash
curl -H "X-API-Key: your-api-key" \
  http://localhost:8080/api/workflow/1/toggle
```

### Using Query Parameter

```bash
curl "http://localhost:8080/api/workflow/1/trigger?api_key=your-api-key"
```

If the API key is missing or incorrect, the server returns `401 Unauthorized`. Static files and HTML pages remain accessible without authentication.

## Branching

gowtf supports dynamic branching where branching tasks output task names to stdout to dynamically determine which path to execute.

### How Branching Works

1. **Mark a task as branching**: Set `branch: true` on the task
2. **Output task names**: The branching task outputs task names to stdout (one per line)
3. **Automatic skipping**: Tasks not listed in the output are automatically skipped, along with their entire downstream branch
4. **Merge points**: Use trigger rules (conditions) like `any_upstream.success` to merge branches

### Branching Example

```yaml
tasks:
  - name: decide_path
    branch: true
    script: |
      # Output task names to determine which path to take
      echo "path_a_task1"
      # Task must succeed (exit 0)
      exit 0
  
  - name: path_a_task1
    depends_on: [decide_path]
    script: echo "Path A"
  
  - name: path_b_task1
    depends_on: [decide_path]
    script: echo "Path B"  # This will be skipped
  
  - name: merge
    depends_on: [path_a_task1, path_b_task1]
    condition: any_upstream.success
    script: echo "Merged"
```

### Multiple Branches

You can output multiple task names for parallel execution:

```yaml
- name: branch_task
  branch: true
  script: |
    echo -e "task_a\ntask_b\ntask_c"
    exit 0
```

### Important Notes

- Branching tasks **must succeed** (exit code 0) - failures are real failures, not branches
- Tasks not in the branching output are skipped along with their entire downstream branch
- Use trigger rules on merge points to handle multiple branches
- Branching tasks cannot have conditions

## Features

- ✅ **Workflow Orchestration**: Define complex workflows with task dependencies
- ✅ **Cron Scheduling**: Schedule workflows using standard cron expressions
- ✅ **Branching**: Dynamic branching based on runtime decisions
- ✅ **Conditional Execution**: Run tasks based on upstream task outcomes using trigger rules
- ✅ **Retry Logic**: Automatic retries with configurable delays
- ✅ **Timeout Support**: Set timeouts for individual tasks
- ✅ **Parallel Execution**: Run independent tasks concurrently
- ✅ **Health Monitoring**: Automatic detection and recovery from stuck tasks
- ✅ **Web UI**: Monitor and manage workflows through a web interface
- ✅ **Manual Triggers**: Trigger workflows on-demand via UI or API
- ✅ **Environment Variables**: Support for workflow and task-level environment variables
- ✅ **Log Management**: Automatic log capture and retention
- ✅ **Data Retention**: Configurable cleanup of old workflow runs

## Getting Help

- Check the web UI at `http://localhost:8080` for workflow status and errors
- View logs in the `--output-dir` directory (default: `./output`)
- Workflow validation errors are displayed in the web UI

## License

See [LICENSE](LICENSE) file for details.
