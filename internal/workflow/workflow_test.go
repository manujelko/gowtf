package workflow

import (
	"maps"
	"strings"
	"testing"
)

func TestLoadSimple(t *testing.T) {
	wf, err := Load("testdata/simple.yaml")
	if err != nil {
		t.Fatalf("failed to load workflow: %v", err)
	}

	if wf.Name != "hello" {
		t.Errorf("unexpected workflow name: got %q, want %q", wf.Name, "hello")
	}

	if wf.Schedule != "* * * * *" {
		t.Errorf("unexpected workflow schedule: got %q, want %q", wf.Schedule, "* * * * *")
	}

	if len(wf.Tasks) != 1 {
		t.Fatalf("unexpected number of tasks: got %d, want 1", len(wf.Tasks))
	}

	task := wf.Tasks[0]
	want := Task{
		Name:   "hello",
		Script: `echo "Hello, World"`,
	}

	if task.Name != want.Name {
		t.Errorf("unexpected task name: got %q, want %q", task.Name, want.Name)
	}

	if task.Script != want.Script {
		t.Errorf("unexpected task script: got %q, want %q", task.Script, want.Script)
	}
}

func TestLoadComplex(t *testing.T) {
	wf, err := Load("testdata/complex.yaml")
	if err != nil {
		t.Fatalf("failed to load workflow: %v", err)
	}

	if wf.Name != "daily_etl" {
		t.Errorf("unexpected workflow name: got %q, want %q", wf.Name, "daily_etl")
	}

	wantEnv := map[string]string{
		"DATA_DIR": "/var/data",
		"DATE":     "{{ today }}",
	}
	if !maps.Equal(wf.Env, wantEnv) {
		t.Errorf("unexpected workflow env:\n got:  %#v\n want: %#v", wf.Env, wantEnv)
	}

	if len(wf.Tasks) != 9 {
		t.Fatalf("unexpected number of tasks: got %d, want 9", len(wf.Tasks))
	}

	tests := []struct {
		i              int
		name           string
		deps           []string
		condition      string
		scriptContains []string
		retries        int
		retryDelay     string
		timeout        string
	}{
		{
			i: 0, name: "fetch_users",
			scriptContains: []string{
				"curl -s https://api.example.com/users",
				"$DATA_DIR/users.json",
			},
			retries: 3, retryDelay: "30s", timeout: "2m",
		},
		{
			i: 1, name: "fetch_orders",
			scriptContains: []string{
				"curl -s https://api.example.com/orders",
				"$DATA_DIR/orders.json",
			},
			retries: 3, retryDelay: "30s", timeout: "2m",
		},
		{
			i: 2, name: "fetch_products",
			scriptContains: []string{
				"curl -s https://api.example.com/products",
				"$DATA_DIR/products.json",
			},
			retries: 3, retryDelay: "30s", timeout: "2m",
		},
		{
			i: 3, name: "validate_data",
			deps:      []string{"fetch_users", "fetch_orders", "fetch_products"},
			condition: "all_upstream.success",
			scriptContains: []string{
				"python validate.py",
				"--users $DATA_DIR/users.json",
				"--orders $DATA_DIR/orders.json",
				"--products $DATA_DIR/products.json",
			},
		},
		{
			i: 4, name: "send_alert",
			deps:      []string{"validate_data"},
			condition: "validate_data.failed",
			scriptContains: []string{
				"./alert.sh",
				"Validation failed for ETL on $DATE",
			},
		},
		{
			i: 5, name: "transform_data",
			deps:      []string{"validate_data"},
			condition: "validate_data.success",
			scriptContains: []string{
				"python transform.py",
				"--input-dir $DATA_DIR",
				"--output $DATA_DIR/clean.json",
			},
		},
		{
			i: 6, name: "load_to_warehouse",
			deps:      []string{"transform_data"},
			condition: "transform_data.success",
			scriptContains: []string{
				"python load.py",
				"--source $DATA_DIR/clean.json",
			},
			timeout: "10m",
		},
		{
			i: 7, name: "notify_success",
			deps:      []string{"load_to_warehouse"},
			condition: "load_to_warehouse.success",
			scriptContains: []string{
				"./notify.sh",
				"ETL succeeded for $DATE",
			},
		},
		{
			i: 8, name: "notify_failure",
			deps:      []string{"send_alert", "load_to_warehouse"},
			condition: "any_upstream.failed",
			scriptContains: []string{
				"./notify.sh",
				"ETL failed for $DATE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := wf.Tasks[tt.i]

			if task.Name != tt.name {
				t.Errorf("unexpected name: got %q, want %q", task.Name, tt.name)
			}

			if !equalStringSlices(task.DependsOn, tt.deps) {
				t.Errorf("unexpected dependencies for %s: got %v, want %v", tt.name, task.DependsOn, tt.deps)
			}

			if task.Condition != tt.condition {
				t.Errorf("unexpected condition for %s: got %q, want %q", tt.name, task.Condition, tt.condition)
			}

			for _, frag := range tt.scriptContains {
				if !strings.Contains(task.Script, frag) {
					t.Errorf("script for %s missing fragment %q.\nFull script:\n%s", tt.name, frag, task.Script)
				}
			}

			if task.Retries != tt.retries {
				t.Errorf("unexpected retries for %s: got %d, want %d", tt.name, task.Retries, tt.retries)
			}

			if task.RetryDelay != tt.retryDelay {
				t.Errorf("unexpected retryDelay for %s: got %q, want %q", tt.name, task.RetryDelay, tt.retryDelay)
			}

			if task.Timeout != tt.timeout {
				t.Errorf("unexpected timeout for %s: got %q, want %q", tt.name, task.Timeout, tt.timeout)
			}
		})
	}
}

func TestValidation_DuplicateTaskNames(t *testing.T) {
	_, err := Load("testdata/invalid_duplicate.yaml")
	if err == nil {
		t.Fatal("expected validation error but got nil")
	}
	if !strings.Contains(err.Error(), "duplicate task name") {
		t.Fatalf("expected duplicate task error, got: %v", err)
	}
}

func TestValidation_MissingDependency(t *testing.T) {
	_, err := Load("testdata/invalid_missing_dep.yaml")
	if err == nil {
		t.Fatal("expected validation error but got nil")
	}
	if !strings.Contains(err.Error(), "depends_on") {
		t.Fatalf("expected depends_on error, got: %v", err)
	}
}

func TestValidation_Cycle(t *testing.T) {
	_, err := Load("testdata/invalid_cycle.yaml")
	if err == nil {
		t.Fatal("expected cycle validation error but got nil")
	}
	if !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("expected cycle error, got: %v", err)
	}
}

func TestValidation_MissingTaskName(t *testing.T) {
	_, err := Load("testdata/invalid_no_task_name.yaml")
	if err == nil {
		t.Fatal("expected validation error but got nil")
	}
	if !strings.Contains(err.Error(), "task without name") {
		t.Fatalf("expected missing task name error, got: %v", err)
	}
}

func TestValidation_MissingScript(t *testing.T) {
	_, err := Load("testdata/invalid_no_script.yaml")
	if err == nil {
		t.Fatal("expected validation error but got nil")
	}
	if !strings.Contains(err.Error(), "missing script") {
		t.Fatalf("expected missing script error, got: %v", err)
	}
}

func TestValidation_EmptySchedule(t *testing.T) {
	// Empty schedule is now allowed (manual-only workflows)
	wf := &Workflow{
		Name:     "test",
		Schedule: "",
		Tasks: []Task{
			{Name: "task1", Script: "echo hi"},
		},
	}

	err := validateWorkflow(wf)
	if err != nil {
		t.Fatalf("expected empty schedule to be valid, got error: %v", err)
	}
}

func TestValidation_InvalidSchedule(t *testing.T) {
	// Create a temporary workflow with invalid schedule
	wf := &Workflow{
		Name:     "test",
		Schedule: "invalid cron",
		Tasks: []Task{
			{Name: "task1", Script: "echo hi"},
		},
	}

	err := validateWorkflow(wf)
	if err == nil {
		t.Fatal("expected validation error but got nil")
	}
	if !strings.Contains(err.Error(), "invalid schedule format") {
		t.Fatalf("expected invalid schedule format error, got: %v", err)
	}

	// Test with wrong number of fields
	wf.Schedule = "* * *"
	err = validateWorkflow(wf)
	if err == nil {
		t.Fatal("expected validation error for wrong number of fields but got nil")
	}
	if !strings.Contains(err.Error(), "must have exactly 5 space-separated fields") {
		t.Fatalf("expected 5 fields error, got: %v", err)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		if m[s] == 0 {
			return false
		}
		m[s]--
	}
	return true
}
