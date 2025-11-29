package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

func validateWorkflow(wf *Workflow) error {
	var errs []string

	if wf.Name == "" {
		errs = append(errs, "workflow.name is required")
	}

	// Schedule is optional - empty string means manual-only workflow
	if wf.Schedule != "" {
		if err := validateSchedule(wf.Schedule); err != nil {
			errs = append(errs, fmt.Sprintf("workflow %q: invalid schedule format: %v", wf.Name, err))
		}
	}

	if len(wf.Tasks) == 0 {
		errs = append(errs, fmt.Sprintf("workflow %q: must contain at least one task", wf.Name))
	}

	if err := validateTasks(wf.Tasks); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.New(strings.Join(errs, "\n"))
}

func validateTasks(tasks []Task) error {
	var errs []string

	taskNames := map[string]struct{}{}

	for _, t := range tasks {
		if t.Name == "" {
			errs = append(errs, "task without name")
			continue
		}

		if t.Script == "" {
			errs = append(errs, fmt.Sprintf("task %q: missing script", t.Name))
		}

		// Duplicate detection
		if _, exists := taskNames[t.Name]; exists {
			errs = append(errs, fmt.Sprintf("duplicate task name %q", t.Name))
		}
		taskNames[t.Name] = struct{}{}
	}

	// Dependency validation
	for _, t := range tasks {
		if t.Name == "" {
			continue // skip dependency checks for invalid tasks
		}

		for _, dep := range t.DependsOn {
			if _, exists := taskNames[dep]; !exists {
				errs = append(errs, fmt.Sprintf("task %q depends_on %q which does not exist", t.Name, dep))
			}
		}
	}

	// Cycle detection
	if err := detectCycles(tasks); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.New(strings.Join(errs, "\n"))
}

func detectCycles(tasks []Task) error {
	// Adjacency list
	graph := make(map[string][]string)
	for _, t := range tasks {
		graph[t.Name] = t.DependsOn
	}

	visited := make(map[string]bool) // fully explored
	stack := make(map[string]bool)   // currently exploring
	path := []string{}               // depth first search path

	var dfs func(string) error
	dfs = func(node string) error {
		if stack[node] {
			// Cycle found - extract cycle path
			cycle := []string{}
			for i := len(path) - 1; i >= 0; i-- {
				cycle = append([]string{path[i]}, cycle...)
				if path[i] == node {
					break
				}
			}
			cycle = append(cycle, node)
			return fmt.Errorf("cycle detected: %s", strings.Join(cycle, " -> "))
		}

		if visited[node] {
			return nil
		}

		stack[node] = true
		path = append(path, node)

		for _, dep := range graph[node] {
			if err := dfs(dep); err != nil {
				return err
			}
		}

		// Backtrack
		stack[node] = false
		path = path[:len(path)-1]
		visited[node] = true
		return nil
	}

	// Run depth first search from all nodes
	for node := range graph {
		if err := dfs(node); err != nil {
			return err
		}
	}

	return nil
}

func validateSchedule(schedule string) error {
	// Cron format: 5 space-separated fields (minute hour day month weekday)
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return fmt.Errorf("schedule must have exactly 5 space-separated fields (minute hour day month weekday), got %d", len(fields))
	}

	// Basic validation: each field should contain valid cron characters
	// Valid characters: digits, *, -, /, , (comma)
	cronFieldPattern := regexp.MustCompile(`^[\d\*\/\-,]+$`)
	for i, field := range fields {
		fieldNames := []string{"minute", "hour", "day", "month", "weekday"}
		if !cronFieldPattern.MatchString(field) {
			return fmt.Errorf("invalid %s field: %q (must contain only digits, *, -, /, or commas)", fieldNames[i], field)
		}
	}

	return nil
}
