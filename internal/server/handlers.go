package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/manujelko/gowtf/internal/models"
)

type WorkflowWithStatus struct {
	*models.Workflow
	LatestRun  *models.WorkflowRun
	RecentRuns []*models.WorkflowRun // Up to 5 most recent runs
	NextRunAt  *time.Time
	ErrorMsg   string // Error message if workflow file failed to parse
}

// validateIDParam validates that an ID string is a positive integer
// Returns the parsed integer and an error if validation fails
func validateIDParam(idStr string) (int, error) {
	if idStr == "" {
		return 0, errors.New("ID parameter is required")
	}

	// Check for invalid characters (only digits allowed)
	for _, r := range idStr {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("ID parameter contains invalid characters: %q", idStr)
		}
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		return 0, fmt.Errorf("ID parameter is not a valid integer: %w", err)
	}

	// Must be positive
	if id <= 0 {
		return 0, fmt.Errorf("ID parameter must be a positive integer, got: %d", id)
	}

	return id, nil
}

// validateAndSanitizePath validates and sanitizes a file path to prevent path traversal attacks
// It ensures the path is within the baseDir and doesn't contain .. or escape the base directory
// Accepts both absolute and relative paths, but ensures they're within baseDir
// Returns the cleaned absolute path and an error if validation fails
func validateAndSanitizePath(path string, baseDir string) (string, error) {
	if path == "" {
		return "", errors.New("path cannot be empty")
	}

	// Check for path traversal attempts
	if strings.Contains(path, "..") {
		return "", errors.New("path cannot contain '..' (path traversal not allowed)")
	}

	// Get absolute baseDir for comparison
	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve base directory: %w", err)
	}

	var absPath string
	if filepath.IsAbs(path) {
		// Path is already absolute - clean it and verify it's within baseDir
		absPath = filepath.Clean(path)
	} else {
		// Path is relative - join with baseDir and get absolute path
		cleanedPath := filepath.Clean(path)
		absPath, err = filepath.Abs(filepath.Join(baseDir, cleanedPath))
		if err != nil {
			return "", fmt.Errorf("failed to resolve path: %w", err)
		}
	}

	// Ensure the resolved path is within baseDir
	relPath, err := filepath.Rel(absBaseDir, absPath)
	if err != nil {
		return "", fmt.Errorf("failed to check path relationship: %w", err)
	}

	// If relative path starts with .., it's outside the base directory
	if strings.HasPrefix(relPath, "..") {
		return "", errors.New("path is outside the allowed output directory")
	}

	return absPath, nil
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, tmpl string, data any) {
	baseDir := "ui/html"
	// Check if directory exists, if not try to find it relative to project root from internal/server
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if _, err := os.Stat("../../" + baseDir); err == nil {
			baseDir = "../../" + baseDir
		}
	}

	files := []string{
		filepath.Join(baseDir, "base.html"),
		filepath.Join(baseDir, tmpl),
	}

	ts, err := template.ParseFiles(files...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = ts.ExecuteTemplate(w, "base.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	ctx := r.Context()
	// Get all workflows (not just enabled)
	workflows, err := s.Workflows.GetAll(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get errors by workflow name from watcher
	workflowErrors := make(map[string]string)
	if s.Watcher != nil {
		workflowErrors = s.Watcher.GetErrorsByWorkflowName()
	}

	// Create cron parser (same as scheduler uses)
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	var data []WorkflowWithStatus
	for _, w := range workflows {
		run, err := s.WorkflowRuns.GetLatestForWorkflow(ctx, w.ID)
		if err != nil {
			s.logger.Error("Failed to get latest run for workflow",
				"workflow_id", w.ID,
				"workflow_name", w.Name,
				"error", err)
		}

		// Get last 5 runs for status circles
		recentRuns, err := s.WorkflowRuns.GetRunsForWorkflow(ctx, w.ID, 5)
		if err != nil {
			s.logger.Error("Failed to get recent runs for workflow",
				"workflow_id", w.ID,
				"workflow_name", w.Name,
				"error", err)
			recentRuns = []*models.WorkflowRun{}
		}

		// Reverse the order so newest runs appear on the right
		for i, j := 0, len(recentRuns)-1; i < j; i, j = i+1, j-1 {
			recentRuns[i], recentRuns[j] = recentRuns[j], recentRuns[i]
		}

		// Calculate next run time from cron schedule
		var nextRunAt *time.Time
		schedule, err := parser.Parse(w.Schedule)
		if err == nil {
			next := schedule.Next(time.Now())
			nextRunAt = &next
		}

		// Get error message for this workflow if any
		errorMsg := workflowErrors[w.Name]

		data = append(data, WorkflowWithStatus{
			Workflow:   w,
			LatestRun:  run,
			RecentRuns: recentRuns,
			NextRunAt:  nextRunAt,
			ErrorMsg:   errorMsg,
		})
	}

	// Check if this is an HTMX request (for auto-refresh)
	if r.Header.Get("HX-Request") == "true" {
		// Return just the table fragment
		s.renderWorkflowTable(w, data)
		return
	}

	// Full page render
	s.render(w, r, "dashboard.html", map[string]any{
		"Workflows": data,
	})
}

func (s *Server) renderWorkflowTable(w http.ResponseWriter, workflows []WorkflowWithStatus) {
	baseDir := "ui/html"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if _, err := os.Stat("../../" + baseDir); err == nil {
			baseDir = "../../" + baseDir
		}
	}

	files := []string{
		filepath.Join(baseDir, "dashboard.html"),
	}

	ts, err := template.ParseFiles(files...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute just the workflow-table fragment
	data := map[string]any{
		"Workflows": workflows,
	}
	err = ts.ExecuteTemplate(w, "workflow-table", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type GridCell struct {
	Run      *models.WorkflowRun
	IsFuture bool
	Time     time.Time
	Status   string
}

type WorkflowGridData struct {
	Workflow *models.Workflow
	Cells    []GridCell
}

func (s *Server) handleWorkflowDetail(w http.ResponseWriter, r *http.Request) {
	// Extract workflow ID from URL path (e.g., /workflow/1)
	path := r.URL.Path
	if !strings.HasPrefix(path, "/workflow/") {
		http.NotFound(w, r)
		return
	}

	workflowIDStr := path[len("/workflow/"):]
	if workflowIDStr == "" {
		http.NotFound(w, r)
		return
	}

	workflowID, err := validateIDParam(workflowIDStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid workflow ID: %s", err.Error()), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get workflow
	workflow, err := s.Workflows.GetByID(ctx, workflowID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if workflow == nil {
		http.NotFound(w, r)
		return
	}

	// Get all runs for this workflow (limit to reasonable number for display)
	runs, err := s.WorkflowRuns.GetRunsForWorkflow(ctx, workflowID, 200)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse cron schedule to calculate future runs
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(workflow.Schedule)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid schedule: %v", err), http.StatusInternalServerError)
		return
	}

	// Build grid cells from runs
	cells := make([]GridCell, 0)
	now := time.Now()

	// Add actual runs (runs are returned in DESC order, newest first)
	for _, run := range runs {
		cells = append(cells, GridCell{
			Run:      run,
			IsFuture: false,
			Time:     run.StartedAt,
			Status:   run.Status.String(),
		})
	}

	// Calculate future runs
	// Start from the most recent run time, or now if no runs exist
	var startTime time.Time
	if len(runs) > 0 {
		// Get the newest run (first in the list since it's DESC ordered)
		startTime = runs[0].StartedAt
		// Get next scheduled time after the newest run
		startTime = schedule.Next(startTime)
	} else {
		// No runs yet, get next scheduled time from now
		startTime = schedule.Next(now)
	}

	// Add future runs (up to 50 future runs, but only if they're actually in the future)
	maxFutureRuns := 50
	nextTime := startTime
	for i := 0; i < maxFutureRuns && nextTime.After(now); i++ {
		cells = append(cells, GridCell{
			Run:      nil,
			IsFuture: true,
			Time:     nextTime,
			Status:   "future",
		})
		nextTime = schedule.Next(nextTime)
	}

	// Sort cells by time (oldest first) using insertion sort
	for i := 1; i < len(cells); i++ {
		key := cells[i]
		j := i - 1
		for j >= 0 && cells[j].Time.After(key.Time) {
			cells[j+1] = cells[j]
			j--
		}
		cells[j+1] = key
	}

	data := WorkflowGridData{
		Workflow: workflow,
		Cells:    cells,
	}

	// Check if this is an HTMX request (for auto-refresh)
	if r.Header.Get("HX-Request") == "true" {
		// Return just the grid fragment
		s.renderWorkflowGrid(w, data)
		return
	}

	// Full page render
	s.render(w, r, "workflow_grid.html", data)
}

func (s *Server) renderWorkflowGrid(w http.ResponseWriter, data WorkflowGridData) {
	baseDir := "ui/html"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if _, err := os.Stat("../../" + baseDir); err == nil {
			baseDir = "../../" + baseDir
		}
	}

	files := []string{
		filepath.Join(baseDir, "workflow_grid.html"),
	}

	ts, err := template.ParseFiles(files...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute just the workflow-grid fragment
	err = ts.ExecuteTemplate(w, "workflow-grid", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type GraphNode struct {
	Task         *models.WorkflowTask
	Instance     *models.TaskInstance
	State        string
	X            float64
	Y            float64
	Dependencies []int // Task IDs this node depends on
}

type GraphEdge struct {
	FromX float64
	FromY float64
	ToX   float64
	ToY   float64
}

type GraphData struct {
	Workflow    *models.Workflow
	WorkflowRun *models.WorkflowRun
	Nodes       []GraphNode
	Edges       []GraphEdge
}

func (s *Server) handleRunGraph(w http.ResponseWriter, r *http.Request) {
	// Extract run ID from URL path (e.g., /run/1)
	path := r.URL.Path
	if !strings.HasPrefix(path, "/run/") {
		http.NotFound(w, r)
		return
	}

	runIDStr := path[len("/run/"):]
	if runIDStr == "" {
		http.NotFound(w, r)
		return
	}

	runID, err := validateIDParam(runIDStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid run ID: %s", err.Error()), http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get workflow run
	workflowRun, err := s.WorkflowRuns.GetByID(ctx, runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if workflowRun == nil {
		http.NotFound(w, r)
		return
	}

	// Get workflow
	workflow, err := s.Workflows.GetByID(ctx, workflowRun.WorkflowID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if workflow == nil {
		http.NotFound(w, r)
		return
	}

	// Get all tasks for the workflow
	tasks, err := s.WorkflowTasks.GetForWorkflow(ctx, workflow.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get all task instances for this run
	taskInstances, err := s.TaskInstances.GetForRun(ctx, runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create a map of task ID to task instance
	instanceMap := make(map[int]*models.TaskInstance)
	for _, ti := range taskInstances {
		instanceMap[ti.TaskID] = ti
	}

	// Build graph nodes and edges
	nodes := make([]GraphNode, 0, len(tasks))
	edgeData := make([]struct {
		From int
		To   int
	}, 0)

	// Create nodes for each task
	for _, task := range tasks {
		instance := instanceMap[task.ID]
		state := "pending"
		if instance != nil {
			state = instance.State.String()
		}

		// Get dependencies for this task
		deps, err := s.Dependencies.GetForTask(ctx, task.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		nodes = append(nodes, GraphNode{
			Task:         task,
			Instance:     instance,
			State:        state,
			Dependencies: deps,
		})

		// Create edges for dependencies
		for _, depID := range deps {
			edgeData = append(edgeData, struct {
				From int
				To   int
			}{
				From: depID,
				To:   task.ID,
			})
		}
	}

	// Simple layout: topological sort with layers
	// Calculate positions for nodes
	calculateLayout(nodes, edgeData)

	// Build edges with coordinates
	edges := make([]GraphEdge, 0, len(edgeData))
	taskIDToNode := make(map[int]*GraphNode)
	for i := range nodes {
		taskIDToNode[nodes[i].Task.ID] = &nodes[i]
	}

	// Node radius in pixels
	const nodeRadius = 40.0

	for _, e := range edgeData {
		fromNode, fromOk := taskIDToNode[e.From]
		toNode, toOk := taskIDToNode[e.To]
		if fromOk && toOk {
			// Calculate direction vector from child (toNode) to ancestor (fromNode)
			dx := fromNode.X - toNode.X
			dy := fromNode.Y - toNode.Y
			length := math.Sqrt(dx*dx + dy*dy)

			if length > 0 {
				// Normalize direction vector
				dx /= length
				dy /= length

				// Calculate edge points (where line touches the node circles)
				// Start point: edge of child node (toNode) in direction of ancestor
				startX := toNode.X + dx*nodeRadius
				startY := toNode.Y + dy*nodeRadius

				// End point: edge of ancestor node (fromNode) opposite direction
				endX := fromNode.X - dx*nodeRadius
				endY := fromNode.Y - dy*nodeRadius

				edges = append(edges, GraphEdge{
					FromX: endX, // Ancestor node edge
					FromY: endY,
					ToX:   startX, // Child node edge
					ToY:   startY,
				})
			} else {
				// Fallback for same position (shouldn't happen)
				edges = append(edges, GraphEdge{
					FromX: fromNode.X,
					FromY: fromNode.Y,
					ToX:   toNode.X,
					ToY:   toNode.Y,
				})
			}
		}
	}

	data := GraphData{
		Workflow:    workflow,
		WorkflowRun: workflowRun,
		Nodes:       nodes,
		Edges:       edges,
	}

	// Check if this is a JSON request (for auto-refresh)
	if r.URL.Query().Get("format") == "json" {
		// Return JSON with node states only
		w.Header().Set("Content-Type", "application/json")
		nodeStates := make(map[int]string)
		for _, node := range nodes {
			nodeStates[node.Task.ID] = node.State
		}
		jsonData := map[string]any{
			"workflowRun": map[string]any{
				"id":        workflowRun.ID,
				"status":    workflowRun.Status.String(),
				"startedAt": workflowRun.StartedAt,
			},
			"nodeStates": nodeStates,
		}
		json.NewEncoder(w).Encode(jsonData)
		return
	}

	// Full page render
	s.render(w, r, "run_graph.html", data)
}

// calculateLayout performs a simple layered layout (topological sort)
func calculateLayout(nodes []GraphNode, edges []struct{ From, To int }) {
	// Create a map of task ID to node index
	taskIDToIndex := make(map[int]int)
	for i, node := range nodes {
		taskIDToIndex[node.Task.ID] = i
	}

	// Calculate in-degree for each node
	inDegree := make(map[int]int)
	for _, node := range nodes {
		inDegree[node.Task.ID] = 0
	}
	for _, edge := range edges {
		inDegree[edge.To]++
	}

	// Topological sort to assign layers
	layer := make(map[int]int)
	queue := make([]int, 0)

	// Start with nodes that have no dependencies
	for _, node := range nodes {
		if inDegree[node.Task.ID] == 0 {
			queue = append(queue, node.Task.ID)
			layer[node.Task.ID] = 0
		}
	}

	// Process nodes layer by layer
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Find all nodes that depend on current
		for _, edge := range edges {
			if edge.From == current {
				inDegree[edge.To]--
				if inDegree[edge.To] == 0 {
					layer[edge.To] = layer[current] + 1
					queue = append(queue, edge.To)
				}
			}
		}
	}

	// Assign positions based on layers
	layerCounts := make(map[int]int)
	for _, node := range nodes {
		l := layer[node.Task.ID]
		layerCounts[l]++
	}

	layerPositions := make(map[int]int)
	for i, node := range nodes {
		l := layer[node.Task.ID]
		pos := layerPositions[l]
		layerPositions[l]++

		// Position: X based on layer, Y based on position in layer
		maxLayer := 0
		for l := range layerCounts {
			if l > maxLayer {
				maxLayer = l
			}
		}

		// Normalize to 0-800 range for X, 0-600 for Y
		if maxLayer > 0 {
			nodes[i].X = float64(l) * 200.0
		} else {
			nodes[i].X = 100.0
		}

		if layerCounts[l] > 1 {
			nodes[i].Y = float64(pos) * (500.0 / float64(layerCounts[l]-1))
		} else {
			nodes[i].Y = 250.0
		}
	}
}

func (s *Server) renderRunGraph(w http.ResponseWriter, data GraphData) {
	baseDir := "ui/html"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		if _, err := os.Stat("../../" + baseDir); err == nil {
			baseDir = "../../" + baseDir
		}
	}

	files := []string{
		filepath.Join(baseDir, "run_graph.html"),
	}

	ts, err := template.ParseFiles(files...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Execute just the graph fragment
	err = ts.ExecuteTemplate(w, "run-graph", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleToggleWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract workflow ID from URL path
	path := r.URL.Path
	if !strings.HasPrefix(path, "/api/workflow/") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	idStr := strings.TrimPrefix(path, "/api/workflow/")
	idStr = strings.TrimSuffix(idStr, "/toggle")
	id, err := validateIDParam(idStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid workflow ID: %s", err.Error()), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	workflow, err := s.Workflows.GetByID(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if workflow == nil {
		http.Error(w, "Workflow not found", http.StatusNotFound)
		return
	}

	// Toggle enabled state
	workflow.Enabled = !workflow.Enabled
	if err := s.Workflows.Update(ctx, workflow); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify scheduler of the enabled state change
	if s.Scheduler != nil {
		if err := s.Scheduler.HandleEnabledStateChange(ctx, id); err != nil {
			// Log error but don't fail the request - the database is updated
			s.logger.Error("Failed to notify scheduler of enabled state change",
				"workflow_id", id,
				"workflow_name", workflow.Name,
				"error", err)
		}
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) handleTaskLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract task instance ID from URL path
	path := r.URL.Path
	if !strings.HasPrefix(path, "/api/task-instance/") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	idStr := strings.TrimPrefix(path, "/api/task-instance/")
	idStr = strings.TrimSuffix(idStr, "/logs")
	instanceID, err := validateIDParam(idStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid task instance ID: %s", err.Error()), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	taskInstance, err := s.TaskInstances.GetByID(ctx, instanceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if taskInstance == nil {
		http.Error(w, "Task instance not found", http.StatusNotFound)
		return
	}

	// Read stdout and stderr files
	type LogResponse struct {
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
		TaskName string `json:"task_name"`
		State    string `json:"state"`
		ExitCode *int   `json:"exit_code,omitempty"`
	}

	response := LogResponse{
		TaskName: "",
		State:    taskInstance.State.String(),
		ExitCode: taskInstance.ExitCode,
	}

	// Get task name
	if taskInstance.TaskID > 0 {
		// We need to get the task to get its name
		// For now, we'll try to get it from the workflow run
		workflowRun, err := s.WorkflowRuns.GetByID(ctx, taskInstance.WorkflowRunID)
		if err == nil && workflowRun != nil {
			workflow, err := s.Workflows.GetByID(ctx, workflowRun.WorkflowID)
			if err == nil && workflow != nil {
				tasks, err := s.WorkflowTasks.GetForWorkflow(ctx, workflow.ID)
				if err == nil {
					for _, task := range tasks {
						if task.ID == taskInstance.TaskID {
							response.TaskName = task.Name
							break
						}
					}
				}
			}
		}
	}

	// Determine log file paths
	var stdoutPath, stderrPath string

	// If paths are in database, use them (but validate them)
	if taskInstance.StdoutPath != nil && *taskInstance.StdoutPath != "" {
		validatedPath, err := validateAndSanitizePath(*taskInstance.StdoutPath, s.OutputDir)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid stdout path: %s", err.Error()), http.StatusBadRequest)
			return
		}
		stdoutPath = validatedPath
	}
	if taskInstance.StderrPath != nil && *taskInstance.StderrPath != "" {
		validatedPath, err := validateAndSanitizePath(*taskInstance.StderrPath, s.OutputDir)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid stderr path: %s", err.Error()), http.StatusBadRequest)
			return
		}
		stderrPath = validatedPath
	}

	// If paths not in database yet (task still running), construct them
	if stdoutPath == "" || stderrPath == "" {
		// Get workflow run to get start time
		workflowRun, err := s.WorkflowRuns.GetByID(ctx, taskInstance.WorkflowRunID)
		if err == nil && workflowRun != nil {
			// Get workflow to get name
			workflow, err := s.Workflows.GetByID(ctx, workflowRun.WorkflowID)
			if err == nil && workflow != nil {
				// Get all tasks for workflow and find the matching one
				tasks, err := s.WorkflowTasks.GetForWorkflow(ctx, workflow.ID)
				if err == nil {
					for _, task := range tasks {
						if task.ID == taskInstance.TaskID {
							// Construct path: outputDir/workflow-name/YYYY-MM-DD/HH-MM-SS/task-name/stdout.log
							dateDir := workflowRun.StartedAt.Format("2006-01-02")
							timeDir := workflowRun.StartedAt.Format("15-04-05")
							// Build relative path components (no path traversal possible here as we control all values)
							relativePath := filepath.Join(workflow.Name, dateDir, timeDir, task.Name)

							if stdoutPath == "" {
								// Validate the constructed path
								validatedPath, err := validateAndSanitizePath(filepath.Join(relativePath, "stdout.log"), s.OutputDir)
								if err != nil {
									http.Error(w, fmt.Sprintf("Invalid stdout path: %s", err.Error()), http.StatusBadRequest)
									return
								}
								stdoutPath = validatedPath
							}
							if stderrPath == "" {
								// Validate the constructed path
								validatedPath, err := validateAndSanitizePath(filepath.Join(relativePath, "stderr.log"), s.OutputDir)
								if err != nil {
									http.Error(w, fmt.Sprintf("Invalid stderr path: %s", err.Error()), http.StatusBadRequest)
									return
								}
								stderrPath = validatedPath
							}
							break
						}
					}
				}
			}
		}
	}

	// Read stdout
	if stdoutPath != "" {
		if content, err := os.ReadFile(stdoutPath); err == nil {
			response.Stdout = string(content)
		}
	}

	// Read stderr
	if stderrPath != "" {
		if content, err := os.ReadFile(stderrPath); err == nil {
			response.Stderr = string(content)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HealthResponse represents the JSON response for health endpoints
type HealthResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// handleHealth checks database connectivity
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Perform a simple database query to check connectivity
	err := s.DB.PingContext(ctx)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(HealthResponse{
			Status: "error",
			Error:  "Database connection failed: " + err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthResponse{
		Status: "ok",
	})
}

// handleReady checks if all components are initialized
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if all required components are initialized
	var errors []string

	if s.DB == nil {
		errors = append(errors, "database not initialized")
	}
	if s.Workflows == nil {
		errors = append(errors, "workflows store not initialized")
	}
	if s.WorkflowRuns == nil {
		errors = append(errors, "workflow runs store not initialized")
	}
	if s.WorkflowTasks == nil {
		errors = append(errors, "workflow tasks store not initialized")
	}
	if s.TaskInstances == nil {
		errors = append(errors, "task instances store not initialized")
	}
	if s.Dependencies == nil {
		errors = append(errors, "dependencies store not initialized")
	}
	if s.Watcher == nil {
		errors = append(errors, "watcher not initialized")
	}
	if s.Scheduler == nil {
		errors = append(errors, "scheduler not initialized")
	}

	// If there are any errors, return 503
	if len(errors) > 0 {
		errorMsg := "Components not ready: " + strings.Join(errors, ", ")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(HealthResponse{
			Status: "error",
			Error:  errorMsg,
		})
		return
	}

	// All components are initialized
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthResponse{
		Status: "ok",
	})
}
