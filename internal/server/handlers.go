package server

import (
	"encoding/json"
	"fmt"
	"html/template"
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
			// Log error? For now just ignore latest run if failed
			fmt.Printf("Failed to get latest run for wf %d: %v\n", w.ID, err)
		}

		// Get last 5 runs for status circles
		recentRuns, err := s.WorkflowRuns.GetRunsForWorkflow(ctx, w.ID, 5)
		if err != nil {
			fmt.Printf("Failed to get recent runs for wf %d: %v\n", w.ID, err)
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

	workflowID, err := strconv.Atoi(workflowIDStr)
	if err != nil {
		http.Error(w, "Invalid workflow ID", http.StatusBadRequest)
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

	runID, err := strconv.Atoi(runIDStr)
	if err != nil {
		http.Error(w, "Invalid run ID", http.StatusBadRequest)
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

	for _, e := range edgeData {
		fromNode, fromOk := taskIDToNode[e.From]
		toNode, toOk := taskIDToNode[e.To]
		if fromOk && toOk {
			edges = append(edges, GraphEdge{
				FromX: fromNode.X,
				FromY: fromNode.Y,
				ToX:   toNode.X,
				ToY:   toNode.Y,
			})
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
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid workflow ID", http.StatusBadRequest)
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
			fmt.Printf("Failed to notify scheduler of enabled state change for workflow %d: %v\n", id, err)
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
	instanceID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid task instance ID", http.StatusBadRequest)
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

	// If paths are in database, use them
	if taskInstance.StdoutPath != nil && *taskInstance.StdoutPath != "" {
		stdoutPath = *taskInstance.StdoutPath
	}
	if taskInstance.StderrPath != nil && *taskInstance.StderrPath != "" {
		stderrPath = *taskInstance.StderrPath
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
							taskDir := filepath.Join(s.OutputDir, workflow.Name, dateDir, timeDir, task.Name)

							if stdoutPath == "" {
								stdoutPath = filepath.Join(taskDir, "stdout.log")
							}
							if stderrPath == "" {
								stderrPath = filepath.Join(taskDir, "stderr.log")
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
