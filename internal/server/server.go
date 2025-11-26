package server

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"time"

	"github.com/manujelko/gowtf/internal/models"
)

type Server struct {
	DB            *sql.DB
	Workflows     *models.WorkflowStore
	WorkflowRuns  *models.WorkflowRunStore
	WorkflowTasks *models.WorkflowTaskStore
	TaskInstances *models.TaskInstanceStore
	Dependencies  *models.TaskDependenciesStore
	Watcher       WatcherInterface
	OutputDir     string
	httpServer    *http.Server
}

// WatcherInterface defines the interface for accessing watcher errors
type WatcherInterface interface {
	GetFileErrors() map[string]string
	GetErrorsByWorkflowName() map[string]string
}

func New(db *sql.DB, watcher WatcherInterface, outputDir string) (*Server, error) {
	workflows, err := models.NewWorkflowStore(db)
	if err != nil {
		return nil, err
	}

	workflowRuns, err := models.NewWorkflowRunStore(db)
	if err != nil {
		return nil, err
	}

	workflowTasks, err := models.NewWorkflowTaskStore(db)
	if err != nil {
		return nil, err
	}

	taskInstances, err := models.NewTaskInstanceStore(db)
	if err != nil {
		return nil, err
	}

	dependencies, err := models.NewTaskDependenciesStore(db)
	if err != nil {
		return nil, err
	}

	return &Server{
		DB:            db,
		Workflows:     workflows,
		WorkflowRuns:  workflowRuns,
		WorkflowTasks: workflowTasks,
		TaskInstances: taskInstances,
		Dependencies:  dependencies,
		Watcher:       watcher,
		OutputDir:     outputDir,
	}, nil
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	fileServer := http.FileServer(http.Dir("./ui/static"))
	mux.Handle("/static/", http.StripPrefix("/static", fileServer))

	mux.HandleFunc("GET /{$}", s.handleHome)
	mux.HandleFunc("GET /workflow/", s.handleWorkflowDetail)
	mux.HandleFunc("GET /run/", s.handleRunGraph)
	mux.HandleFunc("POST /api/workflow/{id}/toggle", s.handleToggleWorkflow)
	mux.HandleFunc("GET /api/task-instance/{id}/logs", s.handleTaskLogs)

	return s.logRequest(mux)
}

func (s *Server) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap ResponseWriter to capture status code
		ww := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(ww, r)

		log.Printf("%s %s %d %v", r.Method, r.URL.Path, ww.statusCode, time.Since(start))
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (s *Server) ListenAndServe(addr string) error {
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.Routes(),
	}
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
