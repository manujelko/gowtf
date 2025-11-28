package server

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"strings"
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
	Scheduler     SchedulerInterface
	OutputDir     string
	APIKey        string
	httpServer    *http.Server
}

// WatcherInterface defines the interface for accessing watcher errors
type WatcherInterface interface {
	GetFileErrors() map[string]string
	GetErrorsByWorkflowName() map[string]string
}

// SchedulerInterface defines the interface for notifying the scheduler
type SchedulerInterface interface {
	HandleEnabledStateChange(ctx context.Context, workflowID int) error
}

func New(db *sql.DB, watcher WatcherInterface, scheduler SchedulerInterface, outputDir string, apiKey string) (*Server, error) {
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
		Scheduler:     scheduler,
		OutputDir:     outputDir,
		APIKey:        apiKey,
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

	// Apply API key middleware if API key is set
	handler := s.logRequest(mux)
	if s.APIKey != "" {
		handler = s.apiKeyMiddleware(handler)
	}

	return handler
}

func (s *Server) apiKeyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only protect API endpoints
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}

		// Check for API key in header or query parameter
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.URL.Query().Get("api_key")
		}

		// Validate API key
		if apiKey == "" || apiKey != s.APIKey {
			log.Printf("API authentication failed for %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
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
