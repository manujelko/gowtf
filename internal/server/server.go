package server

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/manujelko/gowtf/internal/models"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const requestIDKey contextKey = "request_id"

// getRequestID extracts the request ID from the context
func getRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// loggerWithRequestID creates a logger that includes request ID from context
func (s *Server) loggerWithRequestID(ctx context.Context) *slog.Logger {
	requestID := getRequestID(ctx)
	if requestID != "" {
		return s.logger.With("request_id", requestID)
	}
	return s.logger
}

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
	logger        *slog.Logger
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
	TriggerWorkflow(ctx context.Context, workflowID int) (*models.WorkflowRun, error)
}

func New(db *sql.DB, watcher WatcherInterface, scheduler SchedulerInterface, outputDir string, apiKey string, logger *slog.Logger) (*Server, error) {
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
		logger:        logger,
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
	mux.HandleFunc("POST /api/workflow/{id}/trigger", s.handleTriggerWorkflow)
	mux.HandleFunc("GET /api/task-instance/{id}/logs", s.handleTaskLogs)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)

	// Apply middleware in order:
	// 1. Request ID middleware (innermost)
	// 2. Logging middleware
	// 3. API key middleware (outermost)
	handler := s.requestIDMiddleware(mux)
	handler = s.logRequest(handler)
	if s.APIKey != "" {
		handler = s.apiKeyMiddleware(handler)
	}

	return handler
}

// requestIDMiddleware generates a unique request ID and adds it to the context and response headers
func (s *Server) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Generate or extract request ID
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Add request ID to context
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)

		// Add request ID to response headers
		w.Header().Set("X-Request-ID", requestID)

		// Continue with the request using the new context
		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
			s.loggerWithRequestID(r.Context()).Warn("API authentication failed",
				"method", r.Method,
				"path", r.URL.Path,
				"remote_addr", r.RemoteAddr)
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

		// Use logger with request ID from context
		s.loggerWithRequestID(r.Context()).Info("HTTP request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.statusCode,
			"duration", time.Since(start))
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
