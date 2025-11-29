package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/manujelko/gowtf/internal/db/migrations"
	"github.com/manujelko/gowtf/internal/executor"
	"github.com/manujelko/gowtf/internal/scheduler"
	"github.com/manujelko/gowtf/internal/server"
	"github.com/manujelko/gowtf/internal/watcher"
)

func main() {
	// Parse CLI flags
	dbPath := flag.String("db", "./gowtf.db", "Database file path")
	watchDir := flag.String("watch-dir", "./workflows", "Directory to watch for workflow YAML files")
	outputDir := flag.String("output-dir", "./output", "Directory for task output/logs")
	workers := flag.Int("workers", 4, "Worker pool size")
	httpAddr := flag.String("http-addr", ":8080", "HTTP server address")
	apiKey := flag.String("api-key", "", "API key for protecting API endpoints (optional, but recommended for production)")
	flag.Parse()

	// Initialize logger with human-readable text handler
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Validate flags
	if *workers <= 0 {
		logger.Error("Invalid workers count", "workers", *workers)
		os.Exit(1)
	}

	// Initialize database
	logger.Info("Initializing database", "path", *dbPath)
	db, err := sql.Open("sqlite", *dbPath+"?_timeout=5000&_journal_mode=WAL")
	if err != nil {
		logger.Error("Failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Configure connection pool for better concurrency
	// SQLite benefits from multiple connections when using WAL mode
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0) // Don't close connections due to age

	// Set WAL mode and busy timeout for better concurrent access
	// WAL mode allows concurrent reads and a single writer, which is perfect for our use case
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		logger.Error("Failed to set busy timeout", "error", err)
		os.Exit(1)
	}
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		logger.Error("Failed to set WAL mode", "error", err)
		os.Exit(1)
	}
	if _, err := db.Exec("PRAGMA synchronous = NORMAL"); err != nil {
		logger.Error("Failed to set synchronous mode", "error", err)
		os.Exit(1)
	}

	// Apply migrations
	if _, err := db.Exec(migrations.InitialMigration); err != nil {
		logger.Error("Failed to apply migrations", "error", err)
		os.Exit(1)
	}
	logger.Info("Database initialized successfully")

	// Ensure output directory exists
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		logger.Error("Failed to create output directory", "path", *outputDir, "error", err)
		os.Exit(1)
	}

	// Ensure watch directory exists
	if err := os.MkdirAll(*watchDir, 0755); err != nil {
		logger.Error("Failed to create watch directory", "path", *watchDir, "error", err)
		os.Exit(1)
	}

	// Create communication channels
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	schedulerEvents := make(chan scheduler.WorkflowRunEvent, 10)

	// Create main context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize watcher
	logger.Info("Initializing watcher", "directory", *watchDir)
	w, err := watcher.NewWatcher(db, *watchDir, watcherEvents, logger)
	if err != nil {
		logger.Error("Failed to create watcher", "error", err)
		os.Exit(1)
	}

	// Initialize scheduler
	logger.Info("Initializing scheduler")
	s, err := scheduler.NewScheduler(db, watcherEvents, schedulerEvents, logger)
	if err != nil {
		logger.Error("Failed to create scheduler", "error", err)
		os.Exit(1)
	}

	// Initialize executor
	logger.Info("Initializing executor", "workers", *workers)
	e, err := executor.NewExecutor(db, schedulerEvents, *workers, *outputDir, logger)
	if err != nil {
		logger.Error("Failed to create executor", "error", err)
		os.Exit(1)
	}

	// Initialize web server
	logger.Info("Initializing web server")
	srv, err := server.New(db, w, s, *outputDir, *apiKey, logger)
	if err != nil {
		logger.Error("Failed to create server", "error", err)
		os.Exit(1)
	}

	// Start watcher (runs in background)
	logger.Info("Starting watcher")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.Run(); err != nil {
			logger.Error("Watcher error", "error", err)
		}
	}()

	// Start scheduler in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Start(ctx); err != nil {
			logger.Error("Scheduler error", "error", err)
		}
	}()

	// Start executor in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := e.Start(ctx); err != nil {
			logger.Error("Executor error", "error", err)
		}
	}()

	// Start web server in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(*httpAddr); err != nil && err != http.ErrServerClosed {
			logger.Error("Server error", "error", err)
		}
	}()

	logger.Info("gowtf started successfully",
		"watch_dir", *watchDir,
		"output_dir", *outputDir,
		"web_ui", "http://localhost"+*httpAddr)
	logger.Info("Press Ctrl+C to stop")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down gracefully...")

	// Cancel context to stop scheduler and executor
	cancel()

	// Stop components
	w.Stop()
	s.Stop()
	e.Stop()

	// Stop server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	}

	// Wait for goroutines to finish
	wg.Wait()

	logger.Info("Shutdown complete")
}
