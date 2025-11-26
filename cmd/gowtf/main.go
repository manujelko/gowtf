package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
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
	flag.Parse()

	// Validate flags
	if *workers <= 0 {
		log.Fatalf("workers must be greater than 0, got %d", *workers)
	}

	// Initialize database
	log.Printf("Initializing database at %s", *dbPath)
	db, err := sql.Open("sqlite", *dbPath+"?_timeout=5000&_journal_mode=WAL")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
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
		log.Fatalf("Failed to set busy timeout: %v", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		log.Fatalf("Failed to set WAL mode: %v", err)
	}
	if _, err := db.Exec("PRAGMA synchronous = NORMAL"); err != nil {
		log.Fatalf("Failed to set synchronous mode: %v", err)
	}

	// Apply migrations
	if _, err := db.Exec(migrations.InitialMigration); err != nil {
		log.Fatalf("Failed to apply migrations: %v", err)
	}
	log.Printf("Database initialized successfully")

	// Ensure output directory exists
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Ensure watch directory exists
	if err := os.MkdirAll(*watchDir, 0755); err != nil {
		log.Fatalf("Failed to create watch directory: %v", err)
	}

	// Create communication channels
	watcherEvents := make(chan watcher.WorkflowEvent, 10)
	schedulerEvents := make(chan scheduler.WorkflowRunEvent, 10)

	// Create main context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize watcher
	log.Printf("Initializing watcher for directory: %s", *watchDir)
	w, err := watcher.NewWatcher(db, *watchDir, watcherEvents)
	if err != nil {
		log.Fatalf("Failed to create watcher: %v", err)
	}

	// Initialize scheduler
	log.Printf("Initializing scheduler")
	s, err := scheduler.NewScheduler(db, watcherEvents, schedulerEvents)
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}

	// Initialize executor
	log.Printf("Initializing executor with %d workers", *workers)
	e, err := executor.NewExecutor(db, schedulerEvents, *workers, *outputDir)
	if err != nil {
		log.Fatalf("Failed to create executor: %v", err)
	}

	// Initialize web server
	log.Printf("Initializing web server")
	srv, err := server.New(db, w, *outputDir)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start watcher (runs in background)
	log.Printf("Starting watcher")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := w.Run(); err != nil {
			log.Printf("Watcher error: %v", err)
		}
	}()

	// Start scheduler in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Start(ctx); err != nil {
			log.Printf("Scheduler error: %v", err)
		}
	}()

	// Start executor in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := e.Start(ctx); err != nil {
			log.Printf("Executor error: %v", err)
		}
	}()

	// Start web server in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(*httpAddr); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}()

	log.Printf("gowtf started successfully")
	log.Printf("Watching directory: %s", *watchDir)
	log.Printf("Output directory: %s", *outputDir)
	log.Printf("Web UI available at: http://localhost%s", *httpAddr)
	log.Printf("Press Ctrl+C to stop")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down gracefully...")

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
		log.Printf("Server shutdown error: %v", err)
	}

	// Wait for goroutines to finish
	wg.Wait()

	log.Printf("Shutdown complete")
}
