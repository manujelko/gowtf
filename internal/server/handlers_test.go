package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/manujelko/gowtf/internal/models"
)

// Helper to setup server with test DB
func setupTestServer(t *testing.T) *Server {
	t.Helper()

	db := models.NewTestDB(t)
	srv, err := New(db, nil, nil, "./output")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return srv
}

func TestHandleHome(t *testing.T) {
	// Hack: We need to make sure we are in the project root for templates
	wd, _ := os.Getwd()
	if !strings.HasSuffix(wd, "gowtf") {
		// Attempt to move up to root
		// This is brittle but works for `go test ./internal/server/...`
		if _, err := os.Stat("../../ui/html"); err == nil {
			os.Chdir("../../")
			defer os.Chdir(wd)
		}
	}

	srv := setupTestServer(t)

	// Insert some data
	models.InsertTestWorkflow(t, srv.DB, "wf1")

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	srv.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(w.Body.String(), "wf1") {
		t.Errorf("expected body to contain 'wf1'")
	}
}
