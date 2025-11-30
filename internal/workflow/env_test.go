package workflow

import (
	"log/slog"
	"os"
	"testing"
)

func TestResolveEnvValue_NoReference(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	result := ResolveEnvValue("plain-value", logger)
	if result != "plain-value" {
		t.Errorf("Expected 'plain-value', got %q", result)
	}
}

func TestResolveEnvValue_SimpleReference(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	// Set test environment variable
	os.Setenv("TEST_VAR", "test-value")
	defer os.Unsetenv("TEST_VAR")

	result := ResolveEnvValue("$TEST_VAR", logger)
	if result != "test-value" {
		t.Errorf("Expected 'test-value', got %q", result)
	}
}

func TestResolveEnvValue_WithDefault(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	// Variable not set, should use default
	result := ResolveEnvValue("$MISSING_VAR:default-value", logger)
	if result != "default-value" {
		t.Errorf("Expected 'default-value', got %q", result)
	}

	// Variable set, should use value from env
	os.Setenv("MISSING_VAR", "env-value")
	defer os.Unsetenv("MISSING_VAR")
	result = ResolveEnvValue("$MISSING_VAR:default-value", logger)
	if result != "env-value" {
		t.Errorf("Expected 'env-value', got %q", result)
	}
}

func TestResolveEnvValue_WithoutDefault(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	// Variable not set, no default
	result := ResolveEnvValue("$MISSING_VAR_NO_DEFAULT", logger)
	if result != "" {
		t.Errorf("Expected empty string, got %q", result)
	}
}

func TestResolveEnvMap(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	// Set test environment variables
	os.Setenv("VAR1", "value1")
	os.Setenv("VAR2", "value2")
	defer func() {
		os.Unsetenv("VAR1")
		os.Unsetenv("VAR2")
	}()

	env := map[string]string{
		"KEY1": "$VAR1",
		"KEY2": "$VAR2",
		"KEY3": "plain-value",
		"KEY4": "$MISSING:default-value",
		"KEY5": "$MISSING_NO_DEFAULT",
	}

	resolved := ResolveEnvMap(env, logger)

	if resolved["KEY1"] != "value1" {
		t.Errorf("Expected KEY1='value1', got %q", resolved["KEY1"])
	}
	if resolved["KEY2"] != "value2" {
		t.Errorf("Expected KEY2='value2', got %q", resolved["KEY2"])
	}
	if resolved["KEY3"] != "plain-value" {
		t.Errorf("Expected KEY3='plain-value', got %q", resolved["KEY3"])
	}
	if resolved["KEY4"] != "default-value" {
		t.Errorf("Expected KEY4='default-value', got %q", resolved["KEY4"])
	}
	if resolved["KEY5"] != "" {
		t.Errorf("Expected KEY5='', got %q", resolved["KEY5"])
	}
}

func TestResolveEnvMap_Nil(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	result := ResolveEnvMap(nil, logger)
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}
