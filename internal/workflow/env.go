package workflow

import (
	"log/slog"
	"os"
	"strings"
)

// ResolveEnvValue resolves an environment variable reference from the process environment
// Supports:
// - $VAR_NAME - resolves from os.Getenv("VAR_NAME")
// - $VAR_NAME:default_value - resolves from os.Getenv("VAR_NAME") or uses default if not set
// Returns the resolved value (or empty string/default if not found)
// Logs when variables are missing (but never logs the resolved value for security)
func ResolveEnvValue(value string, logger *slog.Logger) string {
	// Check if this looks like an environment variable reference
	if !strings.HasPrefix(value, "$") {
		// Not an env var reference, return as-is
		return value
	}

	// Extract variable name and optional default value
	// Format: $VAR_NAME or $VAR_NAME:default_value
	varName := value[1:] // Remove leading $
	var defaultValue string
	var hasDefault bool

	// Check for default value separator
	if idx := strings.Index(varName, ":"); idx != -1 {
		defaultValue = varName[idx+1:]
		varName = varName[:idx]
		hasDefault = true
	}

	// Get from environment
	resolved := os.Getenv(varName)

	if resolved == "" {
		// Variable not set
		if hasDefault {
			// Use default value
			logger.Warn("Environment variable not set, using default",
				"variable", varName,
				"has_default", true)
			return defaultValue
		} else {
			// No default, use empty string
			logger.Warn("Environment variable not set, using empty string",
				"variable", varName,
				"has_default", false)
			return ""
		}
	}

	// Variable found - don't log the value for security
	return resolved
}

// ResolveEnvMap resolves all environment variable references in a map
func ResolveEnvMap(env map[string]string, logger *slog.Logger) map[string]string {
	if env == nil {
		return nil
	}

	resolved := make(map[string]string, len(env))
	for k, v := range env {
		resolved[k] = ResolveEnvValue(v, logger)
	}
	return resolved
}
