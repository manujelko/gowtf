package models

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

// isSQLiteBusy checks if an error is a SQLite busy/locked error
func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "database is locked (5)")
}

// retryDBOperation retries a database operation with exponential backoff if it encounters SQLITE_BUSY errors
func retryDBOperation(maxRetries int, fn func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := fn()
		if err == nil {
			return nil
		}

		// Only retry on SQLite busy errors
		if !isSQLiteBusy(err) {
			return err
		}

		lastErr = err
		if i < maxRetries-1 {
			// Exponential backoff: 10ms, 20ms, 40ms, etc.
			backoff := time.Duration(1<<uint(i)) * 10 * time.Millisecond
			time.Sleep(backoff)
		}
	}
	return lastErr
}

func encodeEnv(env map[string]string) (sql.NullString, error) {
	if env == nil {
		return sql.NullString{}, nil
	}

	b, err := json.Marshal(env)
	if err != nil {
		return sql.NullString{}, err
	}

	return sql.NullString{
		String: string(b),
		Valid:  true,
	}, nil
}

func decodeEnv(ns sql.NullString) (map[string]string, error) {
	if !ns.Valid || ns.String == "" {
		return map[string]string{}, nil
	}

	var m map[string]string
	err := json.Unmarshal([]byte(ns.String), &m)
	return m, err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
