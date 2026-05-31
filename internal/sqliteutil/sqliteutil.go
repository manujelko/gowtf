package sqliteutil

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// IsSQLiteBusy reports whether err is a SQLite busy/locked error.
func IsSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "database is locked") ||
		strings.Contains(s, "SQLITE_BUSY") ||
		strings.Contains(s, "database is locked (5)")
}

// RetryOperation retries fn with exponential backoff when SQLite returns a busy
// error. The sleep between attempts is interrupted if ctx is cancelled.
func RetryOperation(ctx context.Context, maxRetries int, fn func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !IsSQLiteBusy(err) {
			return err
		}
		lastErr = err
		if i < maxRetries-1 {
			backoff := time.Duration(1<<uint(i)) * 10 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

// RetryWithTx retries a transactional operation with exponential backoff when
// SQLite returns a busy error. The sleep between attempts is interrupted if ctx
// is cancelled.
func RetryWithTx(ctx context.Context, db *sql.DB, maxRetries int, fn func(*sql.Tx) error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			if IsSQLiteBusy(err) && i < maxRetries-1 {
				lastErr = err
				select {
				case <-time.After(time.Duration(1<<uint(i)) * 10 * time.Millisecond):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return err
		}

		err = fn(tx)
		if err != nil {
			tx.Rollback()
			if IsSQLiteBusy(err) && i < maxRetries-1 {
				lastErr = err
				select {
				case <-time.After(time.Duration(1<<uint(i)) * 10 * time.Millisecond):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return err
		}

		err = tx.Commit()
		if err != nil {
			if IsSQLiteBusy(err) && i < maxRetries-1 {
				lastErr = err
				select {
				case <-time.After(time.Duration(1<<uint(i)) * 10 * time.Millisecond):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return err
		}

		return nil
	}
	return lastErr
}
