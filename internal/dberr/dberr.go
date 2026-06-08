// Package dberr provides typed PostgreSQL error classification shared across
// packages that need to decide whether a failed query is safe to retry.
//
// Classification uses pgconn's typed SQLSTATE code rather than substring
// matching on the error message: substring matching can false-positive on
// payloads or identifiers that happen to contain a SQLSTATE-shaped token, and
// breaks when messages are wrapped or localized.
package dberr

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQL SQLSTATE codes referenced by the helpers below.
const (
	codeQueryCanceled     = "57014" // statement_timeout / transaction_timeout
	codeSerializationFail = "40001" // serialization_failure
	codeDeadlockDetected  = "40P01" // deadlock_detected
	codeLockNotAvailable  = "55P03" // lock_not_available
	codeObjectInUse       = "55006" // object_in_use
	codeTooManyConns      = "53300" // too_many_connections
	codeConfigLimit       = "53400" // configuration_limit_exceeded
)

// IsRetryableLock reports whether err is a transient lock contention failure
// (deadlock or lock-not-available) that is safe to retry for an idempotent,
// self-contained statement holding no cross-transaction locks.
func IsRetryableLock(err error) bool {
	code, ok := pgCode(err)
	if !ok {
		return false
	}
	return code == codeDeadlockDetected || code == codeLockNotAvailable
}

// IsTransient reports whether err is a transient database failure that is
// generally safe to retry: lock contention, serialization failures, statement
// timeouts, resource-exhaustion conditions, connection-class (08*) errors, and
// dropped connections.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if code, ok := pgCode(err); ok {
		return isTransientCode(code)
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return false
}

func pgCode(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code, true
	}
	return "", false
}

func isTransientCode(code string) bool {
	switch code {
	case codeQueryCanceled,
		codeSerializationFail,
		codeDeadlockDetected,
		codeLockNotAvailable,
		codeObjectInUse,
		codeTooManyConns,
		codeConfigLimit:
		return true
	}
	// Class 08 — connection exception.
	return len(code) >= 2 && code[:2] == "08"
}
