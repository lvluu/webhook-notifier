package worker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// isTransientDBError reports whether err is a recoverable database condition
// that the worker run loop should absorb (back off and retry) rather than treat
// as fatal.
//
// Motivation: a single slow mark/claim query can trip Postgres's
// statement_timeout and return SQLSTATE 57014 (query_canceled). That error is
// NOT context.Canceled, so the run loop previously returned it as fatal, which
// tears down every worker, discovery, and the ingest server on the replica.
// Under shared database pressure (e.g. a partner outage driving a retry storm)
// all replicas trip the same timeout and the entire fleet exits at once. None
// of these conditions indicate a permanent fault — the correct response is to
// pause briefly and continue, letting the database recover.
func isTransientDBError(err error) bool {
	if err == nil {
		return false
	}
	// A statement/transaction timeout that surfaces as a deadline (rather than
	// the caller's context being canceled) is transient. The caller checks
	// ctx.Err() separately, so a genuinely canceled run context is handled as
	// shutdown before reaching here.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return isTransientPgCode(pgErr.Code)
	}
	// Connection went bad mid-flight (pool recycle, server restart, pgbouncer
	// disconnect). database/sql transparently retries ErrBadConn on a fresh
	// connection; seeing it (or ErrConnDone) returned here means that round
	// gave up, but the condition is still transient.
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return false
}

// isTransientPgCode classifies Postgres SQLSTATE codes that represent load or
// contention conditions rather than permanent faults (bad SQL, constraint
// violations). See https://www.postgresql.org/docs/current/errcodes-appendix.html
func isTransientPgCode(code string) bool {
	switch code {
	case "57014", // query_canceled (statement_timeout / transaction_timeout)
		"40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03", // lock_not_available
		"55006", // object_in_use
		"53300", // too_many_connections
		"53400": // configuration_limit_exceeded
		return true
	}
	// Class 08 — connection exception (connection lost, failure, rejected).
	if len(code) >= 2 && code[:2] == "08" {
		return true
	}
	return false
}
