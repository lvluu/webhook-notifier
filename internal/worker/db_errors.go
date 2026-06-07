package worker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

func isTransientDBError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return isTransientPgCode(pgErr.Code)
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return false
}

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
	if len(code) >= 2 && code[:2] == "08" {
		return true
	}
	return false
}
