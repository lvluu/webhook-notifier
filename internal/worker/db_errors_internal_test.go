package worker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsTransientDBError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"statement timeout 57014", &pgconn.PgError{Code: "57014"}, true},
		{"serialization failure 40001", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock 40P01", &pgconn.PgError{Code: "40P01"}, true},
		{"lock not available 55P03", &pgconn.PgError{Code: "55P03"}, true},
		{"too many connections 53300", &pgconn.PgError{Code: "53300"}, true},
		{"connection exception class 08", &pgconn.PgError{Code: "08006"}, true},
		{"context deadline exceeded", context.DeadlineExceeded, true},
		{"bad conn", driver.ErrBadConn, true},
		{"conn done", sql.ErrConnDone, true},
		{"wrapped statement timeout", fmt.Errorf("batch mark retry: %w", &pgconn.PgError{Code: "57014"}), true},
		{"unique violation 23505 is not transient", &pgconn.PgError{Code: "23505"}, false},
		{"syntax error 42601 is not transient", &pgconn.PgError{Code: "42601"}, false},
		{"plain error is not transient", errors.New("boom"), false},
		{"context canceled is not transient", context.Canceled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientDBError(tt.err); got != tt.want {
				t.Errorf("isTransientDBError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
