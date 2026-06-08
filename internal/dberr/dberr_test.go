package dberr

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryableLock(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"lock_not_available", &pgconn.PgError{Code: "55P03"}, true},
		{"wrapped deadlock", fmt.Errorf("defer: %w", &pgconn.PgError{Code: "40P01"}), true},
		{"serialization is not a lock error", &pgconn.PgError{Code: "40001"}, false},
		{"plain error mentioning 40P01 string is not retryable", errors.New("payload contains 40P01 token"), false},
		{"unrelated pg error", &pgconn.PgError{Code: "23505"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryableLock(tt.err); got != tt.want {
				t.Fatalf("IsRetryableLock(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"serialization", &pgconn.PgError{Code: "40001"}, true},
		{"query canceled", &pgconn.PgError{Code: "57014"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"connection exception class 08", &pgconn.PgError{Code: "08006"}, true},
		{"bad conn", driver.ErrBadConn, true},
		{"conn done", sql.ErrConnDone, true},
		{"unique violation is not transient", &pgconn.PgError{Code: "23505"}, false},
		{"generic error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransient(tt.err); got != tt.want {
				t.Fatalf("IsTransient(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
