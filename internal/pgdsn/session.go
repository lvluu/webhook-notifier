// Package pgdsn provides tiny helpers for rewriting Postgres DSNs with
// session-level options.
package pgdsn

import (
	"net/url"
	"strings"
)

// WithSessionSetting ensures the Postgres DSN carries a session-level
// `-c key=value` option. It preserves any existing options and avoids
// duplicating the same setting on repeated calls.
func WithSessionSetting(dsn, key, value string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return dsn
	}

	setting := key + "=" + value
	q := u.Query()
	opts := q.Get("options")
	if strings.Contains(opts, setting) {
		return dsn
	}
	if opts == "" {
		q.Set("options", "-c "+setting)
	} else {
		q.Set("options", opts+" -c "+setting)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
