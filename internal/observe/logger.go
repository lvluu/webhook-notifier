package observe

import (
	"io"
	"log/slog"
	"os"
)

// NewLogger returns a base structured logger configured for JSON output.
// Production callers should pass nil for w to write to stderr.
func NewLogger(w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
