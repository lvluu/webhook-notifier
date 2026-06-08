package worker

import "github.com/lvluu/webhook-notifier/internal/dberr"

func isTransientDBError(err error) bool {
	return dberr.IsTransient(err)
}
