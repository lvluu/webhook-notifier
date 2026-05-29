// Command notifier runs the Webhook Notifier service.
//
// This is a stub entrypoint. Real wiring lands in subsequent slices
// (see GitHub issues #3 onwards).
package main

import (
	"fmt"
	"os"
)

func main() {
	_, _ = fmt.Fprintln(os.Stdout, "webhook-notifier: scaffold OK — service wiring not yet implemented (see issue #3)")
}
