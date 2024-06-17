// package debug provides a collection of tools for debugging Go programs at
// runtime.
//
// The primary interface is a simple HTTP server on port 7878 (r8r8...?) that
// provides access to the standard Go pprof debugging tools, an endpoint to
// inspect and change log level at runtime, etc.
//
// Applications can add their own debug endpoints using [Handle] or
// [HandleFunc].
//
// Note: for security reasons, the debug server is disabled by default. To
// enable it, send SIGUSR1 to the process.
package debug

import (
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

var Enabled atomic.Bool

func init() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	go func() {
		for {
			<-c
			Toggle()
		}
	}()
}

func Toggle() {
	val := Enabled.Load()
	if Enabled.CompareAndSwap(val, !val) {
		if val {
			logger.Sugar().Info("debug mode disabled")
		} else {
			logger.Sugar().Info("debug mode enabled")
		}
	}
}
