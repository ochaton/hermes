//go:build !windows

package cmd

import (
	"os"
	"syscall"
)

var stopSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func enableVT() bool { return true }
