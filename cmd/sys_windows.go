package cmd

import (
	"os"

	"golang.org/x/sys/windows"
)

var stopSignals = []os.Signal{os.Interrupt}

func enableVT() bool {
	h := windows.Handle(os.Stderr.Fd())
	var mode uint32
	if windows.GetConsoleMode(h, &mode) != nil {
		return false
	}
	return windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING) == nil
}
