package cmd

import (
	"os"
	"syscall"

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

func udpControl(_, _ string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		for _, opt := range []int{windows.SO_REUSEADDR, windows.SO_BROADCAST} {
			if serr == nil {
				serr = windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, opt, 1)
			}
		}
	})
	if err != nil {
		return err
	}
	return serr
}
