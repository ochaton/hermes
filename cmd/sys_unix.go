//go:build !windows

package cmd

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

var stopSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func enableVT() bool { return true }

func udpControl(_, _ string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		for _, opt := range []int{unix.SO_REUSEADDR, unix.SO_REUSEPORT, unix.SO_BROADCAST} {
			if serr == nil {
				serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, opt, 1)
			}
		}
	})
	if err != nil {
		return err
	}
	return serr
}
