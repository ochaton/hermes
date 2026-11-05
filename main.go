package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/ochaton/hermes/cmd"
)

func main() {
	// quic-go moves every UDP packet between goroutines; with more Ps on macOS that
	// costs a thread wakeup per packet, which caps a 1 Gbit/s link at ~60 MB/s.
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(1)
	}
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: hermes <share|download> [options]\n")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "share":
		cmd.Share(os.Args[2:])
	case "download":
		cmd.Download(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
