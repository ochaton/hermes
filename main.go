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
		fmt.Print(cmd.Usage)
		return
	}
	switch os.Args[1] {
	case "send", "share":
		cmd.Send(os.Args[2:])
	case "get", "download":
		cmd.Get(os.Args[2:])
	case "-h", "-help", "--help", "help":
		fmt.Print(cmd.Usage)
	default:
		cmd.Get(os.Args[1:])
	}
}
