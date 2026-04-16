package main

import (
	"fmt"
	"os"

	"github.com/ochaton/hermes/cmd"
)

func main() {
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
