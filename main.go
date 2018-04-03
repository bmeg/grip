package main

import (
	"os"

	"github.com/bmeg/arachne/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(-1)
	}
}
