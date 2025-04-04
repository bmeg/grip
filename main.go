package main

import (
	"os"

	"github.com/bmeg/grip/cmd"
	"github.com/bmeg/grip/log"
)

func main() {
	log.ConfigureLogger(log.DefaultLoggerConfig())
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Errorf("Error:", err.Error())
		os.Exit(1)
	}
}
