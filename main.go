package main

import (
	"fmt"
	"os"

	"github.com/bmeg/grip/cmd"
	"github.com/bmeg/grip/config"
)

func main() {
	config.ConfigureLogger(config.DefaultLoggerConfig())
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
}
