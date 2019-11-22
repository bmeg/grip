package main

import (
	"fmt"
	"os"

	"github.com/bmeg/grip/cmd"
	"github.com/bmeg/grip/log"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	log.ConfigureLogger(log.DefaultLoggerConfig())
	go func() {
		err := http.ListenAndServe(":6060", nil)
		if err != nil {
			panic(fmt.Errorf("pprof error: %v", err))
		}
	}()

	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
}
