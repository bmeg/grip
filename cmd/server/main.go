package server

import (
	"os"
	"log"
	"path/filepath"
	"github.com/spf13/cobra"
	"github.com/bmeg/arachne/server"
)

var httpPort string = "8000"
var rpcPort string = "9090"
var dbPath string = "graph.db"

var Cmd = &cobra.Command{
	Use: "server",
	Short: "Starts arachne server",
	Long: ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		dir, _ := filepath.Abs(os.Args[0])
		contentDir := filepath.Join(dir, "..", "..", "share")

		log.Printf("Starting Server")
		server := arachne.NewArachneServer(dbPath)
		server.Start(rpcPort)
		arachne.StartHttpProxy(rpcPort, httpPort, contentDir)
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&httpPort, "port", "8000", "HTTP Port")
	flags.StringVar(&rpcPort, "rpc", "9090", "TCP+RPC Port")
	flags.StringVar(&dbPath, "db", "graph.db", "DB Path")
}
