package server

import (
	"github.com/bmeg/arachne/graphserver"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"path/filepath"
)

var httpPort string = "8000"
var rpcPort string = "9090"
var dbPath string = "graph.db"
var mongoUrl string = ""

var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Starts arachne server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		dir, _ := filepath.Abs(os.Args[0])
		contentDir := filepath.Join(dir, "..", "..", "share")

		log.Printf("Starting Server")

		var server *graphserver.ArachneServer = nil
		if mongoUrl != "" {
			server = graphserver.NewArachneMongoServer(mongoUrl, dbPath)
		} else {
			server = graphserver.NewArachneBadgerServer(dbPath)
		}
		server.Start(rpcPort)
		proxy := graphserver.NewHttpProxy(rpcPort, httpPort, contentDir)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			<-c
			proxy.Stop()
		}()
		proxy.Run()
		log.Printf("Server Stoped, closing database")
		server.CloseDB()
		return nil
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&httpPort, "port", "8000", "HTTP Port")
	flags.StringVar(&rpcPort, "rpc", "9090", "TCP+RPC Port")
	flags.StringVar(&dbPath, "db", "graph_db", "DB Path")
	flags.StringVar(&mongoUrl, "mongo", "", "Mongo URL")
}
