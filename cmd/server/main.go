package server

import (
	"github.com/bmeg/arachne/graphserver"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"path/filepath"
)

var httpPort = "8201"
var rpcPort = "8202"
var dbPath = "graph.db"
var dbName = "arachne"
var mongoURL string
var boltPath string
var rocksPath string

// Cmd the main command called by the cobra library
var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Starts arachne server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		dir, _ := filepath.Abs(os.Args[0])
		contentDir := filepath.Join(dir, "..", "..", "share")

		log.Printf("Starting Server")

		var server *graphserver.ArachneServer = nil
		if mongoURL != "" {
			server = graphserver.NewArachneMongoServer(mongoURL, dbName)
		} else if boltPath != "" {
			server = graphserver.NewArachneBoltServer(boltPath)
		} else if rocksPath != "" {
			server = graphserver.NewArachneRocksServer(rocksPath)
		} else {
			server = graphserver.NewArachneBadgerServer(dbPath)
		}
		server.Start(rpcPort)
		proxy := graphserver.NewHTTPProxy(rpcPort, httpPort, contentDir)

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
	flags.StringVar(&httpPort, "port", httpPort, "HTTP Port")
	flags.StringVar(&rpcPort, "rpc", rpcPort, "TCP+RPC Port")
	flags.StringVar(&dbPath, "db", "arachne.db", "DB Path")
	flags.StringVar(&mongoURL, "mongo", "", "Mongo URL")
	flags.StringVar(&dbName, "name", "arachne", "DB Name")
	flags.StringVar(&boltPath, "bolt", "", "Bolt DB Path")
	flags.StringVar(&rocksPath, "rocks", "", "RocksDB Path")
}
