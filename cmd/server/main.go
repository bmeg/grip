package server

import (
	"github.com/bmeg/arachne/graphserver"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
)

var httpPort = "8201"
var rpcPort = "8202"
var dbPath = "graph.db"
var dbName = "arachne"
var workDir = "arachne.work"
var mongoURL string
var boltPath string
var rocksPath string
var contentDir string

// Cmd the main command called by the cobra library
var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Starts arachne server",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Printf("Starting Server")
		_, err := os.Stat(workDir)
		if os.IsNotExist(err) {
			os.Mkdir(workDir, 0700)
		}
		
		var server *graphserver.ArachneServer = nil
		if mongoURL != "" {
			server = graphserver.NewArachneMongoServer(mongoURL, dbName, workDir)
		} else if boltPath != "" {
			server = graphserver.NewArachneBoltServer(boltPath, workDir)
		} else if rocksPath != "" {
			server = graphserver.NewArachneRocksServer(rocksPath, workDir)
		} else {
			server = graphserver.NewArachneBadgerServer(dbPath, workDir)
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
		log.Printf("Server Stopped, closing database")
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
	flags.StringVar(&contentDir, "content", "", "Content Path")
	flags.StringVar(&workDir, "workdir", "arachne.work", "WorkDir")
}
