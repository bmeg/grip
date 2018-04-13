package server

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bmeg/arachne/elastic"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/graphserver"
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/mongo"
	"github.com/spf13/cobra"
)

var (
	httpPort   = "8201"
	rpcPort    = "8202"
	dbName     = "arachne"
	workDir    = "arachne.work"
	badgerPath = "arachne.db"
	mongoURL   string
	elasticURL string
	boltPath   string
	rocksPath  string
	levelPath  string
	contentDir string
	readOnly   bool
)

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

		var db gdbi.GraphDB
		if mongoURL != "" {
			db, err = mongo.NewMongo(mongoURL, dbName)
		} else if elasticURL != "" {
			db, err = elastic.NewElastic(elasticURL, dbName)
		} else if boltPath != "" {
			db, err = kvgraph.NewKVGraphDB("bolt", boltPath)
		} else if rocksPath != "" {
			db, err = kvgraph.NewKVGraphDB("rocks", rocksPath)
		} else if levelPath != "" {
			db, err = kvgraph.NewKVGraphDB("level", levelPath)
		} else {
			db, err = kvgraph.NewKVGraphDB("badger", badgerPath)
		}
		if err != nil {
			return fmt.Errorf("Database connection failed: %v", err)
		}

		server := graphserver.NewArachneServer(db, workDir, readOnly)
		err = server.Start(rpcPort)
		if err != nil {
			return fmt.Errorf("Failed to start grpc server: %v", err)
		}

		proxy, err := graphserver.NewHTTPProxy(rpcPort, httpPort, contentDir)
		if err != nil {
			return fmt.Errorf("Failed to setup http proxy: %v", err)
		}

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
	flags.StringVar(&badgerPath, "badger", badgerPath, "BadgerDB Path")
	flags.StringVar(&mongoURL, "mongo", mongoURL, "Mongo URL")
	flags.StringVar(&elasticURL, "elastic", elasticURL, "Elasticsearch URL")
	flags.StringVar(&boltPath, "bolt", boltPath, "BoltDB Path")
	flags.StringVar(&rocksPath, "rocks", rocksPath, "RocksDB Path")
	flags.StringVar(&levelPath, "level", "", "LevelDB Path")
	flags.StringVar(&dbName, "name", dbName, "Database Name")
	flags.StringVar(&contentDir, "content", contentDir, "Content Path")
	flags.StringVar(&workDir, "workdir", workDir, "WorkDir")
	flags.BoolVar(&readOnly, "read-only", readOnly, "Start server in read-only mode")
}
