package server

import (
	"log"
	"os"
	"os/signal"

	"github.com/bmeg/arachne/graphserver"
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

		var server *graphserver.ArachneServer = nil
		if mongoURL != "" {
			server = graphserver.NewArachneMongoServer(mongoURL, dbName, workDir)
		} else if boltPath != "" {
			server = graphserver.NewArachneBoltServer(boltPath, workDir)
		} else if rocksPath != "" {
			server = graphserver.NewArachneRocksServer(rocksPath, workDir)
		} else if levelPath != "" {
			server = graphserver.NewArachneLevelServer(levelPath, workDir)
		} else if elasticURL != "" {
			server = graphserver.NewArachneElasticServer(elasticURL, dbName, workDir)
		} else {
			server = graphserver.NewArachneBadgerServer(badgerPath, workDir)
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
	flags.StringVar(&badgerPath, "badger", badgerPath, "BadgerDB Path")
	flags.StringVar(&mongoURL, "mongo", mongoURL, "Mongo URL")
	flags.StringVar(&elasticURL, "elastic", elasticURL, "Elasticsearch URL")
	flags.StringVar(&boltPath, "bolt", boltPath, "BoltDB Path")
	flags.StringVar(&rocksPath, "rocks", rocksPath, "RocksDB Path")
	flags.StringVar(&levelPath, "level", "", "LevelDB Path")
	flags.StringVar(&dbName, "name", dbName, "Database Name")
	flags.StringVar(&contentDir, "content", contentDir, "Content Path")
	flags.StringVar(&workDir, "workdir", workDir, "WorkDir")
}
