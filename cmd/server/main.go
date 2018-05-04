package server

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	_ "github.com/bmeg/arachne/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/arachne/boltdb"   // import so bolt will register itself
	"github.com/bmeg/arachne/config"
	"github.com/bmeg/arachne/elastic"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/graphserver"
	"github.com/bmeg/arachne/kvgraph"
	_ "github.com/bmeg/arachne/leveldb" // import so level will register itself
	"github.com/bmeg/arachne/mongo"
	_ "github.com/bmeg/arachne/rocksdb" // import so rocks will register itself
	"github.com/imdario/mergo"
	"github.com/spf13/cobra"
)

var conf = &config.Config{}
var configFile string

// Start starts an Arachne server
func Start(conf *config.Config) error {
	log.Printf("Starting Server")
	log.Printf("Config: %+v", conf)

	var db gdbi.GraphDB
	var err error
	switch dbname := strings.ToLower(conf.Database); dbname {
	case "bolt", "badger", "level", "rocks":
		db, err = kvgraph.NewKVGraphDB(dbname, conf.KVStorePath)

	case "elastic":
		db, err = elastic.NewElastic(conf.ElasticSearch)

	case "mongo":
		db, err = mongo.NewMongo(conf.MongoDB)

	default:
		err = fmt.Errorf("unknown database: %s", dbname)
	}
	if err != nil {
		return fmt.Errorf("database connection failed: %v", err)
	}

	_, err = os.Stat(conf.Server.WorkDir)
	if os.IsNotExist(err) {
		os.Mkdir(conf.Server.WorkDir, 0700)
	}

	server := graphserver.NewArachneServer(db, conf.Server.WorkDir, conf.Server.ReadOnly)
	err = server.Start(conf.Server.RPCPort)
	if err != nil {
		return fmt.Errorf("Failed to start grpc server: %v", err)
	}

	proxy, err := graphserver.NewHTTPProxy(conf.Server.RPCPort, conf.Server.HTTPPort, conf.Server.ContentDir)
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
}

// Cmd the main command called by the cobra library
var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Start an arachne server",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		dconf := config.DefaultConfig()
		if configFile != "" {
			err := config.ParseConfigFile(configFile, dconf)
			if err != nil {
				return fmt.Errorf("error processing config file: %v", err)
			}
		}
		// file vals <- cli val
		err := mergo.MergeWithOverwrite(dconf, conf)
		if err != nil {
			return fmt.Errorf("error processing config file: %v", err)
		}
		conf = dconf
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return Start(conf)
	},
}

func init() {
	flags := Cmd.Flags()
	flags.StringVarP(&configFile, "config", "c", configFile, "Config file")
	flags.StringVarP(&conf.Database, "database", "d", conf.Database, `Database to use ["badger", "bolt", "level", "rocks", "mongo", "elastic"]`)
	flags.StringVar(&conf.Server.HTTPPort, "http-port", conf.Server.HTTPPort, "HTTP port")
	flags.StringVar(&conf.Server.RPCPort, "rpc-port", conf.Server.RPCPort, "TCP+RPC port")
	flags.StringVar(&conf.Server.ContentDir, "content", conf.Server.ContentDir, "Server content directory")
	flags.StringVar(&conf.Server.WorkDir, "workdir", conf.Server.WorkDir, "Server working directory")
	flags.BoolVar(&conf.Server.ReadOnly, "read-only", conf.Server.ReadOnly, "Start server in read-only mode")
	flags.StringVar(&conf.KVStorePath, "kvstore-path", conf.KVStorePath, "Path to use for key-value store database (Badger, BoltDB, LevelDB, RocksDB)")
	flags.StringVar(&conf.MongoDB.URL, "mongo-url", conf.MongoDB.URL, "MongoDB URL")
	flags.StringVar(&conf.ElasticSearch.URL, "elastic-url", conf.ElasticSearch.URL, "Elasticsearch URL")
}
