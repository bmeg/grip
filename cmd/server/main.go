package server

import (
	"context"
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
	"github.com/bmeg/arachne/kvgraph"
	_ "github.com/bmeg/arachne/leveldb" // import so level will register itself
	"github.com/bmeg/arachne/mongo"
	_ "github.com/bmeg/arachne/rocksdb" // import so rocks will register itself
	"github.com/bmeg/arachne/server"
	"github.com/bmeg/arachne/sql"
	_ "github.com/go-sql-driver/mysql" //import so mysql will register as a sql driver
	"github.com/imdario/mergo"
	_ "github.com/lib/pq" // import so postgres will register as a sql driver
	"github.com/spf13/cobra"
)

var conf = &config.Config{}
var configFile string

// Run runs an Arachne server.
// This opens a database and starts an API server.
// This blocks indefinitely.
func Run(conf *config.Config) error {
	log.Printf("Starting Server")
	log.Printf("Config: %+v", conf)

	var db gdbi.GraphDB
	var err error
	switch dbname := strings.ToLower(conf.Database); dbname {
	case "bolt", "badger", "level", "rocks":
		db, err = kvgraph.NewKVGraphDB(dbname, conf.KVStorePath)

	case "elastic", "elasticsearch":
		db, err = elastic.NewGraphDB(conf.Elasticsearch)

	case "mongo", "mongodb":
		db, err = mongo.NewGraphDB(conf.MongoDB)

	case "sql":
		db, err = sql.NewGraphDB(conf.SQL)

	default:
		err = fmt.Errorf("unknown database: %s", dbname)
	}
	if err != nil {
		return fmt.Errorf("database connection failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	srv, err := server.NewArachneServer(db, conf.Server)
	if err != nil {
		return err
	}

	// Start server
	errch := make(chan error)
	go func() {
		errch <- srv.Serve(ctx)
	}()

	// Block until done.
	// Server must be stopped via the context.
	return <-errch
}

// Cmd the main command called by the cobra library
var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Args:  cobra.NoArgs,
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

		defaults := config.DefaultConfig()
		if conf.Server.RPCAddress() != defaults.Server.RPCAddress() {
			if conf.Server.RPCAddress() != conf.RPCClient.ServerAddress {
				conf.RPCClient.ServerAddress = conf.Server.RPCAddress()
			}
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return Run(conf)
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
	flags.StringVar(&conf.Elasticsearch.URL, "elastic-url", conf.Elasticsearch.URL, "Elasticsearch URL")
}
