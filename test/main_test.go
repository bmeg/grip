package test

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/bmeg/arachne/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/arachne/boltdb"   // import so bolt will register itself
	"github.com/bmeg/arachne/cmd/server"
	"github.com/bmeg/arachne/elastic"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvgraph"
	_ "github.com/bmeg/arachne/leveldb" // import so level will register itself
	"github.com/bmeg/arachne/mongo"
	_ "github.com/bmeg/arachne/rocksdb" // import so rocks will register itself
)

var configFile string
var gdb gdbi.GraphDB
var db gdbi.GraphInterface
var dbname string

func init() {
	flag.StringVar(&configFile, "config", configFile, "config file to use for tests")
	flag.Parse()
}

func TestMain(m *testing.M) {
	var err error
	var exit = 1

	defer func() {
		os.Exit(exit)
	}()

	conf := server.DefaultConfig()
	if configFile != "" {
		err := server.ParseConfigFile(configFile, conf)
		if err != nil {
			fmt.Printf("error processing config file: %v", err)
			return
		}
	}
	server.TestifyConfig(conf)
	fmt.Printf("Test config: %+v\n", conf)

	switch dbname = strings.ToLower(conf.Database); dbname {
	case "bolt", "badger", "level", "rocks":
		gdb, err = kvgraph.NewKVGraphDB(dbname, conf.KVStorePath)
		defer func() {
			os.RemoveAll(conf.KVStorePath)
		}()

	case "elastic":
		gdb, err = elastic.NewElastic(conf.ElasticSearch.URL, conf.ElasticSearch.DBName)

	case "mongo":
		gdb, err = mongo.NewMongo(conf.MongoDB.URL, conf.MongoDB.DBName)

	default:
		err = fmt.Errorf("unknown database: %s", dbname)
	}
	if err != nil {
		if dbname == "rocks" {
			fmt.Println(`Warning: rocks driver not found; run test with "-tags rocksdb"`)
			exit = 0
			return
		}
		fmt.Println("Error: database connection failed:", err)
		return
	}

	err = gdb.AddGraph("test-graph")
	if err != nil {
		fmt.Println("Error: failed to add graph:", err)
		return
	}

	db, err = gdb.Graph("test-graph")
	if err != nil {
		fmt.Println("Error: failed to connect to graph:", err)
		return
	}

	// run tests
	exit = m.Run()

}
