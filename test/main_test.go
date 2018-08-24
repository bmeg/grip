package test

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/bmeg/grip/gripql"
	_ "github.com/bmeg/grip/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/boltdb"   // import so bolt will register itself
	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/elastic"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/leveldb" // import so level will register itself
	"github.com/bmeg/grip/mongo"
	_ "github.com/bmeg/grip/rocksdb" // import so rocks will register itself
	"github.com/bmeg/grip/sql"
	"github.com/bmeg/grip/util"
	_ "github.com/lib/pq" // import so postgres will register as a sql driver
)

var configFile string
var gdb gdbi.GraphDB
var db gdbi.GraphInterface
var dbname string
var vertices = []*gripql.Vertex{}
var edges = []*gripql.Edge{}

func init() {
	flag.StringVar(&configFile, "config", configFile, "config file to use for tests")
	flag.Parse()
	vertChan := util.StreamVerticesFromFile("./resources/smtest_vertices.txt")
	for v := range vertChan {
		vertices = append(vertices, v)
	}
	edgeChan := util.StreamEdgesFromFile("./resources/smtest_edges.txt")
	for e := range edgeChan {
		edges = append(edges, e)
	}
}

func setupGraph() error {
	err := db.AddVertex(vertices)
	if err != nil {
		return err
	}

	err = db.AddEdge(edges)
	if err != nil {
		return err
	}

	return nil
}

func setupSQLGraph() error {
	cmd := exec.Command("bash", "./resources/postgres_load_test_data.sh")
	return cmd.Run()
	// return nil
}

func TestMain(m *testing.M) {
	var err error
	var exit = 1

	defer func() {
		fmt.Println("tests exiting with code", exit)
		os.Exit(exit)
	}()

	conf := config.DefaultConfig()
	if configFile != "" {
		err := config.ParseConfigFile(configFile, conf)
		if err != nil {
			fmt.Printf("error processing config file: %v", err)
			return
		}
	}

	config.TestifyConfig(conf)
	fmt.Printf("Test config: %+v\n", conf)
	dbname = strings.ToLower(conf.Database)

	if dbname == "sql" {
		err = setupSQLGraph()
		if err != nil {
			fmt.Println("Error: setting up graph:", err)
			return
		}
	}

	switch dbname {
	case "bolt", "badger", "level", "rocks":
		gdb, err = kvgraph.NewKVGraphDB(dbname, conf.KVStorePath)
		defer func() {
			os.RemoveAll(conf.KVStorePath)
		}()

	case "elastic":
		gdb, err = elastic.NewGraphDB(conf.Elasticsearch)

	case "mongo":
		gdb, err = mongo.NewGraphDB(conf.MongoDB)

	case "sql":
		gdb, err = sql.NewGraphDB(conf.SQL)

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
	}

	db, err = gdb.Graph("test-graph")
	if err != nil {
		fmt.Println("Error: failed to connect to graph:", err)
		return
	}

	if dbname != "sql" {
		err = setupGraph()
		if err != nil {
			fmt.Println("Error: setting up graph:", err)
			return
		}
	}

	// run tests
	exit = m.Run()
}
