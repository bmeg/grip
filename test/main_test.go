package test

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/elastic"
	"github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
	_ "github.com/bmeg/grip/kvi/rocksdb"  // import so rocks will register itself
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/psql"
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
	// sort edges/vertices and insert one at a time to ensure the same write order
	sort.Slice(vertices[:], func(i, j int) bool {
		return vertices[i].Gid < vertices[j].Gid
	})
	for _, v := range vertices {
		err := db.AddVertex([]*gripql.Vertex{v})
		if err != nil {
			return err
		}
	}

	sort.Slice(edges[:], func(i, j int) bool {
		return edges[i].Gid < edges[j].Gid
	})
	for _, e := range edges {
		err := db.AddEdge([]*gripql.Edge{e})
		if err != nil {
			return err
		}
	}

	return nil
}

func setupSQLGraph() error {
	cmd := exec.Command("bash", "./resources/postgres_load_test_data.sh")
	return cmd.Run()
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

	if dbname == "existing-sql" {
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

	case "existing-sql":
		gdb, err = esql.NewGraphDB(conf.ExistingSQL)

	case "psql":
		gdb, err = psql.NewGraphDB(conf.PSQL)

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

	if dbname != "existing-sql" {
		err = setupGraph()
		if err != nil {
			fmt.Println("Error: setting up graph:", err)
			return
		}
	}

	// run tests
	exit = m.Run()
}
