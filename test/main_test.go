package test

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/elastic"
	esql "github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
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
	flag.StringVar(&configFile, "config", configFile, "config file to use for tests")
	flag.Parse()
	vertChan, err := util.StreamVerticesFromFile("./resources/smtest_vertices.txt", 2)
	if err != nil {
		panic(err)
	}
	for v := range vertChan {
		vertices = append(vertices, v)
	}
	edgeChan, err := util.StreamEdgesFromFile("./resources/smtest_edges.txt", 2)
	if err != nil {
		panic(err)
	}
	for e := range edgeChan {
		edges = append(edges, e)
	}

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
	} else {
		conf.AddBadgerDefault()
	}

	config.TestifyConfig(conf)
	fmt.Printf("Test config: %+v\n", conf)
	if _, ok := conf.Drivers[conf.Default]; !ok {
		fmt.Printf("default driver %s not found\n", conf.Default)
		return
	}
	dbconfig := conf.Drivers[conf.Default]

	if dbconfig.ExistingSQL != nil {
		err = setupSQLGraph()
		if err != nil {
			fmt.Println("Error: setting up graph:", err)
			return
		}
		gdb, err = esql.NewGraphDB(*dbconfig.ExistingSQL)
	} else if dbconfig.Badger != nil {
		gdb, err = kvgraph.NewKVGraphDB("badger", *dbconfig.Badger)
		defer func() {
			os.RemoveAll(*dbconfig.Badger)
		}()
	} else if dbconfig.Bolt != nil {
		gdb, err = kvgraph.NewKVGraphDB("bolt", *dbconfig.Bolt)
		defer func() {
			os.RemoveAll(*dbconfig.Bolt)
		}()
	} else if dbconfig.Level != nil {
		gdb, err = kvgraph.NewKVGraphDB("badger", *dbconfig.Level)
		defer func() {
			os.RemoveAll(*dbconfig.Level)
		}()
	} else if dbconfig.Grids != nil {
		gdb, err = grids.NewGraphDB(*dbconfig.Grids)
		defer func() {
			os.RemoveAll(*dbconfig.Grids)
		}()
	} else if dbconfig.Elasticsearch != nil {
		gdb, err = elastic.NewGraphDB(*dbconfig.Elasticsearch)
	} else if dbconfig.MongoDB != nil {
		gdb, err = mongo.NewGraphDB(*dbconfig.MongoDB)
	} else if dbconfig.PSQL != nil {
		gdb, err = psql.NewGraphDB(*dbconfig.PSQL)
	} else {
		err = fmt.Errorf("unknown database")
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
