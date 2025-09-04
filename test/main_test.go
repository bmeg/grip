package test

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"

	"github.com/bmeg/grip/config"
	esql "github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
	_ "github.com/bmeg/grip/kvi/pebbledb" // import so pebble will register itself

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
		return vertices[i].Id < vertices[j].Id
	})
	for _, v := range vertices {
		err := db.AddVertex([]*gdbi.Vertex{gdbi.NewElementFromVertex(v)})
		if err != nil {
			return err
		}
	}

	sort.Slice(edges[:], func(i, j int) bool {
		return edges[i].Id < edges[j].Id
	})
	for _, e := range edges {
		err := db.AddEdge([]*gdbi.Edge{gdbi.NewElementFromEdge(e)})
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
		fmt.Printf("Adding vertex: %s %#v\n", v.Id, v.Data.AsMap())
		vertices = append(vertices, v)
	}
	edgeChan, err := util.StreamEdgesFromFile("./resources/smtest_edges.txt", 2)
	if err != nil {
		panic(err)
	}
	for e := range edgeChan {
		fmt.Printf("Adding edge: %s %#v\n", e.Id, e.Data.AsMap())
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
		conf.AddPebbleDefault()
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
			fmt.Println("Error: setting up sql graph:", err)
			return
		}
		gdb, err = esql.NewGraphDB(*dbconfig.ExistingSQL)
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.Badger != nil {
		gdb, err = kvgraph.NewKVGraphDB("badger", *dbconfig.Badger)
		defer func() {
			os.RemoveAll(*dbconfig.Badger)
		}()
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.Pebble != nil {
		gdb, err = kvgraph.NewKVGraphDB("pebble", *dbconfig.Pebble)
		defer func() {
			os.RemoveAll(*dbconfig.Pebble)
		}()
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.Bolt != nil {
		gdb, err = kvgraph.NewKVGraphDB("bolt", *dbconfig.Bolt)
		defer func() {
			os.RemoveAll(*dbconfig.Bolt)
		}()
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.Level != nil {
		gdb, err = kvgraph.NewKVGraphDB("badger", *dbconfig.Level)
		defer func() {
			os.RemoveAll(*dbconfig.Level)
		}()
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.Grids != nil {
		gdb, err = grids.NewGraphDB(*dbconfig.Grids)
		defer func() {
			os.RemoveAll(*dbconfig.Grids)
		}()
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.MongoDB != nil {
		gdb, err = mongo.NewGraphDB(*dbconfig.MongoDB)
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else if dbconfig.PSQL != nil {
		gdb, err = psql.NewGraphDB(*dbconfig.PSQL)
		if err != nil {
			fmt.Printf("Init error: %s\n", err)
		}
	} else {
		err = fmt.Errorf("unknown database")
	}
	if err != nil {
		fmt.Printf("Init error: %s\n", err)
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
			fmt.Printf("Error 1st setting up %s graph: %s", dbname, err)
			return
		}
	}

	// After deleting graph, docs, entries, fields should no longer exist in doc
	err = gdb.DeleteGraph("test-graph")
	if err != nil {
		fmt.Printf("Init error: %s\n", err)
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

	afterVertexLabels, _ := db.ListVertexLabels()
	afterEdgeLabels, _ := db.ListEdgeLabels()
	fmt.Printf("afterEdgeLabels: %s afterVertexLabels: %s\n", afterEdgeLabels, afterVertexLabels)
	if len(afterVertexLabels) != 0 || len(afterEdgeLabels) != 0 {
		panic(fmt.Errorf("afterEdgeLabels: %s or afterVertexLabels: %s are not empty\n", afterEdgeLabels, afterVertexLabels))
	}

	if dbname != "existing-sql" {
		err = setupGraph()
		if err != nil {
			fmt.Printf("Error: 2nd setting up %s graph: %s\n", dbname, err)
			return
		}
	}
	// run tests
	exit = m.Run()
}
