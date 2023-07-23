package test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/elastic"
	"github.com/bmeg/grip/endpoints/graphql"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/gripql/schema"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/psql"
	"github.com/bmeg/grip/server"
	"github.com/bmeg/grip/util"
	_ "github.com/lib/pq" // import so postgres will register as a sql driver
	//"google.golang.org/protobuf/encoding/protojson"
)

var configFile string
var gdb gdbi.GraphDB
var db gdbi.GraphInterface
var dbname string
var vertices = []*gripql.Vertex{}
var edges = []*gripql.Edge{}

var GraphQLAddr = "localhost:9080"
var GraphName = "test-graph"

func setupGraph() error {
	// sort edges/vertices and insert one at a time to ensure the same write order
	sort.Slice(vertices[:], func(i, j int) bool {
		return vertices[i].Gid < vertices[j].Gid
	})
	for _, v := range vertices {
		err := db.AddVertex([]*gdbi.Vertex{gdbi.NewElementFromVertex(v)})
		if err != nil {
			return err
		}
	}

	sort.Slice(edges[:], func(i, j int) bool {
		return edges[i].Gid < edges[j].Gid
	})
	for _, e := range edges {
		err := db.AddEdge([]*gdbi.Edge{gdbi.NewElementFromEdge(e)})
		if err != nil {
			return err
		}
	}

	return nil
}

func TestMain(m *testing.M) {
	flag.StringVar(&configFile, "config", configFile, "config file to use for tests")
	flag.Parse()

	configFile = "../" + configFile

	vertChan, err := util.StreamVerticesFromFile("../../conformance/graphs/swapi.vertices", 2)
	if err != nil {
		panic(err)
	}
	for v := range vertChan {
		vertices = append(vertices, v)
	}
	edgeChan, err := util.StreamEdgesFromFile("../../conformance/graphs/swapi.edges", 2)
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
	}

	config.TestifyConfig(conf)
	fmt.Printf("Test config: %+v\n", conf)
	dbconfig := conf.Drivers[conf.Default]

	if dbconfig.Badger != nil {
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

	err = gdb.AddGraph(GraphName)
	if err != nil {
		fmt.Println("Error: failed to add graph:", err)
		return
	}

	db, err = gdb.Graph(GraphName)
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

	srv, err := server.NewGripServer(conf, "./", map[string]gdbi.GraphDB{conf.Default: gdb})
	if err != nil {
		fmt.Println("Error: failed to init server", err)
		return
	}

	conn := gripql.WrapClient(gripql.NewQueryDirectClient(srv), gripql.NewEditDirectClient(srv), nil, nil)

	sch, err := schema.ScanSchema(conn, GraphName, 10, []string{})
	if err != nil {
		fmt.Println("Error: failed to init server", err)
		return
	}

	//fmt.Printf("Adding Schema: %s\n", protojson.Format(sch))
	err = conn.AddSchema(sch)
	if err != nil {
		fmt.Printf("Error: failed to add schema %s\n", err)
		return
	}
	gqlHandler, err := graphql.NewClientHTTPHandler(conn)

	httpServer := http.Server{Addr: GraphQLAddr, Handler: gqlHandler}

	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			// Error starting or closing listener:
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	defer os.RemoveAll(conf.Server.WorkDir)
	time.Sleep(time.Second)

	// run tests
	exit = m.Run()

	if err := httpServer.Shutdown(context.Background()); err != nil {
		// Error from closing listeners, or context timeout:
		log.Printf("HTTP server Shutdown: %v", err)
	}

}
