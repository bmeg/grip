package test

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
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
	"github.com/jmoiron/sqlx"
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

func parsePostgresDSN(dsn string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Fields(dsn) {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) == 2 {
			out[strings.TrimSpace(pair[0])] = strings.TrimSpace(pair[1])
		}
	}
	return out
}

func postgresCleanupConnection(conf config.DriverConfig) (string, []string, error) {
	dbNames := []string{}
	host := "localhost"
	port := uint(5432)
	user := "postgres"
	password := ""
	sslMode := "disable"

	if conf.PSQL != nil {
		host = conf.PSQL.Host
		port = conf.PSQL.Port
		user = conf.PSQL.User
		password = conf.PSQL.Password
		sslMode = conf.PSQL.SSLMode
		if conf.PSQL.DBName != "" {
			dbNames = append(dbNames, conf.PSQL.DBName)
		}
	}

	if conf.ExistingSQL != nil {
		params := parsePostgresDSN(conf.ExistingSQL.DataSourceName)
		if v := params["host"]; v != "" {
			host = v
		}
		if v := params["port"]; v != "" {
			if p, err := strconv.ParseUint(v, 10, 32); err == nil {
				port = uint(p)
			}
		}
		if v := params["user"]; v != "" {
			user = v
		}
		if v := params["password"]; v != "" {
			password = v
		}
		if v := params["sslmode"]; v != "" {
			sslMode = v
		}
		if v := params["dbname"]; v != "" {
			dbNames = append(dbNames, v)
		}
	}

	if len(dbNames) == 0 {
		return "", nil, nil
	}

	uniq := map[string]bool{}
	filtered := []string{}
	for _, name := range dbNames {
		if name == "" || uniq[name] {
			continue
		}
		uniq[name] = true
		filtered = append(filtered, name)
	}

	connStr, err := util.BuildPostgresConnStr(host, port, user, password, "postgres", sslMode)
	if err != nil {
		return "", nil, err
	}
	return connStr, filtered, nil
}

func cleanupPostgresDatabases(conf config.DriverConfig) error {
	connStr, dbNames, err := postgresCleanupConnection(conf)
	if err != nil || connStr == "" || len(dbNames) == 0 {
		return err
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, name := range dbNames {
		safeName := strings.ReplaceAll(name, `"`, `""`)
		_, _ = db.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", name)
		_, _ = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS \"%s\"", safeName))
	}

	return nil
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
	defer func() {
		if gdb != nil {
			if err := gdb.Close(); err != nil {
				fmt.Printf("cleanup warning: failed to close graph db: %v\n", err)
			}
		}
		if dbconfig.PSQL != nil || dbconfig.ExistingSQL != nil {
			if err := cleanupPostgresDatabases(dbconfig); err != nil {
				fmt.Printf("cleanup warning: failed to clean postgres databases: %v\n", err)
			}
		}
	}()

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
