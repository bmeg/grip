package test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	_ "github.com/bmeg/arachne/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/arachne/boltdb"   // import so bolt will register itself
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/kvgraph"
	_ "github.com/bmeg/arachne/leveldb" // import so level will register itself
	_ "github.com/bmeg/arachne/rocksdb" // import so rocks will register itself
	"github.com/bmeg/arachne/util"
)

var dbname = "badger"
var dbpath string
var gdb gdbi.GraphDB
var db gdbi.GraphInterface

func init() {
	flag.StringVar(&dbname, "db", dbname, "database to use for tests")
	flag.Parse()
}

func TestMain(m *testing.M) {
	var err error
	var exit = 1

	defer func() {
		os.Exit(exit)
	}()

	if dbname == "" {
		fmt.Println("Error: you must specify which database to test using the flag '-db'")
		return
	}

	dbpath = "test.db." + util.RandomString(6)
	defer func() {
		os.RemoveAll(dbpath)
	}()

	gdb, err = kvgraph.NewKVGraphDB(dbname, dbpath)
	if err != nil {
		fmt.Println("Error: failed to initialize database driver:", err)
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
