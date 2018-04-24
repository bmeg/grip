package test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	_ "github.com/bmeg/arachne/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/arachne/boltdb"   // import so bolt will register itself
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/kvi"
	_ "github.com/bmeg/arachne/leveldb" // import so level will register itself
	_ "github.com/bmeg/arachne/rocksdb" // import so rocks will register itself
	"github.com/bmeg/arachne/util"
)

var dbname = "badger"
var dbpath string
var kvdriver kvi.KVInterface

func init() {
	flag.StringVar(&dbname, "db", dbname, "database to use for tests")
	flag.Parse()
}

func resetKVInterface() {
	var err error
	err = os.RemoveAll(dbpath)
	if err != nil {
		panic(err)
	}
	dbpath = "test.db." + util.RandomString(6)
	kvdriver, err = kvgraph.NewKVInterface(dbname, dbpath)
	if err != nil {
		panic(err)
	}
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

	kvdriver, err = kvgraph.NewKVInterface(dbname, dbpath)
	if err != nil {
		fmt.Println("Error: failed to initialize database driver:", err)
		return
	}

	// run tests
	exit = m.Run()
}
