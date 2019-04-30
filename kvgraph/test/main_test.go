package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
	_ "github.com/bmeg/grip/kvi/rocksdb"  // import so rocks will register itself
	"github.com/bmeg/grip/util"
)

var dbname string
var dbpath string
var kvdriver kvi.KVInterface

func resetKVInterface() {
	err := os.RemoveAll(dbpath)
	if err != nil {
		panic(err)
	}
	dbpath = "test.db." + util.RandomString(6)
	kvdriver, err = kvgraph.NewKVInterface(dbname, dbpath, nil)
	if err != nil {
		panic(err)
	}
}

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

func TestMain(m *testing.M) {
	var err error
	var exit = 1

	defer func() {
		os.Exit(exit)
	}()

	defer func() {
		os.RemoveAll(dbpath)
	}()

	for _, dbname = range []string{"badger", "bolt", "level", "rocks"} {
		dbpath = "test.db." + util.RandomString(6)

		kvdriver, err = kvgraph.NewKVInterface(dbname, dbpath, nil)
		if err != nil {
			if dbname == "rocks" {
				fmt.Println(`Warning: rocks driver not found; run test with "-tags rocksdb"`)
				exit = 0
				return
			}
			fmt.Println("Error: failed to initialize database driver:", err)
			exit = 1
			return
		}

		// run tests
		exit = m.Run()
		if exit != 0 {
			return
		}
		os.RemoveAll(dbpath)
	}
}
