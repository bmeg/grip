package test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	"github.com/bmeg/grip/util"
)

var dbname string
var dbpath string
var kvdriver kvi.KVInterface
var gdb gdbi.GraphDB
var db gdbi.GraphInterface

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
	var exit = 1

	defer func() {
		os.Exit(exit)
	}()

	defer func() {
		os.RemoveAll(dbpath)
	}()

	conf := config.DefaultConfig()

	config.TestifyConfig(conf)
	fmt.Printf("Test config: %+v\n", conf)
	dbname = strings.ToLower(conf.Database)

	var err error
	gdb, err = kvgraph.NewKVGraphDB(dbname, conf.KVStorePath)
	if err != nil {
		//m.Error(err)
		return
	}
	defer func() {
		os.RemoveAll(conf.KVStorePath)
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

	// run tests
	exit = m.Run()
	if exit != 0 {
		return
	}

}
