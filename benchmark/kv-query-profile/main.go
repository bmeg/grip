package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsengine/underscore"
	"github.com/bmeg/grip/kvgraph"
	"github.com/bmeg/grip/kvi"
	"github.com/dop251/goja"
	"github.com/golang/protobuf/jsonpb"

	gripqljs "github.com/bmeg/grip/gripql/javascript"

	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
)

func main() {
	flag.Parse()
	dbPath := flag.Arg(0)
	graphName := flag.Arg(1)
	queryString := flag.Arg(2)

	log.Printf("Starting Profile")

	kv, err := kvi.NewKVInterface("badger", dbPath, nil)
	if err != nil {
		return
	}
	db := kvgraph.NewKVGraph(kv)
	defer kv.Close()

	vm := goja.New()

	us, err := underscore.Asset("underscore.js")
	if err != nil {
		log.Printf("failed to load underscore.js")
		return
	}
	if _, err := vm.RunString(string(us)); err != nil {
		log.Printf("%s", err)
		return
	}

	gripqlString, err := gripqljs.Asset("gripql.js")
	if err != nil {
		fmt.Print("failed to load gripql.js")
		return
	}
	if _, err := vm.RunString(string(gripqlString)); err != nil {
		log.Printf("%s", err)
		return
	}

	val, err := vm.RunString(queryString)
	if err != nil {
		log.Printf("%s", err)
		return
	}

	queryJSON, err := json.Marshal(val)
	if err != nil {
		log.Printf("%s", err)
		return
	}

	query := gripql.GraphQuery{}
	err = jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
	if err != nil {
		log.Printf("%s", err)
		return
	}
	kgraph, err := db.Graph(graphName)
	if err != nil {
		log.Printf("%s", err)
		return
	}
	comp := kgraph.Compiler()
	pipe, err := comp.Compile(query.Query)
	if err != nil {
		log.Printf("%s", err)
		return
	}

	log.Printf("Starting Query")
	f, err := os.Create("query.cpu_profile")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	start := time.Now()
	o := pipeline.Run(context.Background(), pipe, "tmp-work")
	count := 0
	for range o {
		count++
	}
	t := time.Now()
	elapsed := t.Sub(start)
	log.Printf("Time: %s", elapsed)
	pprof.StopCPUProfile()
	log.Printf("Found: %d rows", count)
}
