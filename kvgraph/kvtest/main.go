package main

import (
  "fmt"
  "flag"
  "sync"

  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/util"
  "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
  "github.com/bmeg/grip/kvi"
  log "github.com/sirupsen/logrus"
)

var logRate = 10000
var graph = "testgraph"

func main() {
  flag.Parse()
  fmt.Printf("Howdy\n")

  elemChan := make(chan *gripql.GraphElement)

  sg := &sync.WaitGroup{}
  sg.Add(1)
  go func() {
    for range elemChan {}
    sg.Done()
  }()

  count := 0
  db, err := badgerdb.NewKVInterface("test.db", kvi.Options{})
  if err != nil {
    log.Errorf("issue: %s", err)
    return
  }
  vertexFile := flag.Arg(0)
  for v := range util.StreamVerticesFromFile(vertexFile) {
    count++
    if count%logRate == 0 {
      log.Infof("Loaded %d vertices", count)
    }
    elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
  }
  close(elemChan)
  sg.Wait()
  db.Close()
}
