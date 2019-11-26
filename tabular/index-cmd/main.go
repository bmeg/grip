package main


import (
  "fmt"
  "log"
  "context"
  "github.com/bmeg/grip/tabular"
  "github.com/bmeg/grip/gdbi"
  "github.com/golang/protobuf/jsonpb"
  flag "github.com/spf13/pflag"
)


func main() {
  var idxName *string = flag.String("db", "table.db", "Path to index db")
  flag.Parse()
  configFile := flag.Arg(0)
  query := flag.Arg(1)

  config, err := tabular.LoadConfig(configFile)
  if err != nil {
    log.Printf("%s", err)
    return
  }
  graph := tabular.NewGraph(config, *idxName)

  fmt.Printf("%s\n", query)
  fmt.Printf("%s\n", graph)

  Query(graph)
}


func Query(graph gdbi.GraphInterface) {
  marsh := jsonpb.Marshaler{}
  for row := range graph.GetVertexList(context.Background(), false) {
    rowString, _ := marsh.MarshalToString(row)
    fmt.Printf("%s\n", rowString)
  }
}
