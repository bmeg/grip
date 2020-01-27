package main


import (
  "fmt"
  "os"
  "log"
  "context"
  "github.com/bmeg/grip/tabular"

  _ "github.com/bmeg/grip/tabular/tsv"

  "github.com/bmeg/grip/gdbi"
  "github.com/golang/protobuf/jsonpb"
  flag "github.com/spf13/pflag"

  "encoding/json"
  "strings"
  "github.com/bmeg/grip/gripql"
  gripqljs "github.com/bmeg/grip/gripql/javascript"
  "github.com/dop251/goja"
  "github.com/bmeg/grip/jsengine/underscore"

  "github.com/bmeg/grip/engine/pipeline"
  "github.com/bmeg/grip/util"

)


func ParseQuery(queryString string) (gripql.GraphQuery, error) {
  vm := goja.New()

  us, err := underscore.Asset("underscore.js")
  if err != nil {
    return gripql.GraphQuery{}, fmt.Errorf("failed to load underscore.js")
  }
  if _, err := vm.RunString(string(us)); err != nil {
    return gripql.GraphQuery{}, err
  }

  gripqlString, err := gripqljs.Asset("gripql.js")
  if err != nil {
    return gripql.GraphQuery{}, fmt.Errorf("failed to load gripql.js")
  }
  if _, err := vm.RunString(string(gripqlString)); err != nil {
    return gripql.GraphQuery{}, err
  }

  val, err := vm.RunString(queryString)
  if err != nil {
    return gripql.GraphQuery{}, err
  }

  queryJSON, err := json.Marshal(val)
  if err != nil {
    return gripql.GraphQuery{}, err
  }

  query := gripql.GraphQuery{}
  err = jsonpb.Unmarshal(strings.NewReader(string(queryJSON)), &query)
  if err != nil {
    return gripql.GraphQuery{}, err
  }
  return query, nil
}

func main() {
  var idxName *string = flag.String("db", "table.db", "Path to index db")
  flag.Parse()
  configFile := flag.Arg(0)
  queryString := flag.Arg(1)

  config, err := tabular.LoadConfig(configFile)
  if err != nil {
    log.Printf("%s", err)
    return
  }
  gdb, err := tabular.NewGDB(config, *idxName)
  if err != nil {
    log.Printf("%s", err)
    return
  }

  graph, _ := gdb.Graph("main")

  fmt.Printf("%s\n", queryString)
  fmt.Printf("%s\n", graph)

  query, err := ParseQuery(queryString)
  if err != nil {
    log.Printf("%s", err)
    return
  }
  log.Printf("Query: %s", query)
  Query(graph, query)

  gdb.Close()
}


func Query(graph gdbi.GraphInterface, query gripql.GraphQuery) error {
  marsh := jsonpb.Marshaler{}

  p, err := graph.Compiler().Compile(query.Query)
  if err != nil {
    return err
  }
  workdir := "./test.workdir." + util.RandomString(6)
  defer os.RemoveAll(workdir)
  res := pipeline.Run(context.Background(), p, workdir)

  for row := range res {
    rowString, _ := marsh.MarshalToString(row)
    fmt.Printf("%s\n", rowString)
  }
  return nil
}
