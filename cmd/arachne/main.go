
package main

import (
  "os"
  "log"
  "flag"
	"path/filepath"
  "github.com/bmeg/arachne"
)

func main() {
  httpPort := flag.String("port", "8000", "HTTP Port")
	rpcPort := flag.String("rpc", "9090", "TCP+RPC Port")
  
  dir, _ := filepath.Abs(os.Args[0])
	contentDir := filepath.Join(dir, "..", "..", "share")
  
  log.Printf("Starting Server")
  
  server := arachne.NewArachneServer("./graph.db")
  server.Start(*rpcPort)
  arachne.StartHttpProxy(*rpcPort, *httpPort, contentDir)
}