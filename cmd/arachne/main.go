package main

import (
	"flag"
	"github.com/bmeg/arachne"
	"log"
	"os"
	"path/filepath"
)

func main() {
	httpPort := flag.String("port", "8000", "HTTP Port")
	rpcPort := flag.String("rpc", "9090", "TCP+RPC Port")
	dbPath := flag.String("db", "graph.db", "DB Path")

	dir, _ := filepath.Abs(os.Args[0])
	contentDir := filepath.Join(dir, "..", "..", "share")

	flag.Parse()

	log.Printf("Starting Server")

	server := arachne.NewArachneServer(*dbPath)
	server.Start(*rpcPort)
	arachne.StartHttpProxy(*rpcPort, *httpPort, contentDir)
}
