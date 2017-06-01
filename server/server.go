package arachne

import (
	//"github.com/bmeg/arachne/boltdb"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/rocksdb"
	//"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type ArachneServer struct {
	engine GraphEngine
}

// TODO: documentation
func NewArachneServer(baseDir string) *ArachneServer {
	return &ArachneServer{
		//engine: NewGraphEngine(boltdb.NewBoltArachne(baseDir)),
		engine: NewGraphEngine(rocksdb.NewRocksArachne(baseDir)),
	}
}

// TODO: documentation
func (server *ArachneServer) Start(hostPort string) {
	lis, err := net.Listen("tcp", ":"+hostPort)
	if err != nil {
		panic("Cannot open port")
	}
	grpcServer := grpc.NewServer()

	aql.RegisterQueryServer(grpcServer, server)

	log.Println("TCP+RPC server listening on " + hostPort)
	go grpcServer.Serve(lis)
}

func (server *ArachneServer) Traversal(query *aql.GraphQuery, queryServer aql.Query_TraversalServer) error {
	res, err := server.engine.RunTraversal(queryServer.Context(), query)
	if err != nil {
		return err
	}
	for i := range res {
		l := i
		queryServer.Send(&l)
	}
	return nil
}
