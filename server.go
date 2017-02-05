package arachne

import (
	"github.com/bmeg/arachne/boltdb"
	"github.com/bmeg/arachne/ophion"
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
		engine: NewGraphEngine(boltdb.NewBoltArachne(baseDir)),
	}
}

// TODO: documentation
func (server *ArachneServer) Start(hostPort string) {
	lis, err := net.Listen("tcp", ":"+hostPort)
	if err != nil {
		panic("Cannot open port")
	}
	grpcServer := grpc.NewServer()

	ophion.RegisterQueryServer(grpcServer, server)

	log.Println("TCP+RPC server listening on " + hostPort)
	go grpcServer.Serve(lis)
}

func (server *ArachneServer) Traversal(query *ophion.GraphQuery, queryServer ophion.Query_TraversalServer) error {
	res, _ := server.engine.RunTraversal(query)
	for i := range res {
		l := i
		queryServer.Send(&ophion.ResultRow{Value: &l})
	}
	return nil
}
