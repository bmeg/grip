package arachne

import (
	"github.com/bmeg/arachne/boltdb"
	"github.com/bmeg/arachne/ophion"
	"golang.org/x/net/context"
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

func (server *ArachneServer) Traversal(ctx context.Context, query *ophion.GraphQuery) (*ophion.QueryResult, error) {
	return server.engine.RunTraversal(ctx, query)
}
