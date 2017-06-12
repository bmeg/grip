package graphserver

import (
	//"github.com/bmeg/arachne/boltdb"
	//"github.com/bmeg/arachne/rocksdb"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/aql"
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
		//engine: NewGraphEngine(boltdb.NewBoltArachne(baseDir)),
		//engine: NewGraphEngine(rocksdb.NewRocksArachne(baseDir)),
		engine: NewGraphEngine(badgerdb.NewBadgerArachne(baseDir)),
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
	aql.RegisterEditServer(grpcServer, server) //TODO config for read only
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


func (server *ArachneServer) Add(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	if x, ok := elem.GetElement().(*aql.GraphElement_Vertex); ok {
		server.engine.AddVertex(*x.Vertex)
		id = x.Vertex.Gid
	}	else if x, ok := elem.GetElement().(*aql.GraphElement_Edge); ok {
		server.engine.AddEdge(*x.Edge)
		id = x.Edge.Gid
	//} else if x, ok := elem.GetElement().(*aql.AddElement_EdgeBundle); ok {
	//	server.engine.AddEdgeBundle(*x.EdgeBundle)
	}
	return &aql.EditResult{Result:&aql.EditResult_Id{id}}, nil
}
