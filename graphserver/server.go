package graphserver

import (
	//"github.com/bmeg/arachne/boltdb"
	//"github.com/bmeg/arachne/rocksdb"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
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

/*
func (server *ArachneServer) Add(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	if x, ok := elem.GetElement().(*aql.GraphElement_Vertex); ok {
		server.engine.AddVertex(*x.Vertex)
		id = x.Vertex.Gid
	} else if x, ok := elem.GetElement().(*aql.GraphElement_Edge); ok {
		server.engine.AddEdge(*x.Edge)
		id = x.Edge.Gid
		//} else if x, ok := elem.GetElement().(*aql.AddElement_EdgeBundle); ok {
		//	server.engine.AddEdgeBundle(*x.EdgeBundle)
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{id}}, nil
}
*/

func (server *ArachneServer) GetGraphs(empty *aql.Empty, queryServer aql.Query_GetGraphsServer) error {
	for name := range server.engine.GetGraphs() {
		queryServer.Send(name)
	}
	return nil
}

func (server *ArachneServer) GetVertex(ctx context.Context, elem *aql.ElementID) (*aql.Vertex, error) {
	o := server.engine.Vertex(elem.Id)
	return o, nil
}

func (server *ArachneServer) GetEdge(ctx context.Context, elem *aql.ElementID) (*aql.Edge, error) {
	o := server.engine.Edge(elem.Id)
	return o, nil
}


func (server *ArachneServer) DeleteGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	//TODO: Add multiple graphs
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Id}}, nil
}

func (server *ArachneServer) AddGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	//TODO: Add multiple graphs
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Id}}, nil
}

func (server *ArachneServer) AddVertex(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	server.engine.AddVertex(*elem.Vertex)
	id = elem.Vertex.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{id}}, nil
}

func (server *ArachneServer) AddEdge(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	server.engine.AddEdge(*elem.Edge)
	id = elem.Edge.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{id}}, nil
}

func (server *ArachneServer) DeleteVertex(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.engine.DBI.DelVertex(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Id}}, nil
}

func (server *ArachneServer) DeleteEdge(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.engine.DBI.DelEdge(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Id}}, nil
}
