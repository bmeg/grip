package graphserver

import (
	//"github.com/bmeg/arachne/boltdb"
	//"github.com/bmeg/arachne/rocksdb"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	"github.com/bmeg/arachne/mongo"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
)

type ArachneServer struct {
	engine GraphEngine
}

func NewArachneMongoServer(url string, database string) *ArachneServer {
	return &ArachneServer{
		engine: NewGraphEngine(mongo.NewMongoArachne(url, database)),
	}
}

func NewArachneBadgerServer(baseDir string) *ArachneServer {
	return &ArachneServer{
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

func (server *ArachneServer) CloseDB() {
	server.engine.Close()
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

func (server *ArachneServer) GetGraphs(empty *aql.Empty, queryServer aql.Query_GetGraphsServer) error {
	log.Printf("Graph List")
	for _, name := range server.engine.GetGraphs() {
		queryServer.Send(&aql.ElementID{Graph: name})
	}
	return nil
}

func (server *ArachneServer) GetVertex(ctx context.Context, elem *aql.ElementID) (*aql.Vertex, error) {
	o := server.engine.GetVertex(elem.Graph, elem.Id)
	return o, nil
}

func (server *ArachneServer) GetEdge(ctx context.Context, elem *aql.ElementID) (*aql.Edge, error) {
	o := server.engine.GetEdge(elem.Graph, elem.Id)
	return o, nil
}

func (server *ArachneServer) GetBundle(ctx context.Context, elem *aql.ElementID) (*aql.Bundle, error) {
	o := server.engine.GetBundle(elem.Graph, elem.Id)
	return o, nil
}

func (server *ArachneServer) DeleteGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	server.engine.DeleteGraph(elem.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Graph}}, nil
}

func (server *ArachneServer) AddGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	server.engine.AddGraph(elem.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Graph}}, nil
}

func (server *ArachneServer) AddVertex(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	server.engine.AddVertex(elem.Graph, *elem.Vertex)
	id = elem.Vertex.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{id}}, nil
}

func (server *ArachneServer) AddEdge(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	server.engine.AddEdge(elem.Graph, *elem.Edge)
	id = elem.Edge.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{id}}, nil
}

func (server *ArachneServer) AddBundle(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string = ""
	server.engine.AddBundle(elem.Graph, *elem.Bundle)
	id = elem.Bundle.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{id}}, nil
}

func (server *ArachneServer) StreamElements(stream aql.Edit_StreamElementsServer) error {
	vert_count := 0
	edge_count := 0
	bundle_count := 0
	for {
		element, err := stream.Recv()
		if err == io.EOF {
			if vert_count != 0 {
				log.Printf("%d vertices streamed", vert_count)
			}
			if edge_count != 0 {
				log.Printf("%d edges streamed", edge_count)
			}
			if bundle_count != 0 {
				log.Printf("%d bundles streamed", bundle_count)
			}
			return stream.SendAndClose(&aql.EditResult{Result: &aql.EditResult_Id{}})
		}
		if err != nil {
			log.Printf("Streaming Error: %s", err)
			return err
		}
		if element.Vertex != nil {
			server.AddVertex(context.Background(), element)
			vert_count += 1
		} else if element.Edge != nil {
			server.AddEdge(context.Background(), element)
			edge_count += 1
		} else if element.Bundle != nil {
			server.AddBundle(context.Background(), element)
			bundle_count += 1
		}
	}

}

func (server *ArachneServer) DeleteVertex(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(elem.Graph).DelVertex(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Id}}, nil
}

func (server *ArachneServer) DeleteEdge(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(elem.Graph).DelEdge(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{elem.Id}}, nil
}
