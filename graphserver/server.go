package graphserver

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	_ "github.com/bmeg/arachne/boltdb" // import so bolt will register itself
	"github.com/bmeg/arachne/kvgraph"
	"github.com/bmeg/arachne/mongo"
	_ "github.com/bmeg/arachne/rocksdb" // import so rocks will register itself
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
)

// ArachneServer is a GRPC based arachne server
type ArachneServer struct {
	engine GraphEngine
}

// NewArachneMongoServer initializes a GRPC server that uses the mongo driver
// to connect to the graph store
func NewArachneMongoServer(url string, database string) *ArachneServer {
	return &ArachneServer{
		engine: NewGraphEngine(mongo.NewArachne(url, database)),
	}
}

// NewArachneBadgerServer initializes a GRPC server that uses the badger driver
// to run the graph store
func NewArachneBadgerServer(baseDir string) *ArachneServer {
	return &ArachneServer{
		engine: NewGraphEngine(badgerdb.NewBadgerArachne(baseDir)),
	}
}

// NewArachneBoltServer initializes a GRPC server that uses the bolt driver
// to run the graph store
func NewArachneBoltServer(baseDir string) *ArachneServer {
	a, err := kvgraph.NewKVArachne("bolt", baseDir)
	if err != nil {
		return nil
	}
	return &ArachneServer{
		engine: NewGraphEngine(a),
	}
}

// NewArachneRocksServer initializes a GRPC server that uses the rocks driver
// to run the graph store. This may fail if the rocks driver was not compiled
// (using the --tags rocks flag)
func NewArachneRocksServer(baseDir string) *ArachneServer {
	a, err := kvgraph.NewKVArachne("rocks", baseDir)
	if err != nil {
		return nil
	}
	return &ArachneServer{
		engine: NewGraphEngine(a),
	}
}

// Start starts an asynchronous GRPC server
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

// CloseDB tells the driver to close connection or file
func (server *ArachneServer) CloseDB() {
	server.engine.Close()
}

// Traversal parses a traversal request and streams the results back
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

// GetGraphs returns a list of graphs managed by the driver
func (server *ArachneServer) GetGraphs(empty *aql.Empty, queryServer aql.Query_GetGraphsServer) error {
	log.Printf("Graph List")
	for _, name := range server.engine.GetGraphs() {
		queryServer.Send(&aql.ElementID{Graph: name})
	}
	return nil
}

// GetVertex returns a vertex given a aql.Element
func (server *ArachneServer) GetVertex(ctx context.Context, elem *aql.ElementID) (*aql.Vertex, error) {
	o := server.engine.GetVertex(elem.Graph, elem.Id)
	return o, nil
}

// GetEdge returns an edge given a aql.Element
func (server *ArachneServer) GetEdge(ctx context.Context, elem *aql.ElementID) (*aql.Edge, error) {
	o := server.engine.GetEdge(elem.Graph, elem.Id)
	return o, nil
}

// GetBundle returns a bundle given a aql.Element
func (server *ArachneServer) GetBundle(ctx context.Context, elem *aql.ElementID) (*aql.Bundle, error) {
	o := server.engine.GetBundle(elem.Graph, elem.Id)
	return o, nil
}

// DeleteGraph deletes a graph
func (server *ArachneServer) DeleteGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	server.engine.DeleteGraph(elem.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Graph}}, nil
}

// AddGraph creates a new graph on the server
func (server *ArachneServer) AddGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	server.engine.AddGraph(elem.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Graph}}, nil
}

// AddVertex adds a vertex to the graph
func (server *ArachneServer) AddVertex(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.engine.AddVertex(elem.Graph, *elem.Vertex)
	id = elem.Vertex.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddEdge adds an edge to the graph
func (server *ArachneServer) AddEdge(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.engine.AddEdge(elem.Graph, *elem.Edge)
	id = elem.Edge.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddBundle adds a bundle of edges to the graph
func (server *ArachneServer) AddBundle(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.engine.AddBundle(elem.Graph, *elem.Bundle)
	id = elem.Bundle.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddSubGraph adds a full subgraph to the graph in one post
func (server *ArachneServer) AddSubGraph(ctx context.Context, subgraph *aql.Graph) (*aql.EditResult, error) {
	for _, i := range subgraph.Vertices {
		if err := server.engine.AddVertex(subgraph.Graph, *i); err != nil {
			return nil, err
		}
	}
	for _, i := range subgraph.Edges {
		if err := server.engine.AddEdge(subgraph.Graph, *i); err != nil {
			return nil, err
		}
	}
	log.Printf("%d vertices and %d edges added to graph %s", len(subgraph.Vertices), len(subgraph.Edges), subgraph.Graph)
	id := subgraph.Graph
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// StreamElements takes a stream of inputs and loads them into the graph
func (server *ArachneServer) StreamElements(stream aql.Edit_StreamElementsServer) error {
	vertCount := 0
	edgeCount := 0
	bundleCount := 0
	for {
		element, err := stream.Recv()
		if err == io.EOF {
			if vertCount != 0 {
				log.Printf("%d vertices streamed", vertCount)
			}
			if edgeCount != 0 {
				log.Printf("%d edges streamed", edgeCount)
			}
			if bundleCount != 0 {
				log.Printf("%d bundles streamed", bundleCount)
			}
			return stream.SendAndClose(&aql.EditResult{Result: &aql.EditResult_Id{}})
		}
		if err != nil {
			log.Printf("Streaming Error: %s", err)
			return err
		}
		if element.Vertex != nil {
			server.AddVertex(context.Background(), element)
			vertCount++
		} else if element.Edge != nil {
			server.AddEdge(context.Background(), element)
			edgeCount++
		} else if element.Bundle != nil {
			server.AddBundle(context.Background(), element)
			bundleCount++
		}
	}
}

// DeleteVertex deletes a vertex from the server
func (server *ArachneServer) DeleteVertex(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(elem.Graph).DelVertex(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Id}}, nil
}

// DeleteEdge deletes an edge from the graph server
func (server *ArachneServer) DeleteEdge(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(elem.Graph).DelEdge(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Id}}, nil
}


func (server *ArachneServer) AddVertexIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(idx.Graph).AddVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) AddEdgeIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(idx.Graph).AddEdgeIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) DeleteVertexIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(idx.Graph).DeleteVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) DeleteEdgeIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.engine.Arachne.Graph(idx.Graph).AddEdgeIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) GetVertexIndex(idx *aql.IndexID, stream aql.Query_GetVertexIndexServer) error {
	res := server.engine.Arachne.Graph(idx.Graph).GetVertexTermCount(stream.Context(), idx.Label, idx.Field)
	for i := range res {
		l := i
		stream.Send(&l)
	}
	return nil
}

func (server *ArachneServer) GetEdgeIndex(idx *aql.IndexID, stream aql.Query_GetEdgeIndexServer) error {
	res := server.engine.Arachne.Graph(idx.Graph).GetEdgeTermCount(stream.Context(), idx.Label, idx.Field)
	for i := range res {
		l := i
		stream.Send(&l)
	}
	return nil
}

func (server *ArachneServer) VertexIndexTraversal(idx *aql.IndexQuery, stream aql.Query_VertexIndexTraversalServer) error {
	/*
	res := server.engine.Arachne.Graph(idx.Graph).GetVertexTermCount(stream.Context(), idx.Label, idx.Field)
	res, err := server.engine.RunTraversal(stream.Context(), query)
	if err != nil {
		return err
	}
	for i := range res {
		l := i
		queryServer.Send(&l)
	}
	*/
	return nil
}
