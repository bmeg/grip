package graphserver

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/badgerdb"
	_ "github.com/bmeg/arachne/boltdb" // import so bolt will register itself
	"github.com/bmeg/arachne/engine"
	"github.com/bmeg/arachne/gdbi"
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
	db gdbi.DBI
}

// NewArachneMongoServer initializes a GRPC server that uses the mongo driver
// to connect to the graph store
func NewArachneMongoServer(url string, database string) *ArachneServer {
	db := mongo.NewMongo(url, database)
	return &ArachneServer{db: db}
}

// NewArachneBadgerServer initializes a GRPC server that uses the badger driver
// to run the graph store
func NewArachneBadgerServer(baseDir string) *ArachneServer {
	db := badgerdb.NewBadgerArachne(baseDir)
	return &ArachneServer{db: db}
}

// NewArachneBoltServer initializes a GRPC server that uses the bolt driver
// to run the graph store
func NewArachneBoltServer(baseDir string) *ArachneServer {
	db, err := kvgraph.NewKVArachne("bolt", baseDir)
	if err != nil {
		return nil
	}
	return &ArachneServer{db: db}
}

// NewArachneRocksServer initializes a GRPC server that uses the rocks driver
// to run the graph store. This may fail if the rocks driver was not compiled
// (using the --tags rocks flag)
func NewArachneRocksServer(baseDir string) *ArachneServer {
	db, err := kvgraph.NewKVArachne("rocks", baseDir)
	if err != nil {
		return nil
	}
	return &ArachneServer{db: db}
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
	server.db.Close()
}

// Traversal parses a traversal request and streams the results back
func (server *ArachneServer) Traversal(query *aql.GraphQuery, queryServer aql.Query_TraversalServer) error {
	res, err := engine.RunTraversal(queryServer.Context(), query, server.db.Graph(query.Graph))
	if err != nil {
		return err
	}
	for row := range res {
		queryServer.Send(row)
	}
	return nil
}

// GetGraphs returns a list of graphs managed by the driver
func (server *ArachneServer) GetGraphs(empty *aql.Empty, queryServer aql.Query_GetGraphsServer) error {
	log.Printf("Graph List")
	for _, name := range server.db.GetGraphs() {
		queryServer.Send(&aql.ElementID{Graph: name})
	}
	return nil
}

// GetVertex returns a vertex given a aql.Element
func (server *ArachneServer) GetVertex(ctx context.Context, elem *aql.ElementID) (*aql.Vertex, error) {
	o := server.db.Graph(elem.Graph).GetVertex(elem.Id, true)
	return o, nil
}

// GetEdge returns an edge given a aql.Element
func (server *ArachneServer) GetEdge(ctx context.Context, elem *aql.ElementID) (*aql.Edge, error) {
	o := server.db.Graph(elem.Graph).GetEdge(elem.Id, true)
	return o, nil
}

// GetBundle returns a bundle given a aql.Element
func (server *ArachneServer) GetBundle(ctx context.Context, elem *aql.ElementID) (*aql.Bundle, error) {
	o := server.db.Graph(elem.Graph).GetBundle(elem.Id, true)
	return o, nil
}

// DeleteGraph deletes a graph
func (server *ArachneServer) DeleteGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	server.db.DeleteGraph(elem.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Graph}}, nil
}

// AddGraph creates a new graph on the server
func (server *ArachneServer) AddGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	server.db.AddGraph(elem.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Graph}}, nil
}

// AddVertex adds a vertex to the graph
func (server *ArachneServer) AddVertex(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.db.Graph(elem.Graph).AddVertex(elem.Vertex)
	id = elem.Vertex.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddEdge adds an edge to the graph
func (server *ArachneServer) AddEdge(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.db.Graph(elem.Graph).AddEdge(elem.Edge)
	id = elem.Edge.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddBundle adds a bundle of edges to the graph
func (server *ArachneServer) AddBundle(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.db.Graph(elem.Graph).AddBundle(elem.Bundle)
	id = elem.Bundle.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddSubGraph adds a full subgraph to the graph in one post
func (server *ArachneServer) AddSubGraph(ctx context.Context, subgraph *aql.Graph) (*aql.EditResult, error) {
	for _, i := range subgraph.Vertices {
		server.db.Graph(subgraph.Graph).AddVertex(i)
	}
	for _, i := range subgraph.Edges {
		server.db.Graph(subgraph.Graph).AddEdge(i)
	}
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
	err := server.db.Graph(elem.Graph).DelVertex(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Id}}, nil
}

// DeleteEdge deletes an edge from the graph server
func (server *ArachneServer) DeleteEdge(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.db.Graph(elem.Graph).DelEdge(elem.Id)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Id}}, nil
}
