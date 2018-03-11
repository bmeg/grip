package graphserver

import (
	"fmt"
	"github.com/bmeg/arachne/aql"
	_ "github.com/bmeg/arachne/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/arachne/boltdb"   // import so bolt will register itself
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
	db      gdbi.GraphDB
	workDir string
}

// NewArachneMongoServer initializes a GRPC server that uses the mongo driver
// to connect to the graph store
func NewArachneMongoServer(url string, database string, workDir string) *ArachneServer {
	db := mongo.NewMongo(url, database)
	return &ArachneServer{db: db, workDir: workDir}
}

// NewArachneBadgerServer initializes a GRPC server that uses the badger driver
// to run the graph store
func NewArachneBadgerServer(baseDir string, workDir string) *ArachneServer {
	a, err := kvgraph.NewKVArachne("badger", baseDir)
	if err != nil {
		log.Printf("Error Starting Badger")
		return nil
	}
	return &ArachneServer{
		db:      a,
		workDir: workDir,
	}
}

// NewArachneBoltServer initializes a GRPC server that uses the bolt driver
// to run the graph store
func NewArachneBoltServer(baseDir string, workDir string) *ArachneServer {
	db, err := kvgraph.NewKVArachne("bolt", baseDir)
	if err != nil {
		return nil
	}
	return &ArachneServer{db: db, workDir: workDir}
}

// NewArachneRocksServer initializes a GRPC server that uses the rocks driver
// to run the graph store. This may fail if the rocks driver was not compiled
// (using the --tags rocks flag)
func NewArachneRocksServer(baseDir string, workDir string) *ArachneServer {
	db, err := kvgraph.NewKVArachne("rocks", baseDir)
	if err != nil {
		return nil
	}
	return &ArachneServer{db: db, workDir: workDir}
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

	pipeline, err := engine.Compile(query.Query, server.db.Graph(query.Graph), server.workDir)
	if err != nil {
		return err
	}
	//log.Printf("Running: %s", pipeline)
	res := pipeline.Run()
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

// GetTimestamp returns the update timestamp of a graph
func (server *ArachneServer) GetTimestamp(ctx context.Context, elem *aql.ElementID) (*aql.Timestamp, error) {
	o := aql.Timestamp{
		Timestamp: server.db.Graph(elem.Graph).GetTimestamp(),
	}
	return &o, nil
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
	server.db.Graph(elem.Graph).AddVertex([]*aql.Vertex{elem.Vertex})
	id = elem.Vertex.Gid
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

// AddEdge adds an edge to the graph
func (server *ArachneServer) AddEdge(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	var id string
	server.db.Graph(elem.Graph).AddEdge([]*aql.Edge{elem.Edge})
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
	if err := server.db.Graph(subgraph.Graph).AddVertex(subgraph.Vertices); err != nil {
		return nil, err
	}
	if err := server.db.Graph(subgraph.Graph).AddEdge(subgraph.Edges); err != nil {
		return nil, err
	}
	log.Printf("%d vertices and %d edges added to graph %s", len(subgraph.Vertices), len(subgraph.Edges), subgraph.Graph)
	id := subgraph.Graph
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: id}}, nil
}

type graphElementArray struct {
	graph    string
	vertices []*aql.Vertex
	edges    []*aql.Edge
}

func newGraphElementArray(name string, vertexBufSize, edgeBufSize int) *graphElementArray {
	if vertexBufSize != 0 {
		return &graphElementArray{graph: name, vertices: make([]*aql.Vertex, 0, vertexBufSize)}
	}
	if edgeBufSize != 0 {
		return &graphElementArray{graph: name, edges: make([]*aql.Edge, 0, edgeBufSize)}
	}
	return nil
}

// StreamElements takes a stream of inputs and loads them into the graph
func (server *ArachneServer) StreamElements(stream aql.Edit_StreamElementsServer) error {
	vertexBatchSize := 50
	edgeBatchSize := 50

	vertCount := 0
	edgeCount := 0
	bundleCount := 0

	vertexBatchChan := make(chan *graphElementArray)
	edgeBatchChan := make(chan *graphElementArray)
	closeChan := make(chan bool)

	go func() {
		for vBatch := range vertexBatchChan {
			err := server.db.Graph(vBatch.graph).AddVertex(vBatch.vertices)
			if err != nil {
				log.Printf("Insert Error: %s", err)
			}
		}
		closeChan <- true
	}()
	go func() {
		for eBatch := range edgeBatchChan {
			err := server.db.Graph(eBatch.graph).AddEdge(eBatch.edges)
			if err != nil {
				log.Printf("Insert Error: %s", err)
			}
		}
		closeChan <- true
	}()

	vertexBatch := newGraphElementArray("", vertexBatchSize, 0)
	edgeBatch := newGraphElementArray("", 0, edgeBatchSize)
	var loopErr error
	for loopErr == nil {
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
			vertexBatchChan <- vertexBatch
			edgeBatchChan <- edgeBatch
			loopErr = err
		} else if err != nil {
			log.Printf("Streaming Error: %s", err)
			loopErr = err
		} else {
			if element.Vertex != nil {
				if vertexBatch.graph != element.Graph || len(vertexBatch.vertices) >= vertexBatchSize {
					vertexBatchChan <- vertexBatch
					vertexBatch = newGraphElementArray(element.Graph, vertexBatchSize, 0)
				}
				v := *element.Vertex
				vertexBatch.vertices = append(vertexBatch.vertices, &v)
				vertCount++
			} else if element.Edge != nil {
				if edgeBatch.graph != element.Graph || len(edgeBatch.edges) >= edgeBatchSize {
					edgeBatchChan <- edgeBatch
					edgeBatch = newGraphElementArray(element.Graph, 0, edgeBatchSize)
				}
				edgeBatch.edges = append(edgeBatch.edges, element.Edge)
				edgeCount++
			} else if element.Bundle != nil {
				server.AddBundle(context.Background(), element)
				bundleCount++
			}
		}
	}

	close(edgeBatchChan)
	close(vertexBatchChan)
	<-closeChan
	<-closeChan

	if loopErr != io.EOF {
		return loopErr
	}
	return stream.SendAndClose(&aql.EditResult{Result: &aql.EditResult_Id{}})
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

func (server *ArachneServer) AddVertexIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.db.Graph(idx.Graph).AddVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) AddEdgeIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.db.Graph(idx.Graph).AddEdgeIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) DeleteVertexIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.db.Graph(idx.Graph).DeleteVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) DeleteEdgeIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	err := server.db.Graph(idx.Graph).AddEdgeIndex(idx.Label, idx.Field)
	if err != nil {
		return &aql.EditResult{Result: &aql.EditResult_Error{Error: fmt.Sprintf("%s", err)}}, nil
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

func (server *ArachneServer) GetVertexIndex(idx *aql.IndexID, stream aql.Query_GetVertexIndexServer) error {
	res := server.db.Graph(idx.Graph).GetVertexTermCount(stream.Context(), idx.Label, idx.Field)
	for i := range res {
		l := i
		stream.Send(&l)
	}
	return nil
}

func (server *ArachneServer) GetEdgeIndex(idx *aql.IndexID, stream aql.Query_GetEdgeIndexServer) error {
	res := server.db.Graph(idx.Graph).GetEdgeTermCount(stream.Context(), idx.Label, idx.Field)
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
