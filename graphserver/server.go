package graphserver

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine"
	"github.com/bmeg/arachne/gdbi"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// ArachneServer is a GRPC based arachne server
type ArachneServer struct {
	db       gdbi.GraphDB
	workDir  string
	readOnly bool
}

// NewArachneServer initializes a GRPC server to connect to the graph store
func NewArachneServer(db gdbi.GraphDB, workDir string, readonly bool) *ArachneServer {
	return &ArachneServer{db: db, workDir: workDir, readOnly: readonly}
}

// Start starts an asynchronous GRPC server
func (server *ArachneServer) Start(hostPort string) error {
	lis, err := net.Listen("tcp", ":"+hostPort)
	if err != nil {
		return fmt.Errorf("Cannot open port: %v", err)
	}
	grpcServer := grpc.NewServer()
	aql.RegisterQueryServer(grpcServer, server)
	if !server.readOnly {
		aql.RegisterEditServer(grpcServer, server)
	}
	log.Println("TCP+RPC server listening on " + hostPort)
	go grpcServer.Serve(lis)
	return nil
}

// CloseDB tells the driver to close connection or file
func (server *ArachneServer) CloseDB() {
	server.db.Close()
}

// Traversal parses a traversal request and streams the results back
func (server *ArachneServer) Traversal(query *aql.GraphQuery, queryServer aql.Query_TraversalServer) error {
	graph, err := server.db.Graph(query.Graph)
	if err != nil {
		return err
	}
	compiler := graph.Compiler()
	pipeline, err := compiler.Compile(query.Query)
	if err != nil {
		return err
	}
	res := engine.Run(queryServer.Context(), pipeline, server.workDir)
	for row := range res {
		queryServer.Send(row)
	}

	return nil
}

// GetGraphs returns a list of graphs managed by the driver
func (server *ArachneServer) GetGraphs(empty *aql.Empty, queryServer aql.Query_GetGraphsServer) error {
	for _, name := range server.db.GetGraphs() {
		queryServer.Send(&aql.ElementID{Graph: name})
	}
	return nil
}

// GetVertex returns a vertex given a aql.Element
func (server *ArachneServer) GetVertex(ctx context.Context, elem *aql.ElementID) (*aql.Vertex, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	o := graph.GetVertex(elem.Id, true)
	if o == nil {
		return nil, grpc.Errorf(codes.NotFound, fmt.Sprintf("vertex %s not found", elem.Id))
	}
	return o, nil
}

// GetEdge returns an edge given a aql.Element
func (server *ArachneServer) GetEdge(ctx context.Context, elem *aql.ElementID) (*aql.Edge, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	o := graph.GetEdge(elem.Id, true)
	if o == nil {
		return nil, grpc.Errorf(codes.NotFound, fmt.Sprintf("edge %s not found", elem.Id))
	}
	return o, nil
}

// GetTimestamp returns the update timestamp of a graph
func (server *ArachneServer) GetTimestamp(ctx context.Context, elem *aql.ElementID) (*aql.Timestamp, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &aql.Timestamp{Timestamp: graph.GetTimestamp()}, nil
}

// DeleteGraph deletes a graph
func (server *ArachneServer) DeleteGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.db.DeleteGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Graph}}, nil
}

// AddGraph creates a new graph on the server
func (server *ArachneServer) AddGraph(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	err := server.db.AddGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Graph}}, err
}

// AddVertex adds a vertex to the graph
func (server *ArachneServer) AddVertex(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.AddVertex([]*aql.Vertex{elem.Vertex})
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Vertex.Gid}}, nil
}

// AddEdge adds an edge to the graph
func (server *ArachneServer) AddEdge(ctx context.Context, elem *aql.GraphElement) (*aql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.AddEdge([]*aql.Edge{elem.Edge})
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Edge.Gid}}, nil
}

// AddSubGraph adds a full subgraph to the graph in one post
func (server *ArachneServer) AddSubGraph(ctx context.Context, subgraph *aql.Graph) (*aql.EditResult, error) {
	graph, err := server.db.Graph(subgraph.Graph)
	if err != nil {
		return nil, err
	}
	if err := graph.AddVertex(subgraph.Vertices); err != nil {
		return nil, err
	}
	if err := graph.AddEdge(subgraph.Edges); err != nil {
		return nil, err
	}
	log.Printf("%d vertices and %d edges added to graph %s", len(subgraph.Vertices), len(subgraph.Edges), subgraph.Graph)
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: subgraph.Graph}}, nil
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

	vertexBatchChan := make(chan *graphElementArray)
	edgeBatchChan := make(chan *graphElementArray)
	closeChan := make(chan bool)

	go func() {
		for vBatch := range vertexBatchChan {
			if len(vBatch.vertices) > 0 && vBatch.graph != "" {
				graph, err := server.db.Graph(vBatch.graph)
				if err != nil {
					log.Printf("Insert error: %s", err)
				}
				err = graph.AddVertex(vBatch.vertices)
				if err != nil {
					log.Printf("Insert error: %s", err)
				}
			}
		}
		closeChan <- true
	}()

	go func() {
		for eBatch := range edgeBatchChan {
			if len(eBatch.edges) > 0 && eBatch.graph != "" {
				graph, err := server.db.Graph(eBatch.graph)
				if err != nil {
					log.Printf("Insert error: %s", err)
				}
				err = graph.AddEdge(eBatch.edges)
				if err != nil {
					log.Printf("Insert error: %s", err)
				}
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
			vertexBatchChan <- vertexBatch
			edgeBatchChan <- edgeBatch
			loopErr = err
		} else if err != nil {
			log.Printf("Streaming error: %s", err)
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
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.DelVertex(elem.Id)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Id}}, nil
}

// DeleteEdge deletes an edge from the graph server
func (server *ArachneServer) DeleteEdge(ctx context.Context, elem *aql.ElementID) (*aql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.DelEdge(elem.Id)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: elem.Id}}, nil
}

// AddIndex adds a new index
func (server *ArachneServer) AddIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.AddVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

// DeleteIndex removes an index from the server
func (server *ArachneServer) DeleteIndex(ctx context.Context, idx *aql.IndexID) (*aql.EditResult, error) {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.DeleteVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Result: &aql.EditResult_Id{Id: idx.Field}}, nil
}

// GetIndex returns the terms and their counts from an index
func (server *ArachneServer) GetIndex(idx *aql.IndexID, stream aql.Query_GetIndexServer) error {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return err
	}
	res := graph.GetVertexTermCount(stream.Context(), idx.Label, idx.Field)
	for i := range res {
		l := i
		stream.Send(&l)
	}
	return nil
}

// GetIndexList lists avalible indices from a graph
func (server *ArachneServer) GetIndexList(idx *aql.GraphID, stream aql.Query_GetIndexListServer) error {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return err
	}
	res := graph.GetVertexIndexList()
	for i := range res {
		stream.Send(&i)
	}
	return nil
}

// IndexTraversal is not implemented
func (server *ArachneServer) IndexTraversal(idx *aql.IndexQuery, stream aql.Query_IndexTraversalServer) error {
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
	return fmt.Errorf("Not implemented")
}
