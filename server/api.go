package server

import (
	"fmt"
	"io"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

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
	res := engine.Run(queryServer.Context(), pipeline, server.conf.WorkDir)
	for row := range res {
		err := queryServer.Send(row)
		if err != nil {
			return fmt.Errorf("error sending Traversal result: %v", err)
		}
	}

	return nil
}

// ListGraphs returns a list of graphs managed by the driver
func (server *ArachneServer) ListGraphs(empty *aql.Empty, queryServer aql.Query_ListGraphsServer) error {
	for _, name := range server.db.ListGraphs() {
		err := queryServer.Send(&aql.GraphID{Graph: name})
		if err != nil {
			return fmt.Errorf("error sending ListGraphs result: %v", err)
		}
	}
	return nil
}

// GetSchema returns the schema of a specific graph in the database
func (server *ArachneServer) GetSchema(ctx context.Context, elem *aql.GraphID) (*aql.GraphSchema, error) {
	return server.db.GetSchema(elem.Graph)
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
func (server *ArachneServer) GetTimestamp(ctx context.Context, elem *aql.GraphID) (*aql.Timestamp, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &aql.Timestamp{Timestamp: graph.GetTimestamp()}, nil
}

// DeleteGraph deletes a graph
func (server *ArachneServer) DeleteGraph(ctx context.Context, elem *aql.GraphID) (*aql.EditResult, error) {
	err := server.db.DeleteGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Id: elem.Graph}, nil
}

// AddGraph creates a new graph on the server
func (server *ArachneServer) AddGraph(ctx context.Context, elem *aql.GraphID) (*aql.EditResult, error) {
	err := aql.ValidateGraphName(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = server.db.AddGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &aql.EditResult{Id: elem.Graph}, err
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
	return &aql.EditResult{Id: elem.Vertex.Gid}, nil
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
	return &aql.EditResult{Id: elem.Edge.Gid}, nil
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

// BulkAdd a stream of inputs and loads them into the graph
func (server *ArachneServer) BulkAdd(stream aql.Edit_BulkAddServer) error {
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
					return
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
					return
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
	return stream.SendAndClose(&aql.EditResult{})
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
	return &aql.EditResult{Id: elem.Id}, nil
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
	return &aql.EditResult{Id: elem.Id}, nil
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
	return &aql.EditResult{Id: idx.Field}, nil
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
	return &aql.EditResult{Id: idx.Field}, nil
}

// ListIndices lists avalible indices from a graph
func (server *ArachneServer) ListIndices(idx *aql.GraphID, stream aql.Query_ListIndicesServer) error {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return err
	}
	res := graph.GetVertexIndexList()
	for i := range res {
		err := stream.Send(&i)
		if err != nil {
			return fmt.Errorf("error sending ListIndices result: %v", err)
		}
	}
	return nil
}

// Aggregate is partially implemented
func (server *ArachneServer) Aggregate(ctx context.Context, req *aql.AggregationsRequest) (*aql.NamedAggregationResult, error) {
	graph, err := server.db.Graph(req.Graph)
	if err != nil {
		return nil, err
	}

	g, ctx := errgroup.WithContext(ctx)

	aggChan := make(chan map[string]*aql.AggregationResult, len(req.Aggregations))
	for _, agg := range req.Aggregations {
		agg := agg
		switch agg.Aggregation.(type) {
		case *aql.Aggregate_Term:
			g.Go(func() error {
				termagg := agg.GetTerm()
				res, err := graph.GetVertexTermAggregation(ctx, termagg.Label, termagg.Field, termagg.Size)
				if err != nil {
					return fmt.Errorf("term aggregation failed: %s", err)
				}
				aggChan <- map[string]*aql.AggregationResult{agg.Name: res}
				return nil
			})

		case *aql.Aggregate_Percentile:
			g.Go(func() error {
				pagg := agg.GetPercentile()
				res, err := graph.GetVertexPercentileAggregation(ctx, pagg.Label, pagg.Field, pagg.Percents)
				if err != nil {
					return fmt.Errorf("percentile aggregation failed: %s", err)
				}
				aggChan <- map[string]*aql.AggregationResult{agg.Name: res}
				return nil
			})

		case *aql.Aggregate_Histogram:
			g.Go(func() error {
				histagg := agg.GetHistogram()
				res, err := graph.GetVertexHistogramAggregation(ctx, histagg.Label, histagg.Field, histagg.Interval)
				if err != nil {
					return fmt.Errorf("histogram aggregation failed: %s", err)
				}
				aggChan <- map[string]*aql.AggregationResult{agg.Name: res}
				return nil
			})

		default:
			return nil, fmt.Errorf("unknown aggregation type")
		}
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("one or more aggregation failed: %v", err)
	}
	close(aggChan)

	aggs := map[string]*aql.AggregationResult{}
	for a := range aggChan {
		for k, v := range a {
			aggs[k] = v
		}
	}

	return &aql.NamedAggregationResult{Aggregations: aggs}, nil
}
