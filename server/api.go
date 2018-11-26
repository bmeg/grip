package server

import (
	"fmt"
	"io"
	"time"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Traversal parses a traversal request and streams the results back
func (server *GripServer) Traversal(query *gripql.GraphQuery, queryServer gripql.Query_TraversalServer) error {
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
	err = nil
	for row := range res {
		log.WithFields(log.Fields{"query": query, "result": row}).Info("Traversal")
		if err == nil {
			err = queryServer.Send(row)
		}
	}
	if err != nil {
		return fmt.Errorf("error sending Traversal result: %v", err)
	}
	return nil
}

// ListGraphs returns a list of graphs managed by the driver
func (server *GripServer) ListGraphs(empty *gripql.Empty, queryServer gripql.Query_ListGraphsServer) error {
	var err error
	for _, name := range server.db.ListGraphs() {
		if err == nil {
			err = queryServer.Send(&gripql.GraphID{Graph: name})
		}
	}
	if err != nil {
		return fmt.Errorf("error sending ListGraphs result: %v", err)
	}
	return nil
}

func (server *GripServer) getSchemas(ctx context.Context) {
	for _, name := range server.db.ListGraphs() {
		select {
		case <-ctx.Done():
			return

		default:
			log.WithFields(log.Fields{"graph": name}).Debug("get graph schema")
			schema, err := server.db.GetSchema(ctx, name, server.conf.SchemaInspectN, server.conf.SchemaRandomSample)
			if err == nil {
				log.WithFields(log.Fields{"graph": name}).Debug("cached graph schema")
				server.schemas[name] = schema
			} else {
				log.WithFields(log.Fields{"graph": name, "error": err}).Error("failed to get graph schema")
			}
		}
	}
}

// cacheSchemas calls GetSchema on each graph and caches the schemas in memory
func (server *GripServer) cacheSchemas(ctx context.Context) {
	if server.db == nil {
		return
	}
	ticker := time.NewTicker(server.conf.SchemaRefreshInterval)
	go func() {
		server.getSchemas(ctx)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				server.getSchemas(ctx)
			}
		}
	}()
	return
}

// GetSchema returns the schema of a specific graph in the database
func (server *GripServer) GetSchema(ctx context.Context, elem *gripql.GraphID) (*gripql.GraphSchema, error) {
	found := false
	for _, name := range server.db.ListGraphs() {
		if name == elem.Graph {
			found = true
		}
	}
	if !found {
		return nil, grpc.Errorf(codes.NotFound, fmt.Sprintf("graph %s: not found", elem.Graph))
	}

	schema, ok := server.schemas[elem.Graph]
	if !ok {
		return nil, grpc.Errorf(codes.Unavailable, fmt.Sprintf("graph %s: schema not available; try again later", elem.Graph))
	}

	return schema, nil
}

// GetVertex returns a vertex given a gripql.Element
func (server *GripServer) GetVertex(ctx context.Context, elem *gripql.ElementID) (*gripql.Vertex, error) {
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

// GetEdge returns an edge given a gripql.Element
func (server *GripServer) GetEdge(ctx context.Context, elem *gripql.ElementID) (*gripql.Edge, error) {
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
func (server *GripServer) GetTimestamp(ctx context.Context, elem *gripql.GraphID) (*gripql.Timestamp, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &gripql.Timestamp{Timestamp: graph.GetTimestamp()}, nil
}

// DeleteGraph deletes a graph
func (server *GripServer) DeleteGraph(ctx context.Context, elem *gripql.GraphID) (*gripql.EditResult, error) {
	err := server.db.DeleteGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: elem.Graph}, nil
}

// AddGraph creates a new graph on the server
func (server *GripServer) AddGraph(ctx context.Context, elem *gripql.GraphID) (*gripql.EditResult, error) {
	err := gripql.ValidateGraphName(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = server.db.AddGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: elem.Graph}, err
}

// AddVertex adds a vertex to the graph
func (server *GripServer) AddVertex(ctx context.Context, elem *gripql.GraphElement) (*gripql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}

	vertex := elem.Vertex
	err = vertex.Validate()
	if err != nil {
		return nil, fmt.Errorf("vertex validation failed: %v", err)
	}

	err = graph.AddVertex([]*gripql.Vertex{vertex})
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: elem.Vertex.Gid}, nil
}

// AddEdge adds an edge to the graph
func (server *GripServer) AddEdge(ctx context.Context, elem *gripql.GraphElement) (*gripql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}

	edge := elem.Edge
	if edge.Gid == "" {
		edge.Gid = util.UUID()
	}
	err = edge.Validate()
	if err != nil {
		return nil, fmt.Errorf("edge validation failed: %v", err)
	}

	err = graph.AddEdge([]*gripql.Edge{edge})
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: edge.Gid}, nil
}

type graphElementArray struct {
	graph    string
	vertices []*gripql.Vertex
	edges    []*gripql.Edge
}

func newGraphElementArray(name string, vertexBufSize, edgeBufSize int) *graphElementArray {
	if vertexBufSize != 0 {
		return &graphElementArray{graph: name, vertices: make([]*gripql.Vertex, 0, vertexBufSize)}
	}
	if edgeBufSize != 0 {
		return &graphElementArray{graph: name, edges: make([]*gripql.Edge, 0, edgeBufSize)}
	}
	return nil
}

// BulkAdd a stream of inputs and loads them into the graph
func (server *GripServer) BulkAdd(stream gripql.Edit_BulkAddServer) error {
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
					log.WithFields(log.Fields{"error": err}).Error("BulkAdd: graph connection error")
					log.Printf("Insert error: %s", err)
					return
				}
				err = graph.AddVertex(vBatch.vertices)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BulkAdd: add vertex error")
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
					log.WithFields(log.Fields{"error": err}).Error("BulkAdd: graph connection error")
					return
				}
				err = graph.AddEdge(eBatch.edges)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BulkAdd: add edge error")
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
				log.Debugf("%d vertices streamed", vertCount)
			}
			if edgeCount != 0 {
				log.Debugf("%d edges streamed", edgeCount)
			}
			vertexBatchChan <- vertexBatch
			edgeBatchChan <- edgeBatch
			loopErr = err
		} else if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("BulkAdd: streaming error")
			loopErr = err
		} else {
			if element.Vertex != nil {
				if vertexBatch.graph != element.Graph || len(vertexBatch.vertices) >= vertexBatchSize {
					vertexBatchChan <- vertexBatch
					vertexBatch = newGraphElementArray(element.Graph, vertexBatchSize, 0)
				}
				vertex := element.Vertex
				err := vertex.Validate()
				if err != nil {
					return fmt.Errorf("vertex validation failed: %v", err)
				}
				vertexBatch.vertices = append(vertexBatch.vertices, vertex)
				vertCount++
			} else if element.Edge != nil {
				if edgeBatch.graph != element.Graph || len(edgeBatch.edges) >= edgeBatchSize {
					edgeBatchChan <- edgeBatch
					edgeBatch = newGraphElementArray(element.Graph, 0, edgeBatchSize)
				}
				edge := element.Edge
				if edge.Gid == "" {
					edge.Gid = util.UUID()
				}
				err := edge.Validate()
				if err != nil {
					return fmt.Errorf("edge validation failed: %v", err)
				}
				edgeBatch.edges = append(edgeBatch.edges, edge)
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
	return stream.SendAndClose(&gripql.EditResult{})
}

// DeleteVertex deletes a vertex from the server
func (server *GripServer) DeleteVertex(ctx context.Context, elem *gripql.ElementID) (*gripql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.DelVertex(elem.Id)
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: elem.Id}, nil
}

// DeleteEdge deletes an edge from the graph server
func (server *GripServer) DeleteEdge(ctx context.Context, elem *gripql.ElementID) (*gripql.EditResult, error) {
	graph, err := server.db.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.DelEdge(elem.Id)
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: elem.Id}, nil
}

// AddIndex adds a new index
func (server *GripServer) AddIndex(ctx context.Context, idx *gripql.IndexID) (*gripql.EditResult, error) {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.AddVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: idx.Field}, nil
}

// DeleteIndex removes an index from the server
func (server *GripServer) DeleteIndex(ctx context.Context, idx *gripql.IndexID) (*gripql.EditResult, error) {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return nil, err
	}
	err = graph.DeleteVertexIndex(idx.Label, idx.Field)
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: idx.Field}, nil
}

// ListIndices lists avalible indices from a graph
func (server *GripServer) ListIndices(idx *gripql.GraphID, stream gripql.Query_ListIndicesServer) error {
	graph, err := server.db.Graph(idx.Graph)
	if err != nil {
		return err
	}
	res := graph.GetVertexIndexList()
	for i := range res {
		if err == nil {
			a := i
			err = stream.Send(&a)
		}
	}
	if err != nil {
		return fmt.Errorf("error sending ListIndices result: %v", err)
	}
	return nil
}

// Aggregate is partially implemented
func (server *GripServer) Aggregate(ctx context.Context, req *gripql.AggregationsRequest) (*gripql.NamedAggregationResult, error) {
	graph, err := server.db.Graph(req.Graph)
	if err != nil {
		return nil, err
	}

	g, ctx := errgroup.WithContext(ctx)

	aggChan := make(chan map[string]*gripql.AggregationResult, len(req.Aggregations))
	for _, agg := range req.Aggregations {
		agg := agg
		switch agg.Aggregation.(type) {
		case *gripql.Aggregate_Term:
			g.Go(func() error {
				termagg := agg.GetTerm()
				res, err := graph.GetVertexTermAggregation(ctx, termagg.Label, termagg.Field, termagg.Size)
				if err != nil {
					return fmt.Errorf("term aggregation failed: %s", err)
				}
				aggChan <- map[string]*gripql.AggregationResult{agg.Name: res}
				return nil
			})

		case *gripql.Aggregate_Percentile:
			g.Go(func() error {
				pagg := agg.GetPercentile()
				res, err := graph.GetVertexPercentileAggregation(ctx, pagg.Label, pagg.Field, pagg.Percents)
				if err != nil {
					return fmt.Errorf("percentile aggregation failed: %s", err)
				}
				aggChan <- map[string]*gripql.AggregationResult{agg.Name: res}
				return nil
			})

		case *gripql.Aggregate_Histogram:
			g.Go(func() error {
				histagg := agg.GetHistogram()
				res, err := graph.GetVertexHistogramAggregation(ctx, histagg.Label, histagg.Field, histagg.Interval)
				if err != nil {
					return fmt.Errorf("histogram aggregation failed: %s", err)
				}
				aggChan <- map[string]*gripql.AggregationResult{agg.Name: res}
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

	aggs := map[string]*gripql.AggregationResult{}
	for a := range aggChan {
		for k, v := range a {
			aggs[k] = v
		}
	}

	return &gripql.NamedAggregationResult{Aggregations: aggs}, nil
}
