package server

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/schema"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/jsonschema/v6"
	"github.com/bmeg/jsonschemagraph/graph"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Traversal parses a traversal request and streams the results back
func (server *GripServer) Traversal(query *gripql.GraphQuery, queryServer gripql.Query_TraversalServer) error {
	gdb, err := server.getGraphDB(query.Graph)
	if err != nil {
		return err
	}
	graph, err := gdb.Graph(query.Graph)
	if err != nil {
		return err
	}
	if delegatedGraph, ok := graph.(interface {
		Traversal(context.Context, []*gripql.GraphStatement) (<-chan *gripql.QueryResult, error)
	}); ok {
		res, err := delegatedGraph.Traversal(queryServer.Context(), query.Query)
		if err != nil {
			return err
		}
		err = nil
		for row := range res {
			if err == nil {
				err = queryServer.Send(row)
			}
		}
		if err != nil {
			return fmt.Errorf("error sending delegated Traversal result: %v", err)
		}
		return nil
	}
	compiler := graph.Compiler()
	compiledPipeline, err := compiler.Compile(query.Query, nil)
	if err != nil {
		return err
	}
	res := pipeline.Run(queryServer.Context(), compiledPipeline, server.conf.Server.WorkDir)
	err = nil
	for row := range res {
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
func (server *GripServer) ListGraphs(ctx context.Context, empty *gripql.Empty) (*gripql.ListGraphsResponse, error) {
	//server.updateGraphMap()
	graphs := []string{}
	for g := range server.graphMap {
		graphs = append(graphs, g)
	}
	return &gripql.ListGraphsResponse{Graphs: graphs}, nil
}

// ListTables returns list of all tables that are found in plugin system
func (server *GripServer) ListTables(empty *gripql.Empty, srv gripql.Query_ListTablesServer) error {
	client := gripper.NewGripperClient(server.sources)

	for k := range server.sources {
		for col := range client.GetCollections(context.Background(), k) {
			info, _ := client.GetCollectionInfo(context.Background(), k, col)
			srv.Send(&gripql.TableInfo{Source: k, Name: col, Fields: info.SearchFields, LinkMap: info.LinkMap})
		}
	}
	return nil
}

// GetVertex returns a vertex given a gripql.Element
func (server *GripServer) GetVertex(ctx context.Context, elem *gripql.ElementID) (*gripql.Vertex, error) {
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	o := graph.GetVertex(elem.Id, true)
	if o == nil {
		return nil, status.Errorf(codes.NotFound, "vertex %s not found", elem.Id)
	}
	return o.ToVertex(), nil
}

// GetEdge returns an edge given a gripql.Element
func (server *GripServer) GetEdge(ctx context.Context, elem *gripql.ElementID) (*gripql.Edge, error) {
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	o := graph.GetEdge(elem.Id, true)
	if o == nil {
		return nil, status.Errorf(codes.NotFound, "edge %s not found", elem.Id)
	}
	return o.ToEdge(), nil
}

// GetTimestamp returns the update timestamp of a graph
func (server *GripServer) GetTimestamp(ctx context.Context, elem *gripql.GraphID) (*gripql.Timestamp, error) {
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}
	return &gripql.Timestamp{Timestamp: graph.GetTimestamp()}, nil
}

// DeleteGraph deletes a graph
func (server *GripServer) DeleteGraph(ctx context.Context, elem *gripql.GraphID) (*gripql.EditResult, error) {
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = gdb.DeleteGraph(elem.Graph)
	if err != nil {
		return nil, fmt.Errorf("DeleteGraph: deleting graph %s: %v", elem.Graph, err)
	}
	schemaName := fmt.Sprintf("%s%s", elem.Graph, schemaSuffix)
	if server.graphExists(schemaName) {
		err := gdb.DeleteGraph(schemaName)
		if err != nil {
			return nil, fmt.Errorf("DeleteGraph: deleting schema for graph %s: %v", elem.Graph, err)
		}
	}
	server.updateGraphMap()
	return &gripql.EditResult{Id: elem.Graph}, nil
}

// AddGraph creates a new graph on the server
func (server *GripServer) AddGraph(ctx context.Context, elem *gripql.GraphID) (*gripql.EditResult, error) {
	err := gripql.ValidateGraphName(elem.Graph)
	if err != nil {
		return nil, err
	}
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	err = gdb.AddGraph(elem.Graph)
	if err != nil {
		return nil, err
	}
	server.updateGraphMap()
	return &gripql.EditResult{Id: elem.Graph}, err
}

// AddVertex adds a vertex to the graph
func (server *GripServer) AddVertex(ctx context.Context, elem *gripql.GraphElement) (*gripql.EditResult, error) {
	if isSchema(elem.Graph) {
		return nil, fmt.Errorf("unable to add vertex to graph schema; use AddSchema")
	}
	return server.addVertex(ctx, elem)
}

func (server *GripServer) addVertex(ctx context.Context, elem *gripql.GraphElement) (*gripql.EditResult, error) {
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}

	vertex := elem.Vertex
	err = vertex.Validate()
	if err != nil {
		return nil, fmt.Errorf("vertex validation failed: %v", err)
	}

	err = graph.AddVertex([]*gdbi.Vertex{gdbi.NewElementFromVertex(vertex)})
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: elem.Vertex.Id}, nil
}

// AddEdge adds an edge to the graph
func (server *GripServer) AddEdge(ctx context.Context, elem *gripql.GraphElement) (*gripql.EditResult, error) {
	if isSchema(elem.Graph) {
		return nil, fmt.Errorf("unable to add edge to graph schema; use AddSchema")
	}
	return server.addEdge(ctx, elem)
}

func (server *GripServer) addEdge(ctx context.Context, elem *gripql.GraphElement) (*gripql.EditResult, error) {
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
	if err != nil {
		return nil, err
	}

	edge := elem.Edge
	if edge.Id == "" {
		edge.Id = util.UUID()
	}
	err = edge.Validate()
	if err != nil {
		return nil, fmt.Errorf("edge validation failed: %v", err)
	}

	err = graph.AddEdge([]*gdbi.Edge{gdbi.NewElementFromEdge(edge)})
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: edge.Id}, nil
}

func (server *GripServer) BulkAddRaw(stream gripql.Edit_BulkAddRawServer) error {
	ctx := stream.Context()
	inputCh := make(chan *gripql.RawJson, 100)
	elementCh := make(chan *gdbi.GraphElement, 1000)
	errCh := make(chan error, 100)
	var insertCount int32
	var once sync.Once
	var schema *graph.GraphSchema
	var schemaErr error
	var wg sync.WaitGroup
	var producerWG sync.WaitGroup

	// Receive first class
	firstClass, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("initial receive failed: %w", err)
	}

	graphName := firstClass.Graph
	gdb, err := server.getGraphDB(graphName)
	if err != nil {
		return fmt.Errorf("get graph DB failed: %w", err)
	}

	gdbiGraph, err := gdb.Graph(graphName)
	if err != nil {
		return fmt.Errorf("get graph failed: %w", err)
	}

	// Load schema once
	loadSchema := func() {
		sch, err := server.getGraph(graphName + "__schema__")
		if err != nil {
			schemaErr = fmt.Errorf("get schema failed: %w", err)
			return
		}
		schema, err = server.LoadSchemas(sch, &graph.GraphSchema{Classes: map[string]*jsonschema.Schema{}, Compiler: nil})
		if err != nil {
			schemaErr = fmt.Errorf("load schema failed: %w", err)
		}
	}

	// Start bulk add goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := gdbiGraph.BulkAdd(elementCh); err != nil {
			log.WithFields(log.Fields{"graph": graphName, "error": err}).Error("BulkAddRaw: bulk add error")
			errCh <- fmt.Errorf("bulk add failed: %w", err)
		}
	}()

	// Start worker goroutines
	for range runtime.NumCPU() {
		producerWG.Add(1)
		go func() {
			defer producerWG.Done()
			for class := range inputCh {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				default:
				}

				once.Do(loadSchema)
				if schemaErr != nil {
					errCh <- schemaErr
					continue
				}

				classData := class.Data.AsMap()
				resourceType, ok := classData["resourceType"].(string)
				if !ok {
					err := fmt.Errorf("row %v does not have required field resourceType", classData)
					log.WithFields(log.Fields{"error": err}).Error("BulkAddRaw: streaming error")
					errCh <- err
					continue
				}

				result, err := schema.Generate(resourceType, classData, class.ExtraArgs.AsMap())
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Errorf("BulkAddRaw: validation error for %s: %v", resourceType, classData)
					errCh <- fmt.Errorf("validation failed for %s: %w", resourceType, err)
					continue
				}

				for _, element := range result {
					if element.Vertex != nil {
						elementCh <- &gdbi.GraphElement{
							Vertex: &gdbi.Vertex{
								ID:    element.Vertex.Id,
								Data:  element.Vertex.Data.AsMap(),
								Label: element.Vertex.Label,
							},
							Graph: graphName,
						}
					} else if element.Edge != nil {
						elementCh <- &gdbi.GraphElement{
							Edge: &gdbi.Edge{
								ID:    element.Edge.Id,
								Label: element.Edge.Label,
								From:  element.Edge.From,
								To:    element.Edge.To,
								Data:  element.Edge.Data.AsMap(),
							},
							Graph: graphName,
						}
					}
					atomic.AddInt32(&insertCount, 1)
				}
			}
		}()
	}

	// Receiver goroutine
	inputCh <- firstClass
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		defer close(inputCh)
		for {
			class, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				errCh <- fmt.Errorf("receive failed: %w", err)
				break
			}
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case inputCh <- class:
			}
		}
	}()

	// Collect errors
	var retErrs []string
	doneCollecting := make(chan struct{})
	go func() {
		defer close(doneCollecting)
		for err := range errCh {
			retErrs = append(retErrs, err.Error())
		}
	}()

	// Wait for completion
	producerWG.Wait()
	close(elementCh)
	wg.Wait()
	close(errCh)
	<-doneCollecting

	// Return result with all collected errors
	return stream.SendAndClose(&gripql.BulkJsonEditResult{
		InsertCount: insertCount,
		Errors:      retErrs,
	})
}

func (server *GripServer) BulkAdd(stream gripql.Edit_BulkAddServer) error {
	var insertCount int32
	var errorCount int32
	currentGraph := ""
	var elementStream chan *gdbi.GraphElement
	var processErr error

	ctx := stream.Context()
	eg, opCtx := errgroup.WithContext(ctx)

	// Function to start a new BulkAdd goroutine for a graph
	startBulkAdd := func(graphName string, gdb gdbi.GraphInterface) chan *gdbi.GraphElement {
		newStream := make(chan *gdbi.GraphElement, 100)
		eg.Go(func() error {
			log.WithFields(log.Fields{"graph": graphName}).Info("BulkAdd: streaming elements to graph")
			if err := gdb.BulkAdd(newStream); err != nil {
				log.WithFields(log.Fields{"graph": graphName, "error": err}).Error("BulkAdd: error")
				atomic.AddInt32(&errorCount, 1)
				return err
			}
			return nil
		})
		return newStream
	}

	loop:
	for {
		// Check if context is done (client cancellation or goroutine error)
		select {
		case <-opCtx.Done():
			processErr = opCtx.Err()
			break loop
		default:
			// Continue processing
		}

		element, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("BulkAdd: streaming error")
			atomic.AddInt32(&errorCount, 1)
			processErr = err
			break
		}

		if isSchema(element.Graph) {
			err := errors.New("cannot add element to schema graph")
			log.WithFields(log.Fields{"error": err}).Error("BulkAdd: error")
			atomic.AddInt32(&errorCount, 1)
			continue
		}

		// Switch graphs if needed
		if element.Graph != currentGraph {
			if elementStream != nil {
				close(elementStream)
			}

			gdb, err := server.getGraphDB(element.Graph)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("BulkAdd: error getting graph DB")
				atomic.AddInt32(&errorCount, 1)
				continue
			}

			graph, err := gdb.Graph(element.Graph)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("BulkAdd: error")
				atomic.AddInt32(&errorCount, 1)
				continue
			}

			currentGraph = element.Graph
			elementStream = startBulkAdd(currentGraph, graph)
		}

		if element.Vertex != nil {
			if err := element.Vertex.Validate(); err != nil {
				log.WithFields(log.Fields{"graph": element.Graph, "error": err}).Errorf("BulkAdd: vertex validation failed for vertex: %#v", element.Vertex)
				atomic.AddInt32(&errorCount, 1)
			} else {
				select {
				case <-opCtx.Done():
					// Context done, stop processing
				case elementStream <- gdbi.NewGraphElement(element):
					atomic.AddInt32(&insertCount, 1)
				}
			}
		}
		if element.Edge != nil {
			if element.Edge.Id == "" {
				element.Edge.Id = util.UUID()
			}
			if err := element.Edge.Validate(); err != nil {
				log.WithFields(log.Fields{"graph": element.Graph, "error": err}).Errorf("BulkAdd: edge validation failed for edge: %#v", element.Edge)
				atomic.AddInt32(&errorCount, 1)
			} else {
				select {
				case <-opCtx.Done():
					// Context done, stop processing
				case elementStream <- gdbi.NewGraphElement(element):
					atomic.AddInt32(&insertCount, 1)
				}
			}
		}
	}

	if elementStream != nil {
		close(elementStream)
	}
	egErr := eg.Wait()
	if processErr != nil {
		return processErr
	}
	if egErr != nil {
		return egErr
	}
	return stream.SendAndClose(&gripql.BulkEditResult{InsertCount: insertCount, ErrorCount: errorCount})
}

func (server *GripServer) BulkDelete(ctx context.Context, delete *gripql.DeleteData) (*gripql.EditResult, error) {
	gdb, err := server.getGraphDB(delete.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(delete.Graph)
	if err != nil {
		return nil, err
	}

	err = graph.BulkDel(&gdbi.DeleteData{Graph: delete.Graph, Vertices: delete.Vertices, Edges: delete.Edges})
	if err != nil {
		log.WithFields(log.Fields{"graph": delete.Graph, "error": err})
		return nil, err
	}
	return &gripql.EditResult{Id: util.UUID()}, nil
}

// DeleteVertex deletes a vertex from the server
func (server *GripServer) DeleteVertex(ctx context.Context, elem *gripql.ElementID) (*gripql.EditResult, error) {
	if isSchema(elem.Graph) {
		return nil, fmt.Errorf("unable to delete vertex from graph schema; use AddSchema")
	}
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
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
	if isSchema(elem.Graph) {
		return nil, fmt.Errorf("unable to delete edge from graph schema; use AddSchema")
	}
	gdb, err := server.getGraphDB(elem.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(elem.Graph)
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
	if isSchema(idx.Graph) {
		return nil, fmt.Errorf("unupported operation for graph schema")
	}
	gdb, err := server.getGraphDB(idx.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(idx.Graph)
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
	if isSchema(idx.Graph) {
		return nil, fmt.Errorf("unupported operation for graph schema")
	}
	gdb, err := server.getGraphDB(idx.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(idx.Graph)
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
func (server *GripServer) ListIndices(ctx context.Context, idx *gripql.GraphID) (*gripql.ListIndicesResponse, error) {
	gdb, err := server.getGraphDB(idx.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(idx.Graph)
	if err != nil {
		return nil, err
	}
	indices := []*gripql.IndexID{}
	for i := range graph.GetVertexIndexList() {
		indices = append(indices, i)
	}
	return &gripql.ListIndicesResponse{Indices: indices}, nil
}

// ListLabels lists the vertex and edge labels in a graph
func (server *GripServer) ListLabels(ctx context.Context, idx *gripql.GraphID) (*gripql.ListLabelsResponse, error) {
	gdb, err := server.getGraphDB(idx.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(idx.Graph)
	if err != nil {
		return nil, err
	}
	vLabels, err := graph.ListVertexLabels()
	if err != nil {
		return nil, err
	}
	eLabels, err := graph.ListEdgeLabels()
	if err != nil {
		return nil, err
	}
	return &gripql.ListLabelsResponse{VertexLabels: vLabels, EdgeLabels: eLabels}, nil
}

// GetSchema returns the schema of a specific graph in the database
func (server *GripServer) GetSchema(ctx context.Context, elem *gripql.GraphID) (*gripql.Graph, error) {
	if !server.graphExists(elem.Graph) {
		return nil, status.Errorf(codes.NotFound, "graph %s: not found", elem.Graph)
	}
	schema, ok := server.schemas[elem.Graph]
	if !ok {
		if server.conf.Server.AutoBuildSchemas {
			return nil, status.Errorf(codes.Unavailable, "graph %s: schema not available; try again later", elem.Graph)
		}
		return nil, status.Errorf(codes.NotFound, "graph %s: schema not found", elem.Graph)
	}
	if schema.Graph == "" {
		schema.Graph = elem.Graph
	}
	return schema, nil
}

// GetSchema returns the schema of a specific graph in the database
func (server *GripServer) SampleSchema(ctx context.Context, elem *gripql.GraphID) (*gripql.Graph, error) {
	if !server.graphExists(elem.Graph) {
		return nil, status.Errorf(codes.NotFound, "graph %s: not found", elem.Graph)
	}
	if gdb, err := server.getGraphDB(elem.Graph); err == nil {
		schema, err := gdb.BuildSchema(ctx, elem.Graph, 50, true)
		if err != nil {
			return nil, err
		}
		if schema.Graph == "" {
			schema.Graph = elem.Graph
		}
		return schema, err
	}
	return nil, fmt.Errorf("Graph driver not found")
}

// AddSchema caches a graph schema on the server
func (server *GripServer) AddSchema(ctx context.Context, req *gripql.Graph) (*gripql.EditResult, error) {
	err := server.addFullGraph(ctx, fmt.Sprintf("%s%s", req.Graph, schemaSuffix), req)
	if err != nil {
		return nil, fmt.Errorf("failed to store new schema: %v", err)
	}
	if !strings.HasSuffix(req.Graph, schemaSuffix) {
		req.Graph = req.Graph + schemaSuffix
	}
	schema, err := server.getGraph(req.Graph)
	if err != nil {
		log.Errorln("Error in server.getGraph: ", err)
	} else {
		server.schemas[strings.TrimSuffix(req.Graph, schemaSuffix)] = schema
	}
	return &gripql.EditResult{Id: req.Graph}, nil
}

// AddJsonSchema adds a jsonschema to grip as a graph
func (server *GripServer) AddJsonSchema(ctx context.Context, rawjson *gripql.RawJson) (*gripql.EditResult, error) {
	bytes, err := protojson.Marshal(rawjson.Data)
	if err != nil {
		fmt.Printf("Failed to marshal data to bytes: %v\n", err)
		return nil, err
	}
	req, err := schema.ParseJSchema(bytes, rawjson.Graph)
	if err != nil {
		log.Errorf("Failed to parse schema data: %v\n", err)
		return nil, err
	}
	res, err := server.AddSchema(ctx, req[0])
	return res, err
}

// GetMapping returns the schema of a specific graph in the database
func (server *GripServer) GetMapping(ctx context.Context, elem *gripql.GraphID) (*gripql.Graph, error) {
	if !server.graphExists(elem.Graph) {
		return nil, status.Errorf(codes.NotFound, "graph %s: not found", elem.Graph)
	}
	mapping, err := server.getGraph(elem.Graph + mappingSuffix)
	if err != nil {
		return nil, err
	}
	return mapping, nil
}

// AddMapping caches a graph schema on the server
func (server *GripServer) AddMapping(ctx context.Context, req *gripql.Graph) (*gripql.EditResult, error) {
	err := server.addFullGraph(ctx, fmt.Sprintf("%s%s", req.Graph, mappingSuffix), req)
	if err != nil {
		return nil, fmt.Errorf("failed to store new mapping: %v", err)
	}
	server.updateGraphMap()
	return &gripql.EditResult{Id: req.Graph}, nil
}

func (server *GripServer) graphExists(graphName string) bool {
	gdb, err := server.getGraphDB(graphName)
	if err != nil {
		return false
	}
	found := false
	for _, graph := range gdb.ListGraphs() {
		if graph == graphName {
			found = true
		}
	}
	return found
}
