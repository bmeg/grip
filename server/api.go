package server

import (
	"fmt"
	"io"
	"sync"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("vertex %s not found", elem.Id))
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
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("edge %s not found", elem.Id))
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
	return &gripql.EditResult{Id: elem.Vertex.Gid}, nil
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
	if edge.Gid == "" {
		edge.Gid = util.UUID()
	}
	err = edge.Validate()
	if err != nil {
		return nil, fmt.Errorf("edge validation failed: %v", err)
	}

	err = graph.AddEdge([]*gdbi.Edge{gdbi.NewElementFromEdge(edge)})
	if err != nil {
		return nil, err
	}
	return &gripql.EditResult{Id: edge.Gid}, nil
}

// BulkAdd a stream of inputs and loads them into the graph
func (server *GripServer) BulkAdd(stream gripql.Edit_BulkAddServer) error {
	var graphName string
	var insertCount int32
	var errorCount int32

	elementStream := make(chan *gdbi.GraphElement, 100)
	wg := &sync.WaitGroup{}

	for {
		element, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("BulkAdd: streaming error")
			errorCount++
			break
		}

		if isSchema(element.Graph) {
			err := "cannot add element to schema graph"
			log.WithFields(log.Fields{"error": err}).Error("BulkAdd: error")
			errorCount++
			continue
		}

		// create a BulkAdd stream per graph
		// close and switch when a new graph is encountered
		if element.Graph != graphName {
			close(elementStream)
			gdb, err := server.getGraphDB(element.Graph)
			if err != nil {
				errorCount++
				continue
			}

			graph, err := gdb.Graph(element.Graph)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("BulkAdd: error")
				errorCount++
				continue
			}

			graphName = element.Graph
			elementStream = make(chan *gdbi.GraphElement, 100)

			wg.Add(1)
			go func() {
				log.WithFields(log.Fields{"graph": element.Graph}).Info("BulkAdd: streaming elements to graph")
				err := graph.BulkAdd(elementStream)
				if err != nil {
					log.WithFields(log.Fields{"graph": element.Graph, "error": err}).Error("BulkAdd: error")
					// not a good representation of the true number of errors
					errorCount++
				}
				wg.Done()
			}()
		}

		if element.Vertex != nil {
			err := element.Vertex.Validate()
			if err != nil {
				errorCount++
				log.WithFields(log.Fields{"graph": element.Graph, "error": err}).Errorf("BulkAdd: vertex validation failed")
			} else {
				insertCount++
				elementStream <- gdbi.NewGraphElement(element)
			}
		}

		if element.Edge != nil {
			if element.Edge.Gid == "" {
				element.Edge.Gid = util.UUID()
			}
			err := element.Edge.Validate()
			if err != nil {
				errorCount++
				log.WithFields(log.Fields{"graph": element.Graph, "error": err}).Errorf("BulkAdd: edge validation failed")
			} else {
				insertCount++
				elementStream <- gdbi.NewGraphElement(element)
			}
		}
	}

	close(elementStream)
	wg.Wait()

	return stream.SendAndClose(&gripql.BulkEditResult{InsertCount: insertCount, ErrorCount: errorCount})
}

func (server *GripServer) BulkDelete(stream gripql.Edit_BulkDeleteServer) error {
	var graphName string
	var insertCount int32
	var errorCount int32

	elementStream := make(chan *gdbi.ElementID, 100)
	wg := &sync.WaitGroup{}

	for {
		element, err := stream.Recv()
		log.Info("ELEM: ", element)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("BulkDelete: streaming error")
			errorCount++
			break
		}

		// create a BulkAdd stream per graph
		// close and switch when a new graph is encountered
		if element.Graph != graphName {
			close(elementStream)
			gdb, err := server.getGraphDB(element.Graph)
			if err != nil {
				errorCount++
				continue
			}

			graph, err := gdb.Graph(element.Graph)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("BulkAdd: error")
				errorCount++
				continue
			}

			graphName = element.Graph
			elementStream = make(chan *gdbi.ElementID, 100)

			wg.Add(1)
			go func() {
				log.WithFields(log.Fields{"graph": element.Graph}).Info("BulkAdd: streaming elements to graph")
				err := graph.BulkDelete(elementStream)
				if err != nil {
					log.WithFields(log.Fields{"graph": element.Graph, "error": err}).Error("BulkAdd: error")
					// not a good representation of the true number of errors
					errorCount++
				}
				wg.Done()
			}()
		}

		if element.Id != "" {
			insertCount++
			elementStream <- &gdbi.ElementID{Id: element.Id, Graph: element.Graph}
		}
	}

	close(elementStream)
	wg.Wait()

	return stream.SendAndClose(&gripql.BulkEditResult{InsertCount: insertCount, ErrorCount: errorCount})
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
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("graph %s: not found", elem.Graph))
	}
	schema, ok := server.schemas[elem.Graph]
	if !ok {
		if server.conf.Server.AutoBuildSchemas {
			return nil, status.Errorf(codes.Unavailable, fmt.Sprintf("graph %s: schema not available; try again later", elem.Graph))
		}
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("graph %s: schema not found", elem.Graph))
	}

	if schema.Graph == "" {
		schema.Graph = elem.Graph
	}
	return schema, nil
}

// GetSchema returns the schema of a specific graph in the database
func (server *GripServer) SampleSchema(ctx context.Context, elem *gripql.GraphID) (*gripql.Graph, error) {
	if !server.graphExists(elem.Graph) {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("graph %s: not found", elem.Graph))
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
	server.schemas[req.Graph] = req
	return &gripql.EditResult{Id: req.Graph}, nil
}

// GetMapping returns the schema of a specific graph in the database
func (server *GripServer) GetMapping(ctx context.Context, elem *gripql.GraphID) (*gripql.Graph, error) {
	if !server.graphExists(elem.Graph) {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("graph %s: not found", elem.Graph))
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
