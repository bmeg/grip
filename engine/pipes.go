/*
The engine package pulls togeather pipelines and runs processing
*/

package engine

import (
	"context"
  "fmt"
  "strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	log "github.com/sirupsen/logrus"
)

// Start begins processing a query pipeline
func Start(ctx context.Context, pipe gdbi.Pipeline, workdir string, bufsize int) gdbi.InPipe {
	procs := pipe.Processors()
	if len(procs) == 0 {
		ch := make(chan *gdbi.Traveler)
		close(ch)
		return ch
	}

	in := make(chan *gdbi.Traveler, bufsize)
	final := make(chan *gdbi.Traveler, bufsize)
	out := final
	for i := len(procs) - 1; i >= 0; i-- {
		man := NewManager(workdir)
		ctx = procs[i].Process(ctx, man, in, out)
		out = in
		in = make(chan *gdbi.Traveler, bufsize)
	}

	// Write an empty traveler to input
	// to trigger the computation.
	// Sends an empty traveler to the pipe to kick off pipelines of processors.
	go func() {
		out <- &gdbi.Traveler{}
		close(in)
		close(out)
	}()
	return final
}

// Run starts a pipeline and converts the output to server output structures
func Run(ctx context.Context, pipe gdbi.Pipeline, workdir string) <-chan *gripql.QueryResult {
	bufsize := 5000
	resch := make(chan *gripql.QueryResult, bufsize)
	go func() {
		defer close(resch)
		dataType := pipe.DataType()
		markTypes := pipe.MarkTypes()
		for t := range Start(ctx, pipe, workdir, bufsize) {
			resch <- Convert(dataType, markTypes, t)
		}
	}()

	return resch
}

// Convert takes a traveler and converts it to query output
func Convert(dataType gdbi.DataType, markTypes map[string]gdbi.DataType, t *gdbi.Traveler) *gripql.QueryResult {
	switch dataType {
	case gdbi.VertexData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Vertex{
				Vertex: t.GetCurrent().ToVertex(),
			},
		}

	case gdbi.EdgeData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Edge{
				Edge: t.GetCurrent().ToEdge(),
			},
		}

	case gdbi.CountData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Count{
				Count: t.Count,
			},
		}

	case gdbi.SelectionData:
		selections := map[string]*gripql.Selection{}
		for k, v := range t.Selections {
			switch markTypes[k] {
			case gdbi.VertexData:
				selections[k] = &gripql.Selection{
					Result: &gripql.Selection_Vertex{
						Vertex: v.ToVertex(),
					},
				}
			case gdbi.EdgeData:
				selections[k] = &gripql.Selection{
					Result: &gripql.Selection_Edge{
						Edge: v.ToEdge(),
					},
				}
			}
		}
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Selections{
				Selections: &gripql.Selections{
					Selections: selections,
				},
			},
		}

	case gdbi.RenderData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Render{
				Render: protoutil.WrapValue(t.Render),
			},
		}

	case gdbi.AggregationData:
		return &gripql.QueryResult{
			Result: &gripql.QueryResult_Aggregations{
				Aggregations: &gripql.NamedAggregationResult{
					Aggregations: t.Aggregations,
				},
			},
		}

	default:
		log.Errorf("unhandled data type %T", dataType)
	}
	return nil
}

// Traversal runs a GraphQuery request and returns a channel of results
func Traversal(ctx context.Context, db gdbi.GraphDB, workdir string, query *gripql.GraphQuery) (<-chan *gripql.QueryResult, error) {
	log.WithFields(log.Fields{"query": query}).Debug("Traversal")
	graph, err := db.Graph(query.Graph)
	if err != nil {
		return nil, err
	}
	compiler := graph.Compiler()
	pipeline, err := compiler.Compile(query.Query)
	if err != nil {
		return nil, err
	}
	resultsChan := Run(ctx, pipeline, workdir)
	return resultsChan, nil
}

// GetSchema runs a GraphQuery request and returns a channel of results
func GetSchema(ctx context.Context, db gdbi.GraphDB, workdir string, graph string) (*gripql.Graph, error) {
  if !gripql.IsSchema(graph) {
    graph = graph+gripql.SchemaSuffix
  }
  if !GraphExists(db, graph) {
    return nil, fmt.Errorf("graph '%s' not found", graph)
  }
  res, err := Traversal(ctx, db, workdir, &gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().V().Statements})
  if err != nil {
    return nil, fmt.Errorf("failed to load schema for graph %s: %v", graph, err)
  }
  vertices := []*gripql.Vertex{}
  for row := range res {
    vertices = append(vertices, row.GetVertex())
  }
  res, err = Traversal(ctx, db, workdir, &gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().E().Statements})
  if err != nil {
    return nil, fmt.Errorf("failed to load schema for graph %s: %v", graph, err)
  }
  edges := []*gripql.Edge{}
  for row := range res {
    edges = append(edges, row.GetEdge())
  }
  graph = strings.TrimSuffix(graph, gripql.SchemaSuffix)
  return &gripql.Graph{Graph: graph, Vertices: vertices, Edges: edges}, nil
}

// GraphExists reports whether or not a graph exists in
func GraphExists(db gdbi.GraphDB, graphName string) bool {
	found := false
	for _, graph := range db.ListGraphs() {
		if graph == graphName {
			found = true
		}
	}
	return found
}
