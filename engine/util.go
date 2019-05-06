package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	log "github.com/sirupsen/logrus"
)

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
		graph = graph + gripql.SchemaSuffix
	}
	if !GraphExists(db, graph) {
		return nil, fmt.Errorf("graph '%s' not found", graph)
	}
	res, err := Traversal(ctx, db, workdir, &gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().V().Statements})
	if err != nil {
		return nil, fmt.Errorf("failed to load schema for graph '%s': %v", graph, err)
	}
	vertices := []*gripql.Vertex{}
	for row := range res {
		vertices = append(vertices, row.GetVertex())
	}
	res, err = Traversal(ctx, db, workdir, &gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().E().Statements})
	if err != nil {
		return nil, fmt.Errorf("failed to load schema for graph '%s': %v", graph, err)
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
			return true
		}
	}
	return found
}
