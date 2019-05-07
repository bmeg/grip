package server

import (
	"context"
	"fmt"
	"net/http"
	"regexp"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/graphql-go/handler"
	log "github.com/sirupsen/logrus"
)

// graphHandler handles the graphql queries for a single graph
type graphHandler struct {
	gqlHandler *handler.Handler
	db         gdbi.GraphDB
	graph      string
	schema     *gripql.Graph
	workdir    string
}

// GraphQLHandler manages GraphQL queries to the Grip database
type GraphQLHandler struct {
	handlers map[string]*graphHandler
	db       gdbi.GraphDB
	workdir  string
}

// NewGraphQLHandler initilizes a new GraphQLHandler
func NewGraphQLHandler(db gdbi.GraphDB, workdir string) (*GraphQLHandler, error) {
	if db == nil {
		return nil, fmt.Errorf("gdbi.GraphDB interface is nil")
	}
	return &GraphQLHandler{
		db:       db,
		workdir:  workdir,
		handlers: map[string]*graphHandler{},
	}, nil
}

// Generate graphql handlers for all graphs
func (gh *GraphQLHandler) BuildAllGraphHandlers() {
	for _, graph := range gh.db.ListGraphs() {
		if !gripql.IsSchema(graph) {
			handler := newGraphHandler(gh.db, gh.workdir, graph)
			if handler != nil {
				gh.handlers[graph] = handler
			}
		}
	}
}

// Generate graphql handlers for all graphs
func (gh *GraphQLHandler) BuildGraphHandler(graph string) {
	handler := newGraphHandler(gh.db, gh.workdir, graph)
	if handler != nil {
		gh.handlers[graph] = handler
	}
}

// ServeHTTP responds to HTTP graphql requests
func (gh *GraphQLHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	pathRE := regexp.MustCompile("/graphql/(.*)$")
	graphName := pathRE.FindStringSubmatch(request.URL.Path)[1]
	var handler *graphHandler
	var ok bool
	if handler, ok = gh.handlers[graphName]; ok {
		handler.gqlHandler.ServeHTTP(writer, request)
	} else {
		http.Error(writer, fmt.Sprintf("No GraphQL handler found for graph: %s", graphName), http.StatusInternalServerError)
	}
}

// newGraphHandler creates a new graphql handler from schema
func newGraphHandler(db gdbi.GraphDB, workdir string, graph string) *graphHandler {
	if db == nil {
		log.WithFields(log.Fields{"graph": graph, "error": fmt.Errorf("gdbi.GraphDB interface is nil")}).Errorf("newGraphHandler: checking args")
		return nil
	}
	h := &graphHandler{
		graph:   graph,
		db:      db,
		workdir: workdir,
	}
	log.WithFields(log.Fields{"graph": graph}).Info("newGraphHandler: Building GraphQL schema")
	schema, err := engine.GetSchema(context.Background(), db, workdir, graph)
	if err != nil {
		log.WithFields(log.Fields{"graph": graph, "error": err}).Error("newGraphHandler: GetSchema error")
		return nil
	}
	h.schema = schema
	gqlSchema, err := buildGraphQLSchema(db, workdir, graph, schema)
	if err != nil {
		log.WithFields(log.Fields{"graph": graph, "error": err}).Error("newGraphHandler: GraphQL schema build failed")
		return nil
	}
	log.WithFields(log.Fields{"graph": graph}).Info("newGraphHandler: Built GraphQL schema")
	h.gqlHandler = handler.New(&handler.Config{
		Schema: gqlSchema,
	})
	return h
}
