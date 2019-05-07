package server

import (
	"context"
	"fmt"
	"net/http"
	"regexp"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/graphql-go/graphql"
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

func buildField(x string) (*graphql.Field, error) {
	var o *graphql.Field
	switch x {
	case "NUMERIC":
		o = &graphql.Field{Type: graphql.Float}
	case "STRING":
		o = &graphql.Field{Type: graphql.String}
	case "BOOL":
		o = &graphql.Field{Type: graphql.Boolean}
	default:
		return nil, fmt.Errorf("%s does not map to a GraphQL type", x)
	}
	return o, nil
}

func buildSliceField(name string, s []interface{}) (*graphql.Field, error) {
	var f *graphql.Field
	var err error

	if len(s) > 0 {
		val := s[0]

		if x, ok := val.(map[string]interface{}); ok {
			f, err = buildObjectField(name, x)

		} else if x, ok := val.([]interface{}); ok {
			f, err = buildSliceField(name, x)

		} else if x, ok := val.(string); ok {
			f, err = buildField(x)

		} else {
			err = fmt.Errorf("unhandled type: %T %v", val, val)
		}

	} else {
		err = fmt.Errorf("slice is empty")
	}

	if err != nil {
		return nil, fmt.Errorf("buildSliceField error: %v", err)
	}

	return &graphql.Field{Type: graphql.NewList(f.Type)}, nil
}

func buildObjectField(name string, obj map[string]interface{}) (*graphql.Field, error) {
	o, err := buildObject(name, obj)
	if err != nil {
		return nil, err
	}

	return &graphql.Field{Type: o}, nil
}

func buildObject(name string, obj map[string]interface{}) (*graphql.Object, error) {
	objFields := graphql.Fields{}

	for key, val := range obj {
		var err error

		// handle map
		if x, ok := val.(map[string]interface{}); ok {
			objFields[key], err = buildObjectField(key, x)

			// handle slice
		} else if x, ok := val.([]interface{}); ok {
			objFields[key], err = buildSliceField(key, x)

			// handle string
		} else if x, ok := val.(string); ok {
			objFields[key], err = buildField(x)

			// handle other cases
		} else {
			err = fmt.Errorf("unhandled type: %T %v", val, val)
		}

		if err != nil {
			log.WithFields(log.Fields{"object": name, "field": key, "error": err}).Error("graphql: buildObject")
			// return nil, fmt.Errorf("object: %s: field: %s: error: %v", name, key, err)
		}
	}

	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   name,
			Fields: objFields,
		},
	), nil
}

func buildObjectMap(schema *gripql.Graph) (map[string]*graphql.Object, error) {
	objects := map[string]*graphql.Object{}

	for _, obj := range schema.Vertices {
		props := obj.GetDataMap()
		if props == nil {
			props = make(map[string]interface{})
		}
		props["gid"] = "STRING"
		gqlObj, err := buildObject(obj.Label, props)
		if err != nil {
			return nil, err
		}
		objects[obj.Label] = gqlObj
	}

	// Setup outgoing edge fields
	// Note: edge properties are not accessible in this model
	for _, obj := range schema.Edges {
		obj := obj
		fname := obj.Label + "_to_" + obj.To
		f := &graphql.Field{
			Name: fname,
			Type: graphql.NewList(objects[obj.To]),
		}
		objects[obj.From].AddFieldConfig(fname, f)
	}

	return objects, nil
}

func buildGraphQLSchema(db gdbi.GraphDB, workdir string, graph string, schema *gripql.Graph) (*graphql.Schema, error) {
	if schema == nil {
		return nil, fmt.Errorf("graphql.NewSchema error: nil gripql.Graph for graph: %s", graph)
	}

	objectMap, err := buildObjectMap(schema)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}

	queryFields := graphql.Fields{}
	for objName, obj := range objectMap {
		label := obj.Name()
		f := &graphql.Field{
			Name: objName,
			Type: graphql.NewList(obj),
			Args: graphql.FieldConfigArgument{
				"gid":   &graphql.ArgumentConfig{Type: graphql.String},
				"first": &graphql.ArgumentConfig{Type: graphql.Int},
			},
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {
				return ResolveGraphQL(db, workdir, graph, label, params)
			},
		}
		queryFields[objName] = f
	}

	query := graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: queryFields,
		},
	)

	schemaConfig := graphql.SchemaConfig{
		Query: query,
	}

	gqlSchema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}

	return &gqlSchema, nil
}
