/*
GraphQL Web endpoint
*/

package graphql

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

//handle the graphql queries for a single endpoint
type graphHandler struct {
	graph      string
	gqlHandler *handler.Handler
	db         gdbi.GraphDB
	schema     *gripql.Graph
	workdir    string
}

// Handler is a GraphQL endpoint to query the Grip database
type Handler struct {
	handlers map[string]*graphHandler
	db       gdbi.GraphDB
	workdir  string
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(db gdbi.GraphDB, workdir string) *Handler {
	h := &Handler{
		db:       db,
		workdir:  workdir,
		handlers: map[string]*graphHandler{},
	}
	return h
}

// Generate graphql handlers for all graphs
func (gh *Handler) BuildGraphHandlers() {
  if gh.db == nil {
    return
  }
	for _, graph := range gh.db.ListGraphs() {
		if !gripql.IsSchema(graph) {
			handler := newGraphHandler(gh.db, gh.workdir, graph)
			if handler != nil {
				gh.handlers[graph] = handler
			}
		}
	}
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	h := &graphHandler{
		graph:   graph,
		db:      db,
		workdir: workdir,
	}
	log.WithFields(log.Fields{"graph": graph}).Info("Building GraphQL schema")
	schema, err := engine.GetSchema(context.Background(), db, workdir, graph)
	if err != nil {
		log.WithFields(log.Fields{"graph": graph, "error": err}).Error("GetSchema error")
		return nil
	}
	h.schema = schema
	gqlSchema, err := BuildGraphQLSchema(db, workdir, graph, schema)
	if err != nil {
		log.WithFields(log.Fields{"graph": graph, "error": err}).Error("GraphQL schema build failed")
		return nil
	}
	log.WithFields(log.Fields{"graph": graph}).Info("Built GraphQL schema")
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

func buildObjectMap(db gdbi.GraphDB, workdir string, graph string, schema *gripql.Graph) (map[string]*graphql.Object, error) {
	objects := map[string]*graphql.Object{}

	for _, obj := range schema.Vertices {
		props := obj.GetDataMap()
		if props == nil {
			continue
		}
		props["id"] = "STRING"
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
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				srcMap, ok := p.Source.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("source conversion failed: %v", p.Source)
				}
				srcGid, ok := srcMap["id"].(string)
				if !ok {
					return nil, fmt.Errorf("source gid conversion failed: %+v", srcMap)
				}
				q := gripql.V(srcGid).HasLabel(obj.From).Out(obj.Label).HasLabel(obj.To)
				result, err := engine.Traversal(context.Background(), db, workdir, &gripql.GraphQuery{Graph: graph, Query: q.Statements})
				if err != nil {
					return nil, err
				}
				out := []interface{}{}
				for r := range result {
					d := r.GetVertex().GetDataMap()
					d["id"] = r.GetVertex().Gid
					out = append(out, d)
				}
				return out, nil
			},
		}
		objects[obj.From].AddFieldConfig(fname, f)
	}

	return objects, nil
}

func buildQueryObject(db gdbi.GraphDB, workdir string, graph string, objects map[string]*graphql.Object) *graphql.Object {
	queryFields := graphql.Fields{}

	for objName, obj := range objects {
		label := obj.Name()
		f := &graphql.Field{
			Name: objName,
			Type: graphql.NewList(obj),
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{Type: graphql.String},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				q := gripql.V().HasLabel(label)
				if id, ok := p.Args["id"].(string); ok {
					q = gripql.V(id).HasLabel(label)
				}
				result, err := engine.Traversal(context.Background(), db, workdir, &gripql.GraphQuery{Graph: graph, Query: q.Statements})
				if err != nil {
					return nil, err
				}
				out := []interface{}{}
				for r := range result {
					d := r.GetVertex().GetDataMap()
					d["id"] = r.GetVertex().Gid
					out = append(out, d)
				}
				return out, nil
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

	return query
}

func BuildGraphQLSchema(db gdbi.GraphDB, workdir string, graph string, schema *gripql.Graph) (*graphql.Schema, error) {
	if schema == nil {
		return nil, fmt.Errorf("graphql.NewSchema error: nil gripql.Graph for graph: %s", graph)
	}

	objectMap, err := buildObjectMap(db, workdir, graph, schema)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}

	queryObj := buildQueryObject(db, workdir, graph, objectMap)
	schemaConfig := graphql.SchemaConfig{
		Query: queryObj,
	}

	gqlSchema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}

	return &gqlSchema, nil
}
