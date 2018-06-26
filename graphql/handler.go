/*
GraphQL Web endpoint
*/

package graphql

import (
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/util/rpc"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

//handle the graphql queries for a single endpoint
type graphHandler struct {
	graph      string
	gqlHandler *handler.Handler
	timestamp  string
	client     aql.Client
	schema     *aql.GraphSchema
}

// Handler is a GraphQL endpoint to query the Arachne database
type Handler struct {
	handlers map[string]*graphHandler
	client   aql.Client
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(rpcAddress, user, password string) (http.Handler, error) {
	rpcConf := rpc.ConfigWithDefaults(rpcAddress)
	rpcConf.User = user
	rpcConf.Password = password
	client, err := aql.Connect(rpcConf, false)
	if err != nil {
		return nil, err
	}
	h := &Handler{
		client:   client,
		handlers: map[string]*graphHandler{},
	}
	return h, nil
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	pathRE := regexp.MustCompile("/graphql/(.*)$")
	graphName := pathRE.FindStringSubmatch(request.URL.Path)[1]
	var handler *graphHandler
	var ok bool
	if handler, ok = gh.handlers[graphName]; ok {
		handler.setup()
	} else {
		handler = newGraphHandler(graphName, gh.client)
		gh.handlers[graphName] = handler
	}
	if handler != nil && handler.gqlHandler != nil {
		handler.gqlHandler.ServeHTTP(writer, request)
	} else {
		http.Error(writer, fmt.Sprintf("No GraphQL handler found for graph: %s", graphName), http.StatusInternalServerError)
	}
}

// newGraphHandler creates a new graphql handler from schema
func newGraphHandler(graph string, client aql.Client) *graphHandler {
	o := &graphHandler{
		graph:  graph,
		client: client,
	}
	o.setup()
	return o
}

// check timestamp to see if schema needs to be updated, and if so
// rebuild graphql schema
func (gh *graphHandler) setup() {
	ts, _ := gh.client.GetTimestamp(gh.graph)
	if ts == nil || ts.Timestamp != gh.timestamp {
		log.Printf("Reloading GraphQL schema for graph: %s", gh.graph)
		schema, err := buildGraphQLSchema(gh.client, gh.graph)
		if err != nil {
			log.Printf("GraphQL schema build failed: %v", err)
			gh.gqlHandler = nil
			gh.timestamp = ""
		} else {
			log.Printf("Built GraphQL schema for graph: %s: %+v", gh.graph, schema)
			gh.gqlHandler = handler.New(&handler.Config{
				Schema: schema,
			})
			gh.timestamp = ts.Timestamp
		}
	}
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
		return nil, fmt.Errorf("%s does not map to a GQL type", x)
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
			return nil, fmt.Errorf("object: %s: field: %s: error: %v", name, key, err)
		}
	}

	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   name,
			Fields: objFields,
		},
	), nil
}

func buildObjectMap(client aql.Client, graph string, schema *aql.GraphSchema) (map[string]*graphql.Object, error) {
	objects := map[string]*graphql.Object{}

	for _, obj := range schema.Vertices {
		gqlObj, err := buildObject(obj.Label, obj.GetDataMap())
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
				srcGid, ok := srcMap["__gid"].(string)
				if !ok {
					return nil, fmt.Errorf("source gid conversion failed: %+v", srcMap)
				}
				q := aql.V(srcGid).Where(aql.Eq("_label", obj.From)).Out(obj.Label).Where(aql.Eq("_label", obj.To))
				result, err := client.Traversal(&aql.GraphQuery{Graph: graph, Query: q.Statements})
				if err != nil {
					return nil, err
				}
				out := []interface{}{}
				for r := range result {
					d := r.GetVertex().GetDataMap()
					d["__gid"] = r.GetVertex().Gid
					out = append(out, d)
				}
				return out, nil
			},
		}
		objects[obj.From].AddFieldConfig(fname, f)
	}

	return objects, nil
}

func buildQueryObject(client aql.Client, graph string, objects map[string]*graphql.Object) *graphql.Object {
	queryFields := graphql.Fields{}

	for objName, obj := range objects {
		label := obj.Name()
		f := &graphql.Field{
			Name: objName,
			Type: graphql.NewList(obj),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				q := aql.V().Where(aql.Eq("_label", label))
				result, err := client.Traversal(&aql.GraphQuery{Graph: graph, Query: q.Statements})
				if err != nil {
					return nil, err
				}
				out := []interface{}{}
				for r := range result {
					d := r.GetVertex().GetDataMap()
					d["__gid"] = r.GetVertex().Gid
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

func buildGraphQLSchema(client aql.Client, graph string) (*graphql.Schema, error) {
	schema, err := client.GetSchema(graph)
	if err != nil {
		return nil, fmt.Errorf("GetSchema error: %v", err)
	}
	objectMap, err := buildObjectMap(client, graph, schema)
	if err != nil {
		return nil, err
	}
	queryObj := buildQueryObject(client, graph, objectMap)
	schemaConfig := graphql.SchemaConfig{
		Query: queryObj,
	}
	gqlSchema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}
	return &gqlSchema, nil
}
