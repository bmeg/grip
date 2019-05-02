/*
GraphQL Web endpoint
*/

package graphql

import (
	"fmt"
	"net/http"
	"regexp"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/rpc"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
	log "github.com/sirupsen/logrus"
)

//handle the graphql queries for a single endpoint
type graphHandler struct {
	graph      string
	gqlHandler *handler.Handler
	timestamp  string
	client     gripql.Client
	schema     *gripql.Graph
}

// Handler is a GraphQL endpoint to query the Grip database
type Handler struct {
	handlers map[string]*graphHandler
	client   gripql.Client
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(rpcAddress, user, password string) (http.Handler, error) {
	rpcConf := rpc.ConfigWithDefaults(rpcAddress)
	rpcConf.User = user
	rpcConf.Password = password
	client, err := gripql.Connect(rpcConf, false)
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
func newGraphHandler(graph string, client gripql.Client) *graphHandler {
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
	ts, err := gh.client.GetTimestamp(gh.graph)
	if err != nil {
		log.WithFields(log.Fields{"graph": gh.graph, "error": err}).Error("GetTimestamp error")
		return
	}
	if ts == nil || ts.Timestamp != gh.timestamp {
		log.WithFields(log.Fields{"graph": gh.graph}).Info("Reloading GraphQL schema")
		schema, err := gh.client.GetSchema(gh.graph)
		if err != nil {
			log.WithFields(log.Fields{"graph": gh.graph, "error": err}).Error("GetSchema error")
			return
		}
		gqlSchema, err := buildGraphQLSchema(schema, gh.client, gh.graph)
		if err != nil {
			log.WithFields(log.Fields{"graph": gh.graph, "error": err}).Error("GraphQL schema build failed")
			return
		}
		log.WithFields(log.Fields{"graph": gh.graph}).Info("Built GraphQL schema")
		gh.schema = schema
		gh.gqlHandler = handler.New(&handler.Config{
			Schema: gqlSchema,
		})
		gh.timestamp = ts.Timestamp
	} else {
		log.WithFields(log.Fields{"graph": gh.graph}).Info("Using cached GraphQL schema")
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

func buildObjectMap(client gripql.Client, graph string, schema *gripql.Graph) (map[string]*graphql.Object, error) {
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
				result, err := client.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
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

func buildQueryObject(client gripql.Client, graph string, objects map[string]*graphql.Object) *graphql.Object {
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
				result, err := client.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
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

func buildGraphQLSchema(schema *gripql.Graph, client gripql.Client, graph string) (*graphql.Schema, error) {
	if schema == nil {
		return nil, fmt.Errorf("graphql.NewSchema error: nil gripql.Graph for graph: %s", graph)
	}

	objectMap, err := buildObjectMap(client, graph, schema)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
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
