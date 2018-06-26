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
	var v *graphHandler
	var ok bool
	if v, ok = gh.handlers[graphName]; ok {
		v.setup()
	} else {
		v = newGraphHandler(graphName, gh.client)
		gh.handlers[graphName] = v
	}
	if v != nil && v.gqlHandler != nil {
		v.gqlHandler.ServeHTTP(writer, request)
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

type edgeField struct {
	edgeLabel string
	srcType   string
	dstType   string
}

func (f *edgeField) toGQL(client aql.Client, graph string, objects map[string]*graphql.Object) *graphql.Field {
	o := &graphql.Field{
		Name: f.edgeLabel + "_" + f.dstType,
		Type: graphql.NewList(objects[f.dstType]),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			srcMap, ok := p.Source.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("source conversion failed: %v", p.Source)
			}
			srcGid, ok := srcMap["__gid"].(string)
			if !ok {
				return nil, fmt.Errorf("source gid conversion failed: %+v", srcMap)
			}
			q := aql.V(srcGid).Where(aql.Eq("_label", f.srcType)).Out(f.edgeLabel).Where(aql.Eq("_label", f.dstType))
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
	return o
}

func fieldType(x string) *graphql.Field {
	var o *graphql.Field
	switch x {
	case "NUMERIC":
		o = &graphql.Field{Type: graphql.Float}
	case "STRING":
		o = &graphql.Field{Type: graphql.String}
	case "BOOL":
		o = &graphql.Field{Type: graphql.Boolean}
	default:
		log.Printf("Unknown GQL type: %v", x)
	}
	return o
}

func buildObject(objName string, objSchema map[string]interface{}) *graphql.Object {
	fields := graphql.Fields{}

	for fname, ftype := range objSchema {

		// handle map
		if x, ok := ftype.(map[string]interface{}); ok {
			if m := buildObject(fname, x); m != nil {
				fields[fname] = &graphql.Field{Type: m}
			}

			// handle slice
		} else if x, ok := ftype.([]interface{}); ok {
			// we only look at the first element to determine the schema of array elements
			if len(x) > 0 {
				y := x[0]
				if z, ok := y.(map[string]interface{}); ok {
					if m := buildObject(fname, z); m != nil {
						fields[fname] = &graphql.Field{Type: graphql.NewList(m)}
					}
				} else if z, ok := y.(string); ok {
					fields[fname] = fieldType(z)
				} else {
					log.Printf("Unknown GQL type for %s: %v", fname, ftype)
				}
			} else {
				log.Printf("Encountered empty list for field: %s", fname)
			}

			// handle string
		} else if x, ok := ftype.(string); ok {
			fields[fname] = fieldType(x)

			// handle other cases
		} else {
			log.Printf("Unknown GQL type for %s: %v", fname, ftype)
		}
	}

	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   objName,
			Fields: fields,
		},
	)
}

func buildObjectMap(client aql.Client, graph string, schema *aql.GraphSchema) map[string]*graphql.Object {
	objects := map[string]*graphql.Object{}

	for _, obj := range schema.Vertices {
		gqlObj := buildObject(obj.Label, obj.GetDataMap())
		objects[obj.Label] = gqlObj
	}

	for _, obj := range schema.Edges {
		field := &edgeField{obj.Label, obj.From, obj.To}
		f := field.toGQL(client, graph, objects)
		objects[obj.From].AddFieldConfig(obj.Label+"_"+obj.To, f)
	}

	return objects
}

func buildQueryObject(client aql.Client, graph string, objects map[string]*graphql.Object) *graphql.Object {
	queryFields := graphql.Fields{}

	for objName, obj := range objects {
		label := obj.Name()
		log.Printf("objName: %+v", objName)
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
		return nil, fmt.Errorf("getting schema: %v", err)
	}

	objectMap := buildObjectMap(client, graph, schema)
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
