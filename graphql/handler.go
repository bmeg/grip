package graphql

import (
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
	"log"
	"net/http"
	"regexp"
	//"github.com/graphql-go/graphql/testutil"
	"github.com/bmeg/arachne/aql"
)

//handle the graphql queries for a single endpoint
type graphHandler struct {
	graph      string
	schema     string
	gqlHandler *handler.Handler
	timestamp  string
	client     aql.Client
}

// Handler is a GraphQL endpoint to query the Arachne database
type Handler struct {
	handlers map[string]*graphHandler
	client   aql.Client
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(address string) http.Handler {
	client, _ := aql.Connect(address, false)
	h := &Handler{
		client:   client,
		handlers: map[string]*graphHandler{},
	}
	return h
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	pathRE, _ := regexp.Compile("/graphql/(.*)$")
	graphName := pathRE.FindStringSubmatch(request.URL.Path)[1]
	if v, ok := gh.handlers[graphName]; ok {
		v.setup()
		v.gqlHandler.ServeHTTP(writer, request)
	} else {
		v := newGraphHandler(graphName, gh.client)
		v.setup()
		gh.handlers[graphName] = v
		v.gqlHandler.ServeHTTP(writer, request)
	}
}

func newGraphHandler(graph string, client aql.Client) *graphHandler {
	o := &graphHandler{
		graph:  graph,
		schema: fmt.Sprintf("%s:schema", graph),
		client: client,
	}
	o.setup()
	return o
}

func (gh *graphHandler) setup() {
	ts, _ := gh.client.GetTimestamp(gh.schema)
	if ts.Timestamp != gh.timestamp {
		log.Printf("Reloading GraphQL")
		schema := buildGraphQLSchema(gh.client, gh.schema, gh.graph)
		gh.gqlHandler = handler.New(&handler.Config{
			Schema: schema,
		})
		gh.timestamp = ts.Timestamp
	}
}

type object struct {
	name string
	fields map[string]graphql.Type
}

type query struct {
	name string
}

func getObjects(client aql.Client, gqlDB string) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	q := aql.V().HasLabel("Object")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		d := elem.GetValue().GetVertex().GetDataMap()
		out[elem.GetValue().GetVertex().Gid] = d
	}
	return out
}

func getQueries(client aql.Client, gqlDB string) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	q := aql.V().HasLabel("Object")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		d := elem.GetValue().GetVertex().GetDataMap()
		out[elem.GetValue().GetVertex().Gid] = d
	}
	return out
}

func getQueryFields(client aql.Client, gqlDB string, queryGID string) map[string]string {
	out := map[string]string{}
	q := aql.V(queryGID).OutEdge("field").As("a").Out().As("b").Select("a", "b")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		out[fieldName] = fieldObj
	}
	return out
}

func getObjectFields(client aql.Client, gqlDB string, queryGID string) map[string]string {
	out := map[string]string{}
	q := aql.V(queryGID).OutEdge("field").As("a").Out().As("b").Select("a", "b")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		out[fieldName] = fieldObj
	}
	return out
}

func buildGraphQLSchema(client aql.Client, gqlDB string, dataGraph string) *graphql.Schema {

	objects := map[string]*graphql.Object{}

	for gid, obj := range getObjects(client, gqlDB) {
		fields := graphql.Fields{}
		for fname, ftype := range obj["fields"].(map[string]interface{}) {
			log.Printf("%s %s", fname, ftype)
			if ftype == "Int" {
				fields[fname] = &graphql.Field{Type: graphql.Int}
			} else if ftype == "String" || ftype == "string" {
				fields[fname] = &graphql.Field{Type: graphql.String}
			} else if ftype == "Float" {
				fields[fname] = &graphql.Field{Type: graphql.Float}
			}
		}
		gobj := graphql.NewObject(
			graphql.ObjectConfig{
				Name:   obj["name"].(string),
				Fields: fields,
			},
		)
		objects[gid] = gobj
	}

	for gid := range getObjects(client, gqlDB) {
		for edgeName, objID := range getObjectFields(client, gqlDB, gid) {
			log.Printf("Object Field %s %s", edgeName, objID)
			//lID := objID
			f := graphql.Field{
				Name: edgeName,
				Type: graphql.NewList(objects[gid]),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					srcMap := p.Source.(map[string]interface{})
					srcGid := srcMap["__gid"].(string)
					q := aql.V(srcGid).Out(edgeName)
					result, _ := client.Execute(dataGraph, q)
					out := []interface{}{}
					for r := range result {
						i := r.GetValue().GetVertex().GetDataMap()
						i["__gid"] = r.GetValue().GetVertex().Gid
						out = append(out, i)
					}
					return out, nil
				},
			}
			objects[gid].AddFieldConfig(edgeName, &f)
		}
	}

	queryFields := graphql.Fields{}
	for gid, data := range getQueries(client, gqlDB) {
		log.Printf("Query %s %s", gid, data)
		for edgeName, objID := range getQueryFields(client, gqlDB, gid) {
			log.Printf("query field %s %s %s", edgeName, objID, objects[objID])

			queryFields[edgeName] = &graphql.Field{
				Type: objects[objID],
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{
						Type: graphql.String,
					},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					log.Printf("Scanning %s", p.Args)
					v, err := client.GetVertex(dataGraph, p.Args["id"].(string))
					if v == nil || err != nil {
						return nil, fmt.Errorf("Not found")
					}
					d := v.GetDataMap()
					d["__gid"] = v.Gid
					return d, nil
				},
			}
		}
	}
	log.Printf("Fields: %#v", queryFields)
	queryType := graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: queryFields,
		})
	schemaConfig := graphql.SchemaConfig{
		Query: queryType,
	}
	log.Printf("GraphQL Schema: %s", schemaConfig)
	schema, _ := graphql.NewSchema(schemaConfig)
	return &schema
}
