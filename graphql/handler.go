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

// newGraphHandler creates a new graphql handler from schema
func newGraphHandler(graph string, client aql.Client) *graphHandler {
	o := &graphHandler{
		graph:  graph,
		schema: fmt.Sprintf("%s:schema", graph),
		client: client,
	}
	o.setup()
	return o
}

// check timestamp to see if schema needs to be updated, and if so
// rebuild graphql schema
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

// getObjects finds all V.HasLabel('Object') as map[gid]data
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
	q := aql.V().HasLabel("Query")
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
		log.Printf("Field Row :%s: %s", queryGID, elem.GetRow())
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		out[fieldName] = fieldObj
	}
	return out
}

// a field that represent a link to another object
type objectField struct {
	name    string
	label   string
	dstType string
}

func getObjectFields(client aql.Client, gqlDB string, queryGID string) map[string]objectField {
	out := map[string]objectField{}
	q := aql.V(queryGID).OutEdge("field").As("a").Out().As("b").Select("a", "b")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		label := fieldName
		if elem.GetRow()[0].GetEdge().HasProperty("label") {
			label = elem.GetRow()[0].GetEdge().GetProperty("label").(string)
		}
		out[fieldName] = objectField{fieldName, label, fieldObj}
	}
	return out
}

func buildObjectMap(client aql.Client, gqlDB string, dataGraph string) map[string]*graphql.Object {

	objects := map[string]*graphql.Object{}

	//create instance of ever object type, and add constant fields
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

	//list all objects, but this time find edges to other objects that create
	//fields that expand into other objects
	for gid := range getObjects(client, gqlDB) {
		for edgeName, field := range getObjectFields(client, gqlDB, gid) {
			log.Printf("Object Field %s %s", edgeName, field)
			//lID := objID
			lField := field
			f := graphql.Field{
				Name: edgeName,
				Type: graphql.NewList(objects[lField.dstType]),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					srcMap := p.Source.(map[string]interface{})
					srcGid := srcMap["__gid"].(string)
					//log.Printf("Scanning edge from %s out '%s'", srcGid, lField.label)
					q := aql.V(srcGid).Both(lField.label)
					result, _ := client.Execute(dataGraph, q)
					out := []interface{}{}
					for r := range result {
						//log.Printf("Results: %s", r)
						i := r.GetValue().GetVertex().GetDataMap()
						i["__gid"] = r.GetValue().GetVertex().Gid
						out = append(out, i)
					}
					return out, nil
				},
			}
			log.Printf("Add object field %s from %s to %s = %#v", edgeName, gid, field, f)
			objects[gid].AddFieldConfig(edgeName, &f)
		}
	}
	return objects
}

func buildQueryObject(client aql.Client, gqlDB string, dataGraph string, objects map[string]*graphql.Object) *graphql.Object {

	queryFields := graphql.Fields{}
	//find all defined queries and add them as fields to the base query object
	for gid, data := range getQueries(client, gqlDB) {
		log.Printf("Query %s %s", gid, data)
		for edgeName, objID := range getQueryFields(client, gqlDB, gid) {
			lEdgeName := edgeName
			log.Printf("query field %s %s %s", lEdgeName, objID, objects[objID])
			f := &graphql.Field{
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
			log.Printf("Add query field %s %s %#v", objID, lEdgeName, f)
			queryFields[lEdgeName] = f
		}
	}
	log.Printf("QueryFields: %#v", queryFields)
	queryType := graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: queryFields,
		})
	return queryType
}

func buildGraphQLSchema(client aql.Client, gqlDB string, dataGraph string) *graphql.Schema {

	objects := buildObjectMap(client, gqlDB, dataGraph)

	queryType := buildQueryObject(client, gqlDB, dataGraph, objects)

	schemaConfig := graphql.SchemaConfig{
		Query: queryType,
	}
	log.Printf("GraphQL Schema: %s", schemaConfig)
	schema, _ := graphql.NewSchema(schemaConfig)
	return &schema
}
