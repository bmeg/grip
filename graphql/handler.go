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
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
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
func NewHTTPHandler(rpcAddress string) http.Handler {
	client, _ := aql.Connect(rpcAddress, false)
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
	var v *graphHandler
	var ok bool
	if v, ok = gh.handlers[graphName]; ok {
		v.setup()
	} else {
		v := newGraphHandler(graphName, gh.client)
		v.setup()
		gh.handlers[graphName] = v
	}
	if v != nil && v.gqlHandler != nil {
		v.gqlHandler.ServeHTTP(writer, request)
	} else {
		http.Error(writer, "GraphQL Schema error", http.StatusInternalServerError)
	}
}

// newGraphHandler creates a new graphql handler from schema
func newGraphHandler(graph string, client aql.Client) *graphHandler {
	o := &graphHandler{
		graph:  graph,
		schema: fmt.Sprintf("%s-schema", graph),
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
		schema, err := buildGraphQLSchema(gh.client, gh.schema, gh.graph)
		if err != nil {
			log.Printf("Graph Schema build Failed")
			gh.gqlHandler = nil
			gh.timestamp = ""
		} else {
			gh.gqlHandler = handler.New(&handler.Config{
				Schema: schema,
			})
			gh.timestamp = ts.Timestamp
		}
	}
}

// getObjects finds all vertexes with label ('Object') as map[gid]data
func getObjects(client aql.Client, gqlDB string) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	q := aql.V().Where(aql.Eq("_label", "Object"))
	results, _ := client.Traversal(&aql.GraphQuery{Graph: gqlDB, Query: q.Statements})
	for elem := range results {
		d := elem.GetVertex().GetDataMap()
		out[elem.GetVertex().Gid] = d
	}
	return out
}

// a field that represent a link to another object

type fieldType int

const (
	idQuery fieldType = iota
	idList
	objectList
)

type objectField struct {
	name      string
	edgeLabel string
	dstType   string
	fieldType fieldType
}

func (f objectField) toGQL(client aql.Client, dataGraph string, objects map[string]*graphql.Object) *graphql.Field {
	if f.fieldType == objectList {
		o := &graphql.Field{
			Name: f.name,
			Type: graphql.NewList(objects[f.dstType]),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				srcMap := p.Source.(map[string]interface{})
				srcGid := srcMap["__gid"].(string)
				//log.Printf("Scanning edge from %s out '%s'", srcGid, lField.label)
				q := aql.V(srcGid).Both(f.edgeLabel)
				result, _ := client.Traversal(&aql.GraphQuery{Graph: dataGraph, Query: q.Statements})
				out := []interface{}{}
				for r := range result {
					//log.Printf("Results: %s", r)
					i := r.GetVertex().GetDataMap()
					i["__gid"] = r.GetVertex().Gid
					out = append(out, i)
				}
				return out, nil
			},
		}
		//log.Printf("Add object field %s to %s = %#v", f.name, f.dstType, o)
		return o
	} else if f.fieldType == idQuery {
		//log.Printf("query id field %s %s", f.name, objects[f.dstType])
		o := &graphql.Field{
			Type: objects[f.dstType],
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{
					Type: graphql.String,
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				//log.Printf("Scanning %s", p.Args)
				v, err := client.GetVertex(dataGraph, p.Args["id"].(string))
				if v == nil || err != nil {
					return nil, fmt.Errorf("Not found")
				}
				d := v.GetDataMap()
				d["__gid"] = v.Gid
				return d, nil
			},
		}
		return o
	} else if f.fieldType == idList {
		//log.Printf("query id field %s %s", f.name, objects[f.dstType])
		o := &graphql.Field{
			Type: graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				log.Printf("Looking up ids: %s", f.dstType)
				q := aql.V().Where(aql.Eq("label", f.dstType))
				result, _ := client.Traversal(&aql.GraphQuery{Graph: dataGraph, Query: q.Statements})
				out := []interface{}{}
				for r := range result {
					i := r.GetVertex().Gid
					out = append(out, i)
				}
				return out, nil
			},
		}
		return o
	}
	log.Printf("Unhandled type!!!")
	return nil
}

func getQueries(client aql.Client, gqlDB string) map[string]objectField {
	out := map[string]objectField{}
	q := aql.V().Where(aql.Eq("_label", "Query"))
	results, _ := client.Traversal(&aql.GraphQuery{Graph: gqlDB, Query: q.Statements})
	found := false
	for elem := range results {
		found = true
		d := elem.GetVertex().Gid
		for k, v := range getObjectFields(client, gqlDB, d) {
			out[k] = v
		}
	}
	if !found {
		log.Printf("No Root query node found")
	}
	return out
}

func getObjectFields(client aql.Client, gqlDB string, queryGID string) map[string]objectField {
	out := map[string]objectField{}
	q := aql.V(queryGID).OutEdge("field").Mark("a").Out().Mark("b").Select("a", "b")
	results, _ := client.Traversal(&aql.GraphQuery{Graph: gqlDB, Query: q.Statements})
	for elem := range results {
		fieldName := elem.GetSelections().Selections["a"].GetEdge().GetProperty("name")
		if fieldName != nil {
			if fieldNameStr, ok := fieldName.(string); ok {
				fieldObj := elem.GetSelections().Selections["b"].GetVertex().Gid
				label := fieldNameStr
				if elem.GetSelections().Selections["a"].GetEdge().HasProperty("label") {
					l := elem.GetSelections().Selections["a"].GetEdge().GetProperty("label")
					if lStr, ok := l.(string); ok {
						label = lStr
					}
				}
				t := objectList
				if elem.GetSelections().Selections["a"].GetEdge().HasProperty("type") {
					tp := elem.GetSelections().Selections["a"].GetEdge().GetProperty("type")
					if tf, ok := tp.(string); ok {
						if tf == "idList" {
							t = idList
						} else if tf == "idQuery" {
							t = idQuery
						} else {
							log.Printf("Unknown Field type: %s %s", fieldName, tf)
						}
					} else {
						log.Printf("Object field link type not a string")
					}
				}
				out[fieldNameStr] = objectField{fieldNameStr, label, fieldObj, t}
			} else {
				log.Printf("Field name is not string")
			}
		} else {
			log.Printf("Edge missing name parameter: %#v", elem.GetSelections().Selections["a"].GetEdge())
		}
	}
	return out
}

func buildObject(name string, schema map[string]interface{}) *graphql.Object {
	fields := graphql.Fields{}
	//log.Printf("BUILDING: %s", name)
	for fname, ftype := range schema {
		if x, ok := ftype.(map[string]interface{}); ok {
			if m := buildObject(fname, x); m != nil {
				fields[fname] = &graphql.Field{Type: m}
			}
		} else if x, ok := ftype.([]interface{}); ok {
			//log.Printf("array: %s", x)
			//we only look at the first element to determine the schema of array elements
			if len(x) > 0 {
				y := x[0]
				if z, ok := y.(map[string]interface{}); ok {
					if m := buildObject(fname, z); m != nil {
						fields[fname] = &graphql.Field{Type: graphql.NewList(m)}
					}
				} else {
					log.Printf("Unknown GQL type %#v", ftype)
				}
			}
		} else if x, ok := ftype.(string); ok {
			//log.Printf("%s %s", fname, ftype)
			if x == "Int" {
				fields[fname] = &graphql.Field{Type: graphql.Int}
			} else if x == "String" || x == "string" {
				fields[fname] = &graphql.Field{Type: graphql.String}
			} else if x == "Float" {
				fields[fname] = &graphql.Field{Type: graphql.Float}
			}
		} else {
			log.Printf("Unknown GQL type %#v", ftype)
		}
	}
	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   name,
			Fields: fields,
		},
	)
}

func buildObjectMap(client aql.Client, gqlDB string, dataGraph string) map[string]*graphql.Object {

	objects := map[string]*graphql.Object{}

	//create instance of ever object type, and add constant fields
	for gid, obj := range getObjects(client, gqlDB) {
		if x, ok := obj["fields"]; ok {
			if schema, ok := x.(map[string]interface{}); ok {
				gobj := buildObject(gid, schema)
				objects[gid] = gobj
			}
		}
	}

	//list all objects, but this time find edges to other objects that create
	//fields that expand into other objects
	for srcObj := range getObjects(client, gqlDB) {
		for fieldName, field := range getObjectFields(client, gqlDB, srcObj) {
			//log.Printf("Object Field %s %s %s", srcObj, fieldName, field.dstType)
			f := field.toGQL(client, dataGraph, objects)
			objects[srcObj].AddFieldConfig(fieldName, f)
		}
	}
	return objects
}

func buildQueryObject(client aql.Client, gqlDB string, dataGraph string, objects map[string]*graphql.Object) *graphql.Object {

	queryFields := graphql.Fields{}
	//find all defined queries and add them as fields to the base query object
	for fieldName, field := range getQueries(client, gqlDB) {
		//log.Printf("query %s field %s %s", gid, field.name, objects[field.dstType])
		f := field.toGQL(client, dataGraph, objects)
		queryFields[fieldName] = f
	}
	if len(queryFields) == 0 {
		log.Printf("No GraphQL query fields found")
	}
	//log.Printf("QueryFields: %#v", queryFields)
	queryType := graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: queryFields,
		})
	return queryType
}

func buildGraphQLSchema(client aql.Client, gqlDB string, dataGraph string) (*graphql.Schema, error) {
	objects := buildObjectMap(client, gqlDB, dataGraph)
	queryType := buildQueryObject(client, gqlDB, dataGraph, objects)
	schemaConfig := graphql.SchemaConfig{
		Query: queryType,
	}
	//log.Printf("GraphQL Schema: %s", schemaConfig)
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.Printf("graphql.NewSchema error: %s", err)
	}
	return &schema, err
}
