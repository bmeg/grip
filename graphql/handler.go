/*
GraphQL Web endpoint
*/

package graphql

import (
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
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
		log.Printf("Add object field %s to %s = %#v", f.name, f.dstType, o)
		return o
	} else if f.fieldType == idQuery {
		log.Printf("query id field %s %s", f.name, objects[f.dstType])
		o := &graphql.Field{
			Type: objects[f.dstType],
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
		return o
	} else if f.fieldType == idList {
		log.Printf("query id field %s %s", f.name, objects[f.dstType])
		o := &graphql.Field{
			Type: graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				log.Printf("Looking up ids: %s", f.dstType)
				q := aql.V().HasLabel(f.dstType)
				result, _ := client.Execute(dataGraph, q)
				out := []interface{}{}
				for r := range result {
					i := r.GetValue().GetVertex().Gid
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
	q := aql.V().HasLabel("Query")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		d := elem.GetValue().GetVertex().Gid
		for k, v := range getObjectFields(client, gqlDB, d) {
			out[k] = v
		}
	}
	return out
}

func getObjectFields(client aql.Client, gqlDB string, queryGID string) map[string]objectField {
	out := map[string]objectField{}
	q := aql.V(queryGID).OutEdge("field").As("a").Out().As("b").Select("a", "b")
	results, _ := client.Execute(gqlDB, q)
	for elem := range results {
		log.Printf("objectField: %s %s %s", queryGID, elem.GetRow()[0], elem.GetRow()[1].GetVertex().Gid)
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		label := fieldName
		if elem.GetRow()[0].GetEdge().HasProperty("label") {
			label = elem.GetRow()[0].GetEdge().GetProperty("label").(string)
		}
		t := objectList
		if elem.GetRow()[0].GetEdge().HasProperty("type") {
			tf := elem.GetRow()[0].GetEdge().GetProperty("type").(string)
			if tf == "idList" {
				t = idList
			} else if tf == "idQuery" {
				t = idQuery
			} else {
				log.Printf("Unknown Field type: %s %s", fieldName, tf)
			}
		}
		out[fieldName] = objectField{fieldName, label, fieldObj, t}
	}
	return out
}

func buildObject(name string, schema map[string]interface{}) *graphql.Object {
	fields := graphql.Fields{}
	log.Printf("BUILDING: %s", name)
	for fname, ftype := range schema {
		if x, ok := ftype.(map[string]interface{}); ok {
			if m := buildObject(fname, x); m != nil {
				fields[fname] = &graphql.Field{Type: m}
			}
		} else if x, ok := ftype.([]interface{}); ok {
			log.Printf("array: %s", x)
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
			log.Printf("%s %s", fname, ftype)
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
			log.Printf("Object Field %s %s %s", srcObj, fieldName, field.dstType)
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
	//log.Printf("QueryFields: %#v", queryFields)
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
	//log.Printf("GraphQL Schema: %s", schemaConfig)
	schema, _ := graphql.NewSchema(schemaConfig)
	return &schema
}
