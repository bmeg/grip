package graphql

import (
	"fmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
	"log"
	"net/http"
	//"github.com/graphql-go/graphql/testutil"
	"github.com/bmeg/arachne/aql"
)

// Handler is a GraphQL endpoint to query the Arachne database
type Handler struct {
	graphqlHadler *handler.Handler
	client        aql.Client
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(address string) http.Handler {
	client, _ := aql.Connect(address, false)

	schema := buildGraphQLSchema(client, "graphql")
	return &Handler{
		graphqlHadler: handler.New(&handler.Config{
			Schema: schema,
		}),
		client: client,
	}
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	gh.graphqlHadler.ServeHTTP(writer, request)
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

func buildGraphQLSchema(client aql.Client, gqlDB string) *graphql.Schema {

	var dataGraph = "example" //TODO: hard coded for the moment

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
