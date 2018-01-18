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

type GraphQLHandler struct {
	graphqlHadler *handler.Handler
	client        aql.AQLClient
}

func NewHTTPHandler(address string) http.Handler {
	client, _ := aql.Connect(address, false)

	schema := BuildGraphQLSchema(client, "graphql")
	return &GraphQLHandler{
		graphqlHadler: handler.New(&handler.Config{
			Schema: schema,
		}),
		client: client,
	}
}

func (self *GraphQLHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	self.graphqlHadler.ServeHTTP(writer, request)
}

func getObjects(client aql.AQLClient, gqlDB string) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	results, _ := client.Query(gqlDB).V().HasLabel("Object").Execute()
	for elem := range results {
		d := elem.GetValue().GetVertex().GetDataMap()
		out[elem.GetValue().GetVertex().Gid] = d
	}
	return out
}

func getQueries(client aql.AQLClient, gqlDB string) map[string]map[string]interface{} {
	out := map[string]map[string]interface{}{}
	results, _ := client.Query(gqlDB).V().HasLabel("Query").Execute()
	for elem := range results {
		d := elem.GetValue().GetVertex().GetDataMap()
		out[elem.GetValue().GetVertex().Gid] = d
	}
	return out
}

func getQueryFields(client aql.AQLClient, gqlDB string, queryGID string) map[string]string {
	out := map[string]string{}
	results, _ := client.Query(gqlDB).V(queryGID).OutEdge("field").As("a").Out().As("b").Select("a", "b").Execute()
	for elem := range results {
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		out[fieldName] = fieldObj
	}
	return out
}

func getObjectFields(client aql.AQLClient, gqlDB string, queryGID string) map[string]string {
	out := map[string]string{}
	results, _ := client.Query(gqlDB).V(queryGID).OutEdge("field").As("a").Out().As("b").Select("a", "b").Execute()
	for elem := range results {
		fieldName := elem.GetRow()[0].GetEdge().GetProperty("name").(string)
		fieldObj := elem.GetRow()[1].GetVertex().Gid
		out[fieldName] = fieldObj
	}
	return out
}

func BuildGraphQLSchema(client aql.AQLClient, gqlDB string) *graphql.Schema {

	var dataGraph string = "example" //TODO: hard coded for the moment

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

	for gid, _ := range getObjects(client, gqlDB) {
		for edgeName, objID := range getObjectFields(client, gqlDB, gid) {
			log.Printf("Object Field %s %s", edgeName, objID)
			//lID := objID
			f := graphql.Field{
				Name: edgeName,
				Type: graphql.NewList(objects[gid]),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					srcMap := p.Source.(map[string]interface{})
					srcGid := srcMap["__gid"].(string)
					result, _ := client.Query(dataGraph).V(srcGid).Out(edgeName).Execute()
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

	query_fields := graphql.Fields{}
	for gid, data := range getQueries(client, gqlDB) {
		log.Printf("Query %s %s", gid, data)
		for edgeName, objID := range getQueryFields(client, gqlDB, gid) {
			log.Printf("query field %s %s %s", edgeName, objID, objects[objID])

			query_fields[edgeName] = &graphql.Field{
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
	log.Printf("Fields: %#v", query_fields)
	queryType := graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: query_fields,
		})
	schemaConfig := graphql.SchemaConfig{
		Query: queryType,
	}
	log.Printf("GraphQL Schema: %s", schemaConfig)
	schema, _ := graphql.NewSchema(schemaConfig)
	return &schema
}
