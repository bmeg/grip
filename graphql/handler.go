
package graphql

import (
  "log"
  "net/http"
  "github.com/graphql-go/handler"
  "github.com/graphql-go/graphql"
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
      Schema:  schema,
    }),
    client:client,
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
    log.Printf("%s", elem.GetRow()[0])
  }

  results, _ = client.Query(gqlDB).V(queryGID).OutEdge("field").Execute()
  for elem := range results {
    log.Printf("query vertex: %s", elem)
  }

  return out
}

func BuildGraphQLSchema(client aql.AQLClient, gqlDB string) *graphql.Schema {
  /*
  results, _ := query.V().HasLabel("Interface").Execute()
  for elem := range results {
    data := elem.GetValue().GetVertex().GetDataMap()
    //i := graphql.InterfaceConfig{}
    fields := graphql.Fields{}
    for fname, ftype := range data["fields"].(map[string]interface{}) {
      log.Printf("%s %s", fname, ftype)
      if ftype == "Int" {
        fields[fname] = &graphql.Field{Type:graphql.Int}
      } else if ftype == "String" {
        fields[fname] = &graphql.Field{Type:graphql.String}
      } else if ftype == "Float" {
        fields[fname] = &graphql.Field{Type:graphql.Float}
      }
    }
    iface := graphql.InterfaceConfig{
      Name: elem.GetValue().GetVertex().Gid,
      Fields: fields,
    }
  }
  */

  //query := client.Query(gqlDB)
  objects := map[string]*graphql.Object{}

  for gid, obj := range getObjects(client, gqlDB) {
    fields := graphql.Fields{}
    for fname, ftype := range obj["fields"].(map[string]interface{}) {
      log.Printf("%s %s", fname, ftype)
      if ftype == "Int" {
        fields[fname] = &graphql.Field{Type:graphql.Int}
      } else if ftype == "String" || ftype == "string" {
        fields[fname] = &graphql.Field{Type:graphql.String}
      } else if ftype == "Float" {
        fields[fname] = &graphql.Field{Type:graphql.Float}
      }
    }
    gobj := graphql.NewObject(
	     graphql.ObjectConfig {
	        Name: obj["name"].(string),
          Fields: fields,
        },
    )
    objects[gid] = gobj
  }

  query_fields := graphql.Fields{}
  for gid, data := range getQueries(client, gqlDB) {
    log.Printf("Query %s %s", gid, data)
    for edgeName, objID := range getQueryFields(client, gqlDB, gid) {
      log.Printf("query field %s %s", edgeName, objID)
      /*
      query_fields[edgeName] = &graphql.Field{
        Type:graphql.String,
        Args: graphql.FieldConfigArgument{
  				"id": &graphql.ArgumentConfig{
  					Type: graphql.String,
  				},
  			},
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          log.Printf("Scanning %s", p.Args)
          return "Testing", nil
  			    //return filterUser(data, p.Args), nil
  			},
      }
      */
    }
  }

  queryType := graphql.NewObject(
  	graphql.ObjectConfig{
  		Name: "Query",
  		Fields: query_fields,
  })
  schemaConfig := graphql.SchemaConfig{
    Query: queryType,
  }
  log.Printf("GraphQL Schema: %s", schemaConfig)
  schema, _ := graphql.NewSchema(schemaConfig)
  return &schema
}
