package graphql


import (
  "fmt"
  "testing"
  "context"
  "github.com/machinebox/graphql"
)

func TestGraphQL(t *testing.T) {
  fmt.Printf("Running GraphQL testing\n")
  url := fmt.Sprintf("http://%s/graphql/%s", GraphQLAddr, GraphName)
  client := graphql.NewClient(url)

  // make a request
  req := graphql.NewRequest(`
    query {
      Human(id:"1000"){
        name,
        friend_to_Human{
          name
        }
      }
    }
  `)

  ctx := context.Background()
  respData := map[string]interface{}{}
  if err := client.Run(ctx, req, &respData); err != nil {
    t.Error(err)
  }
  fmt.Printf("Response: %#v\n", respData  )

}
