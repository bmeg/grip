package graphql

import (
	"fmt"
	//"io/ioutil"
	//"net/http"
	//"bytes"
	"context"
	"testing"

	"github.com/machinebox/graphql"
)

func TestCharacterQuery(t *testing.T) {
	url := fmt.Sprintf("http://%s/graphql/%s", GraphQLAddr, GraphName)
	client := graphql.NewClient(url)

	// make a request
	req := graphql.NewRequest(`
    query {
      Character(id:"1"){
        name
        starships_to_Starship {
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
	fmt.Printf("Response: %#v\n", respData)
}

func TestIntrospectionQuery(t *testing.T) {
	url := fmt.Sprintf("http://%s/graphql/%s", GraphQLAddr, GraphName)
	client := graphql.NewClient(url)

	// make a request
	req := graphql.NewRequest(`{
     __type(name:"Character") {
        fields {
           name
           description
        }
     }
  }`)

	ctx := context.Background()
	respData := map[string]interface{}{}
	if err := client.Run(ctx, req, &respData); err != nil {
		t.Error(err)
	}
	fmt.Printf("Response: %#v\n", respData)

}
