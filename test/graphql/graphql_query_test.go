package graphql

import (
	"fmt"
	//"io/ioutil"
	//"net/http"
	//"bytes"
	"context"
	"testing"

	//"encoding/json"

	"github.com/machinebox/graphql"
	"github.com/oliveagle/jsonpath"
)

var tests = [][]string{
	{
		`
      query {
        Character(id:"1"){
          id
          name
          starships_to_Starship {
            name
          }
        }
      }
    `,
		`{}`,
	},
}

func TestQuerySet(t *testing.T) {
	url := fmt.Sprintf("http://%s/graphql/%s", GraphQLAddr, GraphName)
	client := graphql.NewClient(url)

	for _, pair := range tests {
		req := graphql.NewRequest(pair[0])

		ctx := context.Background()
		respData := map[string]interface{}{}
		if err := client.Run(ctx, req, &respData); err != nil {
			t.Error(err)
		}
		//out, err := json.Marshal(respData)
		//if err != nil {
		//  t.Error(err)
		//}
		//fmt.Printf("out: %s\n", out)
	}
}

func TestCharacterQuery(t *testing.T) {
	url := fmt.Sprintf("http://%s/graphql/%s", GraphQLAddr, GraphName)
	client := graphql.NewClient(url)

	// make a request
	req := graphql.NewRequest(`
    query {
      Character(id:"Character:1"){
        id
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

	out, err := jsonpath.JsonPathLookup(respData, "$.Character[0].id")
	if err != nil {
		t.Error(err)
	}
	if idStr, ok := out.(string); ok {
		if idStr != "Character:1" {
			t.Errorf("id mismatch: %s != %s", "Character:1", idStr)
		}
	} else {
		t.Errorf("ID not string")
	}
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
	//fmt.Printf("Response: %#v\n", respData)
}
