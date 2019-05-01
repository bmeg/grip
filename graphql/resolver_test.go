package graphql

import (
	"testing"

	"github.com/graphql-go/graphql"
)

func TestResolver(t *testing.T) {
	// Schema
	personObject := graphql.NewObject(graphql.ObjectConfig{Name: "Person",
		Fields: graphql.Fields{
			"name": &graphql.Field{Type: graphql.String},
			"age":  &graphql.Field{Type: graphql.Int},
		},
	})

	personObject.AddFieldConfig(
		"edge_friend",
		&graphql.Field{
			Name: "Friend",
			Type: personObject,
		},
	)

	fields := graphql.Fields{
		"person": &graphql.Field{
			Type: personObject,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				_, err := resolveGraphql("person", p)
				if err != nil {
					return nil, err
				}
				return map[string]interface{}{"name": "bob", "edge_friend": map[string]interface{}{"name": "Joe",
					"edge_friend": map[string]interface{}{"name": "Sam"},
				}}, nil
			},
		},
	}

	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		t.Fatalf("failed to create new schema, error: %v", err)
	}

	// Query
	query := `
		{
      person {
        name
        age
        edge_friend { 
          edge_friend {
            name
            age
          }
        }
      }
		}
	`
	params := graphql.Params{Schema: schema, RequestString: query}
	r := graphql.Do(params)
	if len(r.Errors) > 0 {
		t.Fatalf("failed to execute graphql operation, errors: %+v", r.Errors)
	}
}
