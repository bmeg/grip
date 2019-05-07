package server

import (
	"reflect"
	"testing"

	"github.com/bmeg/grip/gripql"
	"github.com/graphql-go/graphql"
)

func TestGraphQLTranslator(t *testing.T) {
	// GraphQL query
	query := `
		{
      person {
        name
        age
        friend {
          name
          friend {
            name
            age
          }
        }
      }
		}
	`

	// Expected GripQL query
	expected := gripql.NewQuery().V().HasLabel("person").Fields("name", "age").As("person").
		Out("friend").Fields("name").As("person__friend").
		Out("friend").Fields("name", "age").As("person__friend__friend").
		Select("person", "person__friend", "person__friend__friend").
		Render(map[string]interface{}{
			"person": map[string]interface{}{
				"name": "$person.name",
				"age":  "$person.age",
				"friend": map[string]interface{}{
					"name": "$person__friend.name",
					"friend": map[string]interface{}{
						"name": "$person__friend__friend.name",
						"age":  "$person__friend__friend.age",
					},
				},
			},
		})

	// Setup GraphQL schema
	personObject := graphql.NewObject(graphql.ObjectConfig{Name: "Person",
		Fields: graphql.Fields{
			"name": &graphql.Field{Type: graphql.String},
			"age":  &graphql.Field{Type: graphql.Int},
		},
	})

	personObject.AddFieldConfig(
		"friend",
		&graphql.Field{
			Type: personObject,
		},
	)

	fields := graphql.Fields{
		"person": &graphql.Field{
			Type: personObject,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				tr := &gqlTranslator{edgeLabels: []string{"friend"}}
				actual, err := tr.translate("person", p)
				if err != nil {
					t.Fatalf("failed to translate query: %v", err)
					return nil, err
				}
				if !reflect.DeepEqual(expected.Statements, actual.Statements) {
					t.Logf("expected: %+v", expected.JSON())
					t.Logf("actual:   %+v", actual.JSON())
					t.Fatal("unexpected query returned by GraphQL translator")
				}
				return nil, nil
			},
		},
	}

	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		t.Fatalf("failed to create new schema, error: %v", err)
	}

	params := graphql.Params{Schema: schema, RequestString: query}
	graphql.Do(params)
}

func TestGraphQLResolver(t *testing.T) {
	graph := "example-graph"
	ts, err := SetupTestServer(graph)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	gqlSchema, err := buildGraphQLSchema(ts.DB, ts.Config.WorkDir, ts.Graph, ts.Schema)
	if err != nil {
		t.Fatal(err)
	}

	query := `
		{
      Human {
        name
        mass
        friend {
          name
        }
      }
		}
	`

	resp := graphql.Do(graphql.Params{Schema: *gqlSchema, RequestString: query})
	if len(resp.Errors) > 0 {
		t.Fatalf("failed to execute graphql operation, errors: %+v", resp.Errors)
	}
}
