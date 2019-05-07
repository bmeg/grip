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
        _to_person {
          name
          _to_person {
            name
            age
          }
        }
      }
		}
	`

	// Expected GripQL query
	expected := gripql.NewQuery().V().HasLabel("person").Fields("name", "age").As("person").
		Out("friend").HasLabel("person").Fields("name").As("person__to_person").
		Out("friend").HasLabel("person").Fields("name", "age").As("person__to_person__to_person").
		Select("person", "person__to_person", "person__to_person__to_person").
		Render(map[string]interface{}{
			"person": map[string]interface{}{
				"name": "$person.name",
				"age":  "$person.age",
				"_to_person": map[string]interface{}{
					"name": "$person__to_person.name",
					"_to_person": map[string]interface{}{
						"name": "$person__to_person__to_person.name",
						"age":  "$person__to_person__to_person.age",
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
		"_to_person",
		&graphql.Field{
			Type: personObject,
		},
	)

	fields := graphql.Fields{
		"person": &graphql.Field{
			Type: personObject,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				tr := &gqlTranslator{edgeMap: map[string]string{"_to_person": "friend"}}
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
