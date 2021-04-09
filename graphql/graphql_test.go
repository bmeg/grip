package graphql

import (
	"testing"

	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestWellDefined(t *testing.T) {
	vdata := map[string]interface{}{
		"Field1": "STRING",
		"Field2": "NUMERIC",
		"Field3": "BOOL",
	}
	d, _ := structpb.NewStruct(vdata)
	schema := &gripql.Graph{
		Vertices: []*gripql.Vertex{
			{
				Gid:   "STRING",
				Label: "TestVertexLabel1",
				Data:  d,
			},
			{
				Gid:   "STRING",
				Label: "TestVertexLabel2",
				Data:  d,
			},
		},
		Edges: []*gripql.Edge{
			{
				Gid:   "STRING",
				Label: "TestEdgeLabel1",
				From:  "TestVertexLabel1",
				To:    "TestVertexLabel2",
			},
		},
	}

	_, err := buildGraphQLSchema(schema, gripql.Client{}, "test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUnkownType(t *testing.T) {
	vdata := map[string]interface{}{
		"Field1": "UNKNOWN",
		"Field2": "NUMERIC",
	}
	d, _ := structpb.NewStruct(vdata)
	schema := &gripql.Graph{
		Vertices: []*gripql.Vertex{
			{
				Gid:   "STRING",
				Label: "TestVertexLabel1",
				Data:  d,
			},
		},
	}

	_, err := buildGraphQLSchema(schema, gripql.Client{}, "test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNilData(t *testing.T) {
	// TestVertexLabel1 will be omitted from the resulting GQL schema since it has no properties
	vdata := map[string]interface{}{
		"Field1": "STRING",
		"Field2": "NUMERIC",
		"Field3": "BOOL",
	}
	d, _ := structpb.NewStruct(vdata)
	schema := &gripql.Graph{
		Vertices: []*gripql.Vertex{
			{
				Gid:   "STRING",
				Label: "TestVertexLabel1",
			},
			{
				Gid:   "STRING",
				Label: "TestVertexLabel2",
				Data:  d,
			},
		},
	}

	_, err := buildGraphQLSchema(schema, gripql.Client{}, "test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestComplex(t *testing.T) {
	vdata := map[string]interface{}{
		"Field1": "STRING",
		"Field2": "NUMERIC",
		"Field3": "BOOL",
		"Field4": []string{"STRING"},
		"Field5": [][]string{{"STRING"}},
		"Field6": map[string]interface{}{
			"N1": "STRING",
			"N2": "NUMERIC",
			"N3": "BOOL",
			"N4": []string{"STRING"},
			"N5": [][]string{{"STRING"}},
			"N6": []map[string]interface{}{
				{
					"NN1": "STRING",
				},
			},
		},
		"Field7": "UNKNOWN",
	}
	d, _ := structpb.NewStruct(vdata)
	schema := &gripql.Graph{
		Vertices: []*gripql.Vertex{
			{
				Gid:   "STRING",
				Label: "TestVertexLabel1",
				Data:  d,
			},
		},
	}

	_, err := buildGraphQLSchema(schema, gripql.Client{}, "test")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
