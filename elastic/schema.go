package elastic

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/olivere/elastic.v5"
)

// BuildSchema returns the schema of a specific graph in the database
func (es *GraphDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {

	gr, err := es.Graph(graph)
	if err != nil {
		return nil, err
	}

	vertexIndex := fmt.Sprintf("%s_%s_vertex", es.database, graph)
	//edgeIndex := fmt.Sprintf("%s_%s_edge", es.database, graph)

	vSchema := []*gripql.Vertex{}
	eSchema := []*gripql.Edge{}

	vLabels, err := gr.ListVertexLabels()

	for _, label := range vLabels {
		q := es.client.Search().Index(vertexIndex).Query(elastic.NewBoolQuery().Must(elastic.NewTermsQuery("label", label))).Size(int(sampleN))
		for hit := range paginateQuery(ctx, q, 1000) {
			vert := &gripql.Vertex{}
			err := protojson.Unmarshal(*hit.Source, vert)
			if err == nil {
				fmt.Printf("Found %#v\n", vert)
			}
		}
		sSchema, _ := structpb.NewStruct(map[string]any{})
		vEnt := &gripql.Vertex{Gid: label, Label: label, Data: sSchema}
		vSchema = append(vSchema, vEnt)
	}

	schema := &gripql.Graph{Graph: graph, Vertices: vSchema, Edges: eSchema}
	return schema, nil
}
