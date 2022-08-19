package schema

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/types/known/structpb"
)

func SchemaScan(ctx context.Context, graphName string, graph gdbi.GraphInterface, sampleN uint32, random bool) (*gripql.Graph, error) {
	vSchema := []*gripql.Vertex{}
	eSchema := []*gripql.Edge{}

	vLabels, _ := graph.ListVertexLabels()
	for _, label := range vLabels {
		/*
			err := protojson.Unmarshal(*hit.Source, vert)
			if err == nil {
				fmt.Printf("Found %#v\n", vert)
			}
		*/
		sSchema, _ := structpb.NewStruct(map[string]any{})
		vEnt := &gripql.Vertex{Gid: label, Label: label, Data: sSchema}
		vSchema = append(vSchema, vEnt)
	}

	schema := &gripql.Graph{Graph: graphName, Vertices: vSchema, Edges: eSchema}
	return schema, nil
}
