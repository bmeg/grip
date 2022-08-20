package schema

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"google.golang.org/protobuf/types/known/structpb"
)

func SchemaScan(ctx context.Context, graphName string, graph gdbi.GraphInterface, sampleN uint32, random bool) (*gripql.Graph, error) {
	vSchema := []*gripql.Vertex{}
	eSchema := []*gripql.Edge{}

	comp := graph.Compiler()

	vLabels, _ := graph.ListVertexLabels()
	for _, label := range vLabels {
		labelList, _ := structpb.NewList([]any{label})
		p, _ := comp.Compile([]*gripql.GraphStatement{
			{Statement: &gripql.GraphStatement_V{}},
			{Statement: &gripql.GraphStatement_HasLabel{HasLabel: labelList}},
			{Statement: &gripql.GraphStatement_Limit{Limit: sampleN}},
		}, nil)
		lSchema := map[string]any{}
		vids := []any{}
		for res := range pipeline.Run(ctx, p, "./") {
			v := res.GetVertex()
			vids = append(vids, v.Gid)
			util.MergeMaps(lSchema, v.GetData().AsMap())
		}
		sSchema, _ := structpb.NewStruct(lSchema)
		vEnt := &gripql.Vertex{Gid: label, Label: label, Data: sSchema}
		vSchema = append(vSchema, vEnt)

		fromToPairs := make(fromto)

		vidList, _ := structpb.NewList(vids)
		render, _ := structpb.NewValue([]any{"$e._label", "$e._data", "$d._label"})
		p2, _ := comp.Compile([]*gripql.GraphStatement{
			{Statement: &gripql.GraphStatement_V{V: vidList}},
			{Statement: &gripql.GraphStatement_As{As: "s"}},
			{Statement: &gripql.GraphStatement_OutE{}},
			{Statement: &gripql.GraphStatement_As{As: "e"}},
			{Statement: &gripql.GraphStatement_Out{}},
			{Statement: &gripql.GraphStatement_As{As: "d"}},
			{Statement: &gripql.GraphStatement_Render{Render: render}},
		}, nil)

		for res := range pipeline.Run(ctx, p2, "./") {
			r := res.GetRender().GetListValue()
			eLabel := r.Values[0].GetStringValue()
			eData := r.Values[1].GetStructValue().AsMap()
			dLabel := r.Values[2].GetStringValue()
			k := fromtokey{from: label, to: dLabel, label: eLabel}
			if p, ok := fromToPairs[k]; ok {
				fromToPairs[k] = util.MergeMaps(p, eData)
			} else {
				fromToPairs[k] = eData
			}
		}

		for k, v := range fromToPairs {
			sV, _ := structpb.NewStruct(v.(map[string]interface{}))
			e := &gripql.Edge{
				Gid:   fmt.Sprintf("(%s)--%s->(%s)", k.from, k.label, k.to),
				Label: k.label,
				From:  k.from,
				To:    k.to,
				Data:  sV,
			}
			eSchema = append(eSchema, e)
		}

	}

	schema := &gripql.Graph{Graph: graphName, Vertices: vSchema, Edges: eSchema}
	return schema, nil
}

type fromtokey struct {
	from, to, label string
}

type fromto map[fromtokey]interface{}
