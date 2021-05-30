package kvgraph

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	gripSchema "github.com/bmeg/grip/gripql/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

// BuildSchema returns the schema of a specific graph in the database
func (ma *KVGraph) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {
	var vSchema []*gripql.Vertex
	var eSchema []*gripql.Edge
	var err error

	log.WithFields(log.Fields{"graph": graph}).Debug("Starting KV GetSchema call")

	vSchema, eSchema, err = ma.sampleSchema(ctx, graph, sampleN, random)
	if err != nil {
		return nil, fmt.Errorf("getting vertex schema: %v", err)
	}

	schema := &gripql.Graph{Graph: graph, Vertices: vSchema, Edges: eSchema}
	log.WithFields(log.Fields{"graph": graph}).Debug("Finished GetSchema call")
	return schema, nil
}

func (ma *KVGraph) sampleSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, []*gripql.Edge, error) {

	labelField := fmt.Sprintf("%s.v.label", graph)
	labels := []string{}
	for i := range ma.idx.FieldTerms(labelField) {
		labels = append(labels, i.(string))
	}

	vOutput := []*gripql.Vertex{}
	eOutput := []*gripql.Edge{}
	fromToPairs := make(fromto)

	gi, _ := ma.Graph(graph)
	for _, label := range labels {
		schema := map[string]interface{}{}
		for i := range ma.idx.GetTermMatch(context.Background(), labelField, label, int(n)) {
			v := gi.GetVertex(i, true)
			data := v.Data
			ds := gripql.GetDataFieldTypes(data)
			gripSchema.MergeMaps(schema, ds)

			reqChan := make(chan gdbi.ElementLookup, 1)
			reqChan <- gdbi.ElementLookup{ID: i}
			close(reqChan)
			for e := range gi.GetOutEdgeChannel(ctx, reqChan, true, []string{}) {
				o := gi.GetVertex(e.Edge.To, false)
				k := fromtokey{from: v.Label, to: o.Label, label: e.Edge.Label}
				ds := gripql.GetDataFieldTypes(e.Edge.Data)
				if p, ok := fromToPairs[k]; ok {
					fromToPairs[k] = gripSchema.MergeMaps(p, ds)
				} else {
					fromToPairs[k] = ds
				}
			}
		}
		sSchema, _ := structpb.NewStruct(schema)
		vSchema := &gripql.Vertex{Gid: label, Label: label, Data: sSchema}
		vOutput = append(vOutput, vSchema)
	}
	for k, v := range fromToPairs {
		sV, _ := structpb.NewStruct(v.(map[string]interface{}))
		eSchema := &gripql.Edge{
			Gid:   fmt.Sprintf("(%s)--%s->(%s)", k.from, k.label, k.to),
			Label: k.label,
			From:  k.from,
			To:    k.to,
			Data:  sV,
		}
		eOutput = append(eOutput, eSchema)
	}
	return vOutput, eOutput, nil
}

type fromtokey struct {
	from, to, label string
}

type fromto map[fromtokey]interface{}
