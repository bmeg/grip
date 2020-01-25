package grids

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util"
)

// BuildSchema returns the schema of a specific graph in the database
func (ma *GDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {
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

func (ma *GDB) sampleSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, []*gripql.Edge, error) {

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
			data := protoutil.AsMap(v.Data)
			ds := gripql.GetDataFieldTypes(data)
			util.MergeMaps(schema, ds)

			reqChan := make(chan gdbi.ElementLookup, 1)
			reqChan <- gdbi.ElementLookup{ID: i}
			close(reqChan)
			for e := range gi.GetOutEdgeChannel(reqChan, true, []string{}) {
				o := gi.GetVertex(e.Edge.To, false)
				k := fromtokey{from: v.Label, to: o.Label, label: e.Edge.Label}
				ds := gripql.GetDataFieldTypes(protoutil.AsMap(e.Edge.Data))
				if p, ok := fromToPairs[k]; ok {
					fromToPairs[k] = util.MergeMaps(p, ds)
				} else {
					fromToPairs[k] = ds
				}
			}
		}
		vSchema := &gripql.Vertex{Gid: label, Label: label, Data: protoutil.AsStruct(schema)}
		vOutput = append(vOutput, vSchema)
	}
	for k, v := range fromToPairs {
		eSchema := &gripql.Edge{
			Gid:   fmt.Sprintf("(%s)--%s->(%s)", k.from, k.label, k.to),
			Label: k.label,
			From:  k.from,
			To:    k.to,
			Data:  protoutil.AsStruct(v.(map[string]interface{})),
		}
		eOutput = append(eOutput, eSchema)
	}
	return vOutput, eOutput, nil
}

type fromtokey struct {
	from, to, label string
}

type fromto map[fromtokey]interface{}
