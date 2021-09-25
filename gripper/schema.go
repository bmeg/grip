package gripper

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	gripSchema "github.com/bmeg/grip/gripql/schema"
	"github.com/bmeg/grip/log"
	"google.golang.org/protobuf/types/known/structpb"
)

// BuildSchema returns the schema of a specific graph in the database
func (g *TabularGDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {
	var vSchema []*gripql.Vertex
	var eSchema []*gripql.Edge
	var err error

	log.WithFields(log.Fields{"graph": graph}).Debug("Starting Gripper GetSchema call")

	vSchema, eSchema, err = g.sampleSchema(ctx, graph, sampleN, random)
	if err != nil {
		return nil, fmt.Errorf("getting vertex schema: %v", err)
	}

	schema := &gripql.Graph{Graph: graph, Vertices: vSchema, Edges: eSchema}
	log.WithFields(log.Fields{"graph": graph}).Debug("Finished GetSchema call")
	return schema, nil
}

type fromtokey struct {
	from, to, label string
}

type fromto map[fromtokey]interface{}

func (g *TabularGDB) sampleSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, []*gripql.Edge, error) {

	vOutput := []*gripql.Vertex{}
	eOutput := []*gripql.Edge{}
	fromToPairs := make(fromto)

	gi, ok := g.graphs[graph]
	if !ok {
		return vOutput, eOutput, fmt.Errorf("Graph not found")
	}

	vLabelSchemas := map[string]map[string]interface{}{}
	for _, tableName := range gi.vertexSourceOrder {
		table := gi.vertices[tableName]
		tLabel := table.config.Label
		schema := map[string]interface{}{}
		if x, ok := vLabelSchemas[tLabel]; ok {
			schema = x
		}

		vChan := make(chan *gdbi.DataElement, 10)
		go func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer close(vChan)
			count := uint32(0)
			for row := range gi.client.GetRows(cancelCtx, table.config.Data.Source, table.config.Data.Collection) {
				v := &gdbi.DataElement{
					ID:     table.prefix + row.Id,
					Label:  table.config.Label,
					Data:   row.Data.AsMap(),
					Loaded: true,
				}
				vChan <- v
				count++
				if count >= n {
					cancel()
				}
			}
		}()

		for v := range vChan {
			data := v.Data
			ds := gripql.GetDataFieldTypes(data)
			gripSchema.MergeMaps(schema, ds)
		}
		vLabelSchemas[tLabel] = schema
	}

	for _, source := range gi.edgeSourceOrder {
		edgeList := gi.outEdges[source]
		for _, table := range edgeList {
			eChan := make(chan *gdbi.DataElement, 10)
			go func() {
				cancelCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				defer close(eChan)
				count := uint32(0)
				for row := range gi.client.GetRows(cancelCtx, table.config.Data.Source, table.config.Data.Collection) {
					data := row.Data.AsMap()
					if dstStr, err := getFieldString(data, table.config.Data.ToField); err == nil {
						if dstStr != "" {
							if srcStr, err := getFieldString(data, table.config.Data.FromField); err == nil {
								if srcStr != "" {
									e := &gdbi.Edge{
										ID:     table.GenID(srcStr, dstStr),
										To:     gi.vertices[ table.config.To ].config.Label,
										From:   gi.vertices[ table.config.From ].config.Label,
										Label:  table.config.Label,
										Data:   data,
										Loaded: true,
									}
									eChan <- e
									count++
									if count >= n {
										cancel()
									}
								}
							}
						}
					}
				}
			}()

			for e := range eChan {
				k := fromtokey{from: e.From, to: e.To, label: e.Label}
				ds := gripql.GetDataFieldTypes(e.Data)
				if p, ok := fromToPairs[k]; ok {
					fromToPairs[k] = gripSchema.MergeMaps(p, ds)
				} else {
					fromToPairs[k] = ds
				}
			}
		}
	}

	for label, schema := range vLabelSchemas {
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
