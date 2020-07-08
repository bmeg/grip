package gripper

import (
	"context"

	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/setcmp"
	"github.com/bmeg/grip/protoutil"
)

func TabularOptimizer(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	labels, steps := inspect.FindVertexHasLabelStart(pipe)
	if len(labels) > 0 {
		out := []*gripql.GraphStatement{}
		i := gripql.GraphStatement_EngineCustom{Desc: "Tabular Label Scan", Custom: tabularHasLabelStep{labels}}
		out = append(out, &gripql.GraphStatement{Statement: &i})
		out = append(out, steps...)
		return out
	}
	return pipe
}

type tabularHasLabelStep struct {
	labels []string
}

type tabularHasLabelProc struct {
	labels []string
	graph  *TabularGraph
}

func (t *tabularHasLabelProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for i := range in {
			for _, table := range t.graph.vertices {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if setcmp.ContainsString(t.labels, table.config.Label) {
					for row := range t.graph.client.GetRows(ctx, table.config.Source, table.config.Collection) {
						out <- i.AddCurrent(&gdbi.DataElement{
							ID:    table.prefix + row.Id,
							Label: table.config.Label,
							Data:  protoutil.AsMap(row.Data),
						})
					}
				}
			}
		}
	}()
	return ctx
}

func (t tabularHasLabelStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*TabularGraph)
	return &tabularHasLabelProc{t.labels, graph}, nil
}

func (t tabularHasLabelStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}
