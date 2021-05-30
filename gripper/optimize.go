package gripper

import (
	"context"

	"github.com/bmeg/grip/log"

	"github.com/bmeg/grip/engine/inspect"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/setcmp"
	"github.com/bmeg/jsonpath"
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
	labels, steps = inspect.FindEdgeHasLabelStart(pipe)
	if len(labels) > 0 {
		out := []*gripql.GraphStatement{}
		i := gripql.GraphStatement_EngineCustom{Desc: "Tabular Edge Label Scan", Custom: tabularEdgeHasLabelStep{labels}}
		out = append(out, &gripql.GraphStatement{Statement: &i})
		out = append(out, steps...)
		return out
	}
	return pipe
}

// vertex hasLabel optimization

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
			for _, tableName := range t.graph.vertexSourceOrder {
				table := t.graph.vertices[tableName]
				select {
				case <-ctx.Done():
					return
				default:
				}
				if setcmp.ContainsString(t.labels, table.config.Label) {
					for row := range t.graph.client.GetRows(ctx, table.config.Source, table.config.Collection) {
						out <- i.AddCurrent(&gdbi.DataElement{
							ID:     table.prefix + row.Id,
							Label:  table.config.Label,
							Data:   row.Data.AsMap(),
							Loaded: true,
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

// Edge hasLabel optimization

type tabularEdgeHasLabelStep struct {
	labels []string
}

type tabularEdgeHasLabelProc struct {
	labels []string
	graph  *TabularGraph
}

func (t *tabularEdgeHasLabelProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for i := range in {
			for _, source := range t.graph.edgeSourceOrder {
				edgeList := t.graph.outEdges[source]
				for _, edge := range edgeList {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if setcmp.ContainsString(t.labels, edge.config.Label) {
						if edge.config.EdgeTable != nil {
							//srcID := strings.TrimPrefix(src, edge.fromVertex.prefix)
							//dstID := strings.TrimPrefix(dst, edge.toVertex.prefix)
							for row := range t.graph.client.GetRows(ctx, edge.config.EdgeTable.Source, edge.config.EdgeTable.Collection) {
								data := row.Data.AsMap()
								if rowSrc, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.FromField); err == nil {
									if rowSrcStr, ok := rowSrc.(string); ok {
										if rowDst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
											if rowDstStr, ok := rowDst.(string); ok {
												o := gdbi.Edge{
													ID:     edge.GenID(rowSrcStr, rowDstStr), //edge.prefix + row.Id,
													To:     edge.config.ToVertex + rowDstStr,
													From:   edge.config.FromVertex + rowSrcStr,
													Label:  edge.config.Label,
													Data:   row.Data.AsMap(),
													Loaded: true,
												}
												out <- i.AddCurrent(&o)
											}
										}
									}
								}
							}
						} else if edge.config.FieldToField != nil {
							for srcRow := range t.graph.client.GetRows(ctx, edge.fromVertex.config.Source, edge.fromVertex.config.Collection) {
								srcData := srcRow.Data.AsMap()
								if srcField, err := jsonpath.JsonPathLookup(srcData, edge.config.FieldToField.FromField); err == nil {
									if fValue, ok := srcField.(string); ok {
										if fValue != "" {
											dstRes, err := t.graph.client.GetRowsByField(ctx,
												edge.toVertex.config.Source,
												edge.toVertex.config.Collection,
												edge.config.FieldToField.ToField, fValue)
											if err == nil {
												for dstRow := range dstRes {
													o := gdbi.Edge{
														ID:     edge.GenID(srcRow.Id, dstRow.Id),
														From:   edge.fromVertex.prefix + srcRow.Id,
														To:     edge.toVertex.prefix + dstRow.Id,
														Label:  edge.config.Label,
														Loaded: true,
													}
													out <- i.AddCurrent(&o)
												}
											} else {
												if ctx.Err() != context.Canceled {
													log.Errorf("Error doing FieldToField search: %s", err)
												}
											}
										}
									}
								}
							}
						} else {
							log.Errorf("GetEdge.FieldToID not yet implemented")
						}
					}
				}
			}
		}
	}()
	return ctx
}

func (t tabularEdgeHasLabelStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*TabularGraph)
	return &tabularEdgeHasLabelProc{t.labels, graph}, nil
}

func (t tabularEdgeHasLabelStep) GetType() gdbi.DataType {
	return gdbi.EdgeData
}
