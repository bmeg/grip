package gripper

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util/setcmp"
	"github.com/oliveagle/jsonpath"
)

type VertexSource struct {
	prefix string
	config *VertexConfig
}

type EdgeSource struct {
	prefix     string
	toVertex   *VertexSource
	fromVertex *VertexSource
	config     *EdgeConfig
}

type TabularGraph struct {
	client   *DigClient
	vertices map[string]*VertexSource
	outEdges map[string][]*EdgeSource //outbound edges by vertex prefix
	inEdges  map[string][]*EdgeSource //inbound edges by vertex prefix
}

func NewTabularGraph(conf GraphConfig) (*TabularGraph, error) {
	out := TabularGraph{}

	out.vertices = map[string]*VertexSource{}
	out.outEdges = map[string][]*EdgeSource{}
	out.inEdges = map[string][]*EdgeSource{}

	log.Info("Loading Graph Config")

	out.client = NewDigClient(conf.Sources)

	//Check if vertex mapping match to sources
	for _, v := range conf.Vertices {
		_, err := out.client.GetCollectionInfo(context.Background(), v.Source, v.Collection)
		if err != nil {
			return nil, fmt.Errorf("Unable to get collection information %s : %s", v.Source, v.Collection)
		}
	}

	//Check if edges match sources
	for _, e := range conf.Edges {
		if _, ok := conf.Vertices[e.ToVertex]; !ok {
			return nil, fmt.Errorf("Edge ToVertex not found")
		}
		if _, ok := conf.Vertices[e.FromVertex]; !ok {
			return nil, fmt.Errorf("Edge ToVertex not found")
		}
		if e.EdgeTable != nil {
			_, err := out.client.GetCollectionInfo(context.Background(),
				e.EdgeTable.Source, e.EdgeTable.Collection)
			if err != nil {
				return nil, fmt.Errorf("Unable to get collection information %s : %s",
					e.EdgeTable.Source, e.EdgeTable.Collection)
			}
			if !strings.HasPrefix(e.EdgeTable.ToField, "$.") {
				return nil, fmt.Errorf("Edge 'To' Field does not start with JSONPath prefix ($.) = %s", e.EdgeTable.ToField)
			}
			if !strings.HasPrefix(e.EdgeTable.FromField, "$.") {
				return nil, fmt.Errorf("Edge 'From' Field does not start with JSONPath prefix ($.) = %s", e.EdgeTable.FromField)
			}
		} else if e.FieldToID != nil {
			//return nil, fmt.Errorf("Not supported yet")
		} else if e.FieldToField != nil {
			vTo := conf.Vertices[e.ToVertex]
			vFrom := conf.Vertices[e.FromVertex]

			if !strings.HasPrefix(e.FieldToField.ToField, "$.") {
				return nil, fmt.Errorf("Edge 'To' Field does not start with JSONPath prefix ($.) = %s", e.FieldToField.ToField)
			}
			if !strings.HasPrefix(e.FieldToField.FromField, "$.") {
				return nil, fmt.Errorf("Edge 'From' Field does not start with JSONPath prefix ($.) = %s", e.FieldToField.FromField)
			}

			if iTo, err := out.client.GetCollectionInfo(context.Background(),
				vTo.Source, vTo.Collection); err == nil {
				if !setcmp.ContainsString(iTo.SearchFields, e.FieldToField.ToField) {
					return nil, fmt.Errorf("Edge 'To' Field not indexed: %s %s",
						vTo.Collection,
						e.FieldToField.ToField)
				}
			}

			if iFrom, err := out.client.GetCollectionInfo(context.Background(),
				vFrom.Source, vFrom.Collection); err == nil {
				if !setcmp.ContainsString(iFrom.SearchFields, e.FieldToField.FromField) {
					return nil, fmt.Errorf("Edge 'From' Field not indexed: %s %s",
						vFrom.Collection,
						e.FieldToField.FromField)
				}
			}
		} else {
			return nil, fmt.Errorf("Edge Doesn't declare lookup method")
		}
	}

	//map the table drivers back onto the vertices that will use them
	for vPrefix, v := range conf.Vertices {
		vConf := v
		log.Infof("Adding vertex prefix: %s label: %s", vPrefix, v.Label)
		out.vertices[vPrefix] = &VertexSource{prefix: vPrefix, config: &vConf}
	}

	for ePrefix, e := range conf.Edges {
		oConf := EdgeConfig{}
		iConf := EdgeConfig{}

		if e.EdgeTable != nil {
			oConf = e
			//copy the edge config, but flip the field requests for the incoming edges
			iConf.FromVertex = oConf.ToVertex
			iConf.ToVertex = oConf.FromVertex
			iConf.Label = oConf.Label
			iConf.EdgeTable = &EdgeTableConfig{}
			iConf.EdgeTable.Source = oConf.EdgeTable.Source
			iConf.EdgeTable.Collection = oConf.EdgeTable.Collection
			iConf.EdgeTable.FromField = oConf.EdgeTable.ToField
			iConf.EdgeTable.ToField = oConf.EdgeTable.FromField
		} else if e.FieldToID != nil {
			//do something here
		} else if e.FieldToField != nil {
			oConf = e
			//copy the edge config, but flip the field requests for the incoming edges
			iConf.FromVertex = oConf.ToVertex
			iConf.ToVertex = oConf.FromVertex
			iConf.Label = oConf.Label
			iConf.FieldToField = &FieldToFieldConfig{}
			iConf.FieldToField.FromField = oConf.FieldToField.ToField
			iConf.FieldToField.ToField = oConf.FieldToField.FromField
		}

		out.inEdges[e.ToVertex] = append(out.inEdges[e.ToVertex], &EdgeSource{
			prefix:     ePrefix,
			config:     &iConf,
			fromVertex: out.vertices[e.ToVertex],
			toVertex:   out.vertices[e.FromVertex],
		})
		out.outEdges[e.FromVertex] = append(out.outEdges[e.FromVertex], &EdgeSource{
			prefix:     ePrefix,
			config:     &oConf,
			fromVertex: out.vertices[e.FromVertex],
			toVertex:   out.vertices[e.ToVertex],
		})

	}

	return &out, nil
}

func (t *TabularGraph) Close() error {
	return nil
}

func (t *TabularGraph) AddVertex(vertex []*gripql.Vertex) error {
	return fmt.Errorf("DigGraph is ReadOnly")
}

func (t *TabularGraph) AddEdge(edge []*gripql.Edge) error {
	return fmt.Errorf("DigGraph is ReadOnly")
}

func (t *TabularGraph) BulkAdd(stream <-chan *gripql.GraphElement) error {
	return fmt.Errorf("DigGraph is ReadOnly")
}

func (t *TabularGraph) Compiler() gdbi.Compiler {
	return core.NewCompiler(t, TabularOptimizer)
}

func (t *TabularGraph) GetTimestamp() string {
	return "NA"
}

func (t *TabularGraph) GetVertex(key string, load bool) *gripql.Vertex {
	for _, v := range t.vertices {
		if strings.HasPrefix(key, v.prefix) {
			id := key[len(v.prefix):]
			c := make(chan *RowRequest, 1)
			c <- &RowRequest{Id: id, RequestID: 0}
			close(c)
			if rowChan, err := t.client.GetRowsByID(context.Background(), v.config.Source, v.config.Collection, c); err == nil {
				var row *Row
				for i := range rowChan {
					row = i
				}
				if row != nil {
					o := gripql.Vertex{Gid: v.prefix + row.Id, Label: v.config.Label, Data: row.Data}
					return &o
				}
			} else {
				log.Errorf("Row not read: %s", err)
			}
		}
	}
	return nil
}

func (t *TabularGraph) GetEdge(key string, load bool) *gripql.Edge {
	return nil
}

func (t *TabularGraph) DelVertex(key string) error {
	return fmt.Errorf("DelVertex not implemented")

}

func (t *TabularGraph) DelEdge(key string) error {
	return fmt.Errorf("DelEdge not implemented")
}

func (t *TabularGraph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		for _, v := range t.vertices {
			if v.config.Label == label {
				for n := range t.client.GetIDs(ctx, v.config.Source, v.config.Collection) {
					out <- v.prefix + n
				}
			}
		}
	}()
	return out
}

func (t *TabularGraph) ListVertexLabels() ([]string, error) {
	out := []string{}
	for _, i := range t.vertices {
		out = append(out, i.config.Label)
	}
	return out, nil
}

func (t *TabularGraph) ListEdgeLabels() ([]string, error) {
	out := []string{}
	for _, i := range t.outEdges {
		for _, e := range i {
			out = append(out, e.config.Label)
		}
	}
	return out, nil
}

func (t *TabularGraph) AddVertexIndex(label string, field string) error {
	return fmt.Errorf("DelEdge not implemented")
}

func (t *TabularGraph) DeleteVertexIndex(label string, field string) error {
	return fmt.Errorf("DelEdge not implemented")
}

func (t *TabularGraph) GetVertexIndexList() <-chan *gripql.IndexID {
	out := make(chan *gripql.IndexID)
	close(out)
	return out
}

func (t *TabularGraph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
	out := make(chan *gripql.Vertex, 100)
	go func() {
		for _, c := range t.vertices {
			//log.Printf("Getting vertices from table: %s", c.config.Label)
			for row := range t.client.GetRows(ctx, c.config.Source, c.config.Collection) {
				v := gripql.Vertex{Gid: c.prefix + row.Id, Label: c.config.Label, Data: row.Data}
				out <- &v
			}
		}
		defer close(out)
	}()
	return out
}

func (t *TabularGraph) GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge {
	log.Errorf("Calling GetEdgeList. Not supported in current Dig implmentation")
	out := make(chan *gripql.Edge, 100)
	go func() {
		defer close(out)
	}()
	return out
}

func (t *TabularGraph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 10)
	go func() {
		defer close(out)

		inMap := map[string]chan *RowRequest{}
		//outMap := map[string]chan *Row{}
		reqMap := map[uint64]gdbi.ElementLookup{}
		var reqCount uint64
		reqSync := &sync.Mutex{} //sync access to reqMap, other maps are only access by outer thread
		wg := &sync.WaitGroup{}

		for r := range req {
			for _, v := range t.vertices {
				if strings.HasPrefix(r.ID, v.prefix) {
					id := r.ID[len(v.prefix):len(r.ID)]

					var curIn chan *RowRequest
					if x, ok := inMap[v.prefix]; ok {
						curIn = x
					} else {
						curIn = make(chan *RowRequest, 10)
						inMap[v.prefix] = curIn
						if rowChan, err := t.client.GetRowsByID(ctx, v.config.Source, v.config.Collection, curIn); err == nil {
							//outMap[v.prefix] = rowChan
							wg.Add(1)
							go func() {
								for r := range rowChan {
									o := gripql.Vertex{Gid: v.prefix + r.Id, Label: v.config.Label}
									o.Data = r.Data
									reqSync.Lock()
									outReq := reqMap[r.RequestID]
									delete(reqMap, r.RequestID)
									reqSync.Unlock()
									outReq.Vertex = &o
									out <- outReq
								}
								wg.Done()
							}()
						} else {
							log.Error("Error opening streaming connection")
						}
					}
					if curIn != nil {
						reqSync.Lock()
						rNum := reqCount
						reqCount++
						reqMap[rNum] = r
						reqSync.Unlock()
						curIn <- &RowRequest{Id: id, RequestID: rNum}
					}
				}
			}
		}
		for _, c := range inMap {
			close(c)
		}
		wg.Wait()
	}()
	return out
}

func (t *TabularGraph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {

	vReqs := make(chan gdbi.ElementLookup, 10)
	out := t.GetVertexChannel(ctx, vReqs, load)

	go func() {
		defer close(vReqs)

		for r := range req {
			select {
			case <-ctx.Done():
			default:
				for vPrefix, edgeList := range t.outEdges {
					if strings.HasPrefix(r.ID, vPrefix) {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								if edge.config.EdgeTable != nil {
									res, err := t.client.GetRowsByField(context.Background(),
										edge.config.EdgeTable.Source,
										edge.config.EdgeTable.Collection,
										edge.config.EdgeTable.FromField, id)
									if err == nil {
										for row := range res {
											data := protoutil.AsMap(row.Data)
											if dst, ok := data[edge.config.EdgeTable.ToField]; ok {
												if dstStr, ok := dst.(string); ok {
													dstID := edge.config.ToVertex + dstStr
													nReq := gdbi.ElementLookup{ID: dstID, Ref: r.Ref}
													vReqs <- nReq
												}
											}
										}
									} else {
										log.Errorf("Row Error: %s", err)
									}
								} else if edge.config.FieldToID != nil {
									log.Infof("FieldToID not yet implemented")
								} else if edge.config.FieldToField != nil {
									cur := r.Ref.GetCurrent()
									fValue := ""
									if cur != nil && cur.ID == r.ID {
										if v, err := jsonpath.JsonPathLookup(cur.Data, edge.config.FieldToField.FromField); err == nil {
											if vStr, ok := v.(string); ok {
												fValue = vStr
											}
										}
									} else {
										//TODO: getting vertex out request without loading vertex
										//Trying to figure out if this can happen...
										log.Errorf("Source Vertex not in Ref")
									}
									if fValue != "" {
										res, err := t.client.GetRowsByField(context.Background(),
											edge.toVertex.config.Source,
											edge.toVertex.config.Collection,
											edge.config.FieldToField.ToField, fValue)
										if err == nil {
											for row := range res {
												o := gripql.Vertex{Gid: edge.toVertex.prefix + row.Id, Label: edge.toVertex.config.Label, Data: row.Data}
												el := gdbi.ElementLookup{ID: r.ID, Ref: r.Ref, Vertex: &o}
												out <- el
											}
										} else {
											log.Errorf("Error doing FieldToField search: %s", err)
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}()
	return out
}

func (t *TabularGraph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	vReqs := make(chan gdbi.ElementLookup, 10)
	out := t.GetVertexChannel(ctx, vReqs, load)

	go func() {
		defer close(vReqs)

		for r := range req {
			select {
			case <-ctx.Done():
			default:
				for vPrefix, edgeList := range t.inEdges {
					if strings.HasPrefix(r.ID, vPrefix) {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								if edge.config.EdgeTable != nil {
									//log.Printf("Using EdgeTable %s", *edge.config.EdgeTable)
									res, err := t.client.GetRowsByField(context.Background(),
										edge.config.EdgeTable.Source,
										edge.config.EdgeTable.Collection,
										edge.config.EdgeTable.FromField, id)
									if err == nil {
										for row := range res {
											data := protoutil.AsMap(row.Data)
											if dst, ok := data[edge.config.EdgeTable.ToField]; ok {
												if dstStr, ok := dst.(string); ok {
													dstID := edge.config.ToVertex + dstStr
													//log.Printf("Edge to %s", dstID)
													nReq := gdbi.ElementLookup{ID: dstID, Ref: r.Ref}
													vReqs <- nReq
												}
											}
										}
									} else {
										log.Errorf("Row Error: %s", err)
									}
								} else if edge.config.FieldToField != nil {
									cur := r.Ref.GetCurrent()
									fValue := ""
									if cur != nil && cur.ID == r.ID {
										if v, err := jsonpath.JsonPathLookup(cur.Data, edge.config.FieldToField.FromField); err == nil {
											if vStr, ok := v.(string); ok {
												fValue = vStr
											}
										} else {
											log.Infof("Missing Field: %s", edge.config.FieldToField.FromField)
										}
									} else {
										//TODO: getting vertex out request without loading vertex
										//Trying to figure out if this can happen...
										log.Errorf("Source Vertex not in Ref")
									}
									if fValue != "" {
										res, err := t.client.GetRowsByField(context.Background(),
											edge.toVertex.config.Source,
											edge.toVertex.config.Collection,
											edge.config.FieldToField.ToField, fValue)
										if err == nil {
											for row := range res {
												o := gripql.Vertex{Gid: edge.toVertex.prefix + row.Id, Label: edge.toVertex.config.Label, Data: row.Data}
												el := gdbi.ElementLookup{ID: r.ID, Ref: r.Ref, Vertex: &o}
												out <- el
											}
										} else {
											log.Errorf("Error doing FieldToField search: %s", err)
										}
									}
								} else if edge.config.FieldToID != nil {
									log.Infof("Need to implement FieldToID")
								}
							}
						}
					}
				}
			}
		}
	}()
	return out
}

func (t *TabularGraph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 10)

	go func() {
		defer close(out)

		for r := range req {
			select {
			case <-ctx.Done():
			default:
				for vPrefix, edgeList := range t.outEdges {
					if strings.HasPrefix(r.ID, vPrefix) {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								if edge.config.EdgeTable != nil {
									//log.Infof("Using EdgeTable %s", *edge.config.EdgeTable)
									res, err := t.client.GetRowsByField(context.Background(),
										edge.config.EdgeTable.Source,
										edge.config.EdgeTable.Collection,
										edge.config.EdgeTable.FromField, id)
									if err == nil {
										for row := range res {
											data := protoutil.AsMap(row.Data)
											if dst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
												if dstStr, ok := dst.(string); ok {
													o := gripql.Edge{
														Gid:   edge.prefix + row.Id,
														To:    edge.config.ToVertex + dstStr,
														From:  r.ID,
														Label: edge.config.Label,
														Data:  row.Data,
													}
													out <- gdbi.ElementLookup{Ref: r.Ref, Edge: &o}
												}
											}
										}
									} else {
										log.Errorf("Row Error: %s", err)
									}
								} else if edge.config.FieldToID != nil {
									log.Infof("Need to implement FieldToID")
								}
							}
						}
					}
				}
			}
		}
	}()
	return out
}

func (t *TabularGraph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 10)

	go func() {
		defer close(out)

		for r := range req {
			select {
			case <-ctx.Done():
			default:
				for vPrefix, edgeList := range t.inEdges {
					if strings.HasPrefix(r.ID, vPrefix) {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								if edge.config.EdgeTable != nil {
									//log.Printf("Using EdgeTable %s", *edge.config.EdgeTable)
									res, err := t.client.GetRowsByField(context.Background(),
										edge.config.EdgeTable.Source,
										edge.config.EdgeTable.Collection,
										edge.config.EdgeTable.FromField, id)
									if err == nil {
										for row := range res {
											data := protoutil.AsMap(row.Data)
											if dst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
												if dstStr, ok := dst.(string); ok {
													o := gripql.Edge{
														Gid:   edge.prefix + row.Id,
														From:  edge.config.ToVertex + dstStr,
														To:    r.ID,
														Label: edge.config.Label,
														Data:  row.Data,
													}
													out <- gdbi.ElementLookup{Ref: r.Ref, Edge: &o}
												}
											}
										}
									} else {
										log.Errorf("Row Error: %s", err)
									}
								} else if edge.config.FieldToID != nil {
									log.Info("Need to implement FieldToID")
								}
							}
						}
					}
				}
			}
		}
	}()
	return out
}
