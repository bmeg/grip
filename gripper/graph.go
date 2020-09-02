package gripper

import (
	"context"
	"fmt"
	"sort"
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
	reverse    bool
}

type TabularGraph struct {
	client   *DigClient
	vertices map[string]*VertexSource
	outEdges map[string][]*EdgeSource //outbound edges by vertex prefix
	inEdges  map[string][]*EdgeSource //inbound edges by vertex prefix

	vertexSourceOrder []string //order of vertex sources, because map key iteration changes order
	edgeSourceOrder   []string
}

func NewTabularGraph(conf GraphConfig) (*TabularGraph, error) {
	out := TabularGraph{}

	out.vertices = map[string]*VertexSource{}
	out.outEdges = map[string][]*EdgeSource{}
	out.inEdges = map[string][]*EdgeSource{}

	out.vertexSourceOrder = []string{}
	out.edgeSourceOrder = []string{}

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
		out.vertexSourceOrder = append(out.vertexSourceOrder, vPrefix)
	}
	sort.Strings(out.vertexSourceOrder)

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
			reverse:    true,
		})
		out.outEdges[e.FromVertex] = append(out.outEdges[e.FromVertex], &EdgeSource{
			prefix:     ePrefix,
			config:     &oConf,
			fromVertex: out.vertices[e.FromVertex],
			toVertex:   out.vertices[e.ToVertex],
			reverse:    false,
		})
	}
	for e := range out.outEdges {
		out.edgeSourceOrder = append(out.edgeSourceOrder, e)
	}
	sort.Strings(out.edgeSourceOrder)

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

func (t *TabularGraph) getRow(source, collection, id string) *Row {
	c := make(chan *RowRequest, 1)
	c <- &RowRequest{Id: id, RequestID: 0}
	close(c)
	var row *Row
	if rowChan, err := t.client.GetRowsByID(context.Background(), source, collection, c); err == nil {
		for i := range rowChan {
			row = i
		}
	} else {
		log.Errorf("Row not read: %s", err)
	}
	return row

}

func (t *TabularGraph) GetVertex(key string, load bool) *gripql.Vertex {
	for _, source := range t.vertexSourceOrder {
		v := t.vertices[source]
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
	src, dst, label, err := t.ParseEdge(key)
	if err != nil {
		return nil
	}
	for _, source := range t.edgeSourceOrder {
		edgeList := t.outEdges[source]
		for _, edge := range edgeList {
			if edge.config.Label == label {
				if strings.HasPrefix(src, edge.fromVertex.prefix) && strings.HasPrefix(dst, edge.toVertex.prefix) {
					if edge.config.EdgeTable != nil {
						srcID := strings.TrimPrefix(src, edge.fromVertex.prefix)
						dstID := strings.TrimPrefix(dst, edge.toVertex.prefix)

						res, err := t.client.GetRowsByField(context.Background(),
							edge.config.EdgeTable.Source,
							edge.config.EdgeTable.Collection,
							edge.config.EdgeTable.FromField, srcID)

						if err == nil {
							var out *gripql.Edge
							for row := range res {
								data := protoutil.AsMap(row.Data)
								if rowDst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
									if rowdDstStr, ok := rowDst.(string); ok {
										if dstID == rowdDstStr {
											o := gripql.Edge{
												Gid:   edge.GenID(srcID, dstID), //edge.prefix + row.Id,
												To:    edge.config.ToVertex + dstID,
												From:  edge.config.FromVertex + srcID,
												Label: edge.config.Label,
												Data:  row.Data,
											}
											out = &o
										}
									}
								}
							}
							return out
						} else {
							log.Errorf("Row Error: %s", err)
						}
					} else if edge.config.FieldToID != nil {
						log.Errorf("GetEdge.FieldToID not yet implemented")
					} else if edge.config.FieldToField != nil {
						srcID := strings.TrimPrefix(src, edge.fromVertex.prefix)
						dstID := strings.TrimPrefix(dst, edge.toVertex.prefix)

						srcRow := t.getRow(edge.fromVertex.config.Source, edge.fromVertex.config.Collection, srcID)
						if srcRow != nil {
							dstRow := t.getRow(edge.fromVertex.config.Source, edge.fromVertex.config.Collection, dstID)
							if dstRow != nil {
								srcData := protoutil.AsMap(srcRow.Data)
								dstData := protoutil.AsMap(dstRow.Data)
								if srcField, err := jsonpath.JsonPathLookup(srcData, edge.config.FieldToField.FromField); err == nil {
									if dstField, err := jsonpath.JsonPathLookup(dstData, edge.config.FieldToField.ToField); err == nil {
										if srcField == dstField {
											o := gripql.Edge{
												Gid:   edge.GenID(srcID, dstID), //edge.prefix + row.Id,
												To:    edge.config.ToVertex + dstID,
												From:  edge.config.FromVertex + srcID,
												Label: edge.config.Label,
											}
											return &o
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
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
		for _, source := range t.vertexSourceOrder {
			v := t.vertices[source]
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
	s := map[string]bool{}
	for _, source := range t.vertexSourceOrder {
		i := t.vertices[source]
		s[i.config.Label] = true
	}
	out := []string{}
	for i := range s {
		out = append(out, i)
	}
	return out, nil
}

func (t *TabularGraph) ListEdgeLabels() ([]string, error) {
	s := map[string]bool{}
	for _, source := range t.edgeSourceOrder {
		i := t.inEdges[source]
		for _, e := range i {
			s[e.config.Label] = true
		}
	}
	out := []string{}
	for i := range s {
		out = append(out, i)
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
		for _, source := range t.vertexSourceOrder {
			c := t.vertices[source]
			//log.Infof("Getting vertices from table: %s", c.config.Label)
			for row := range t.client.GetRows(context.Background(), c.config.Source, c.config.Collection) {
				v := gripql.Vertex{Gid: c.prefix + row.Id, Label: c.config.Label, Data: row.Data}
				out <- &v
			}
		}
		defer close(out)
	}()
	return out
}

func (t *TabularGraph) GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge {
	out := make(chan *gripql.Edge, 100)
	go func() {
		defer close(out)
		for _, source := range t.edgeSourceOrder {
			edgeList := t.outEdges[source]
			for _, edge := range edgeList {
				if edge.config.EdgeTable != nil {
					res := t.client.GetRows(context.Background(),
						edge.config.EdgeTable.Source,
						edge.config.EdgeTable.Collection)
					for row := range res {
						data := protoutil.AsMap(row.Data)
						if dst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
							if dstStr, ok := dst.(string); ok {
								if src, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.FromField); err == nil {
									if srcStr, ok := src.(string); ok {
										e := gripql.Edge{
											Gid:   edge.GenID(srcStr, dstStr),
											To:    edge.toVertex.prefix + dstStr,
											From:  edge.fromVertex.prefix + srcStr,
											Label: edge.config.Label,
											Data:  row.Data,
										}
										out <- &e
									}
								}
							}
						}
					}
				} else if edge.config.FieldToID != nil {
					log.Errorf("GetEdgeList.FieldToID not yet implemented")
				} else if edge.config.FieldToField != nil {
					srcRes := t.client.GetRows(context.Background(),
						edge.fromVertex.config.Source,
						edge.fromVertex.config.Collection)
					for srcRow := range srcRes {
						srcData := protoutil.AsMap(srcRow.Data)
						if field, err := jsonpath.JsonPathLookup(srcData, edge.config.FieldToField.FromField); err == nil {
							if fValue, ok := field.(string); ok {
								if fValue != "" {
									dstRes, err := t.client.GetRowsByField(context.Background(),
										edge.toVertex.config.Source,
										edge.toVertex.config.Collection,
										edge.config.FieldToField.ToField, fValue)
									if err == nil {
										for dstRow := range dstRes {
											o := gripql.Edge{
												Gid:   edge.GenID(srcRow.Id, dstRow.Id),
												From:  edge.fromVertex.prefix + srcRow.Id,
												To:    edge.toVertex.prefix + dstRow.Id,
												Label: edge.config.Label,
											}
											out <- &o
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
	}()
	return out
}

func rowRequestVertexPipeline(ctx context.Context, prefix string,
	label string, client *DigClient, source string, collection string) (chan interface{}, chan interface{}) {
	reqSync := &sync.Mutex{}
	reqMap := map[uint64]gdbi.ElementLookup{}
	in := make(chan interface{}, 10)
	rowIn := make(chan *RowRequest, 10)
	go func() {
		defer close(rowIn)
		var reqCount uint64
		for r := range in {
			req := r.(gdbi.ElementLookup)
			reqSync.Lock()
			rNum := reqCount
			reqCount++
			reqMap[rNum] = req
			reqSync.Unlock()
			id := req.ID[len(prefix):len(req.ID)]
			rowIn <- &RowRequest{Id: id, RequestID: rNum}
		}
	}()

	out := make(chan interface{}, 10)
	if rowChan, err := client.GetRowsByID(context.Background(), source, collection, rowIn); err == nil {
		go func() {
			defer close(out)
			for r := range rowChan {
				o := gripql.Vertex{Gid: prefix + r.Id, Label: label}
				o.Data = r.Data
				reqSync.Lock()
				outReq := reqMap[r.RequestID]
				delete(reqMap, r.RequestID)
				reqSync.Unlock()
				outReq.Vertex = &o
				out <- outReq
			}
		}()
	} else {
		log.Error("Error opening streaming connection") //BUG: deal with this!!!
	}
	return in, out
}

func (t *TabularGraph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 10)

	prefixMap := map[string]int{}
	mux := NewChannelMux()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		mout := mux.GetOutChannel()
		defer close(out)
		for o := range mout {
			if oe, ok := o.(gdbi.ElementLookup); ok {
				out <- oe
			}
		}
		wg.Done()
	}()

	go func() {
		for r := range req {
			for _, vPrefix := range t.vertexSourceOrder {
				if strings.HasPrefix(r.ID, vPrefix) {
					v := t.vertices[vPrefix]
					if x, ok := prefixMap[v.prefix]; ok {
						mux.Put(x, r)
					} else {
						in, out := rowRequestVertexPipeline(ctx, v.prefix, v.config.Label, t.client, v.config.Source, v.config.Collection)
						x, _ := mux.AddPipeline(in, out)
						prefixMap[v.prefix] = x
						mux.Put(x, r)
					}
				}
			}
		}
		mux.Close()
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
				for _, vPrefix := range t.edgeSourceOrder {
					edgeList := t.outEdges[vPrefix]
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
											if dst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
												if dstStr, ok := dst.(string); ok {
													dstID := edge.config.ToVertex + dstStr
													nReq := gdbi.ElementLookup{ID: dstID, Ref: r.Ref}
													vReqs <- nReq
												} else {
													log.Errorf("Type Error")
												}
											}
										}
									} else {
										log.Errorf("Row Error: %s", err)
									}
								} else if edge.config.FieldToID != nil {
									log.Errorf("GetOutChannel.FieldToID not yet implemented")
								} else if edge.config.FieldToField != nil {
									//log.Infof("FieldToField lookup %#v", edge.config.FieldToField)
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
											//log.Infof("Searching %s : %s == %s", edge.toVertex.config.Collection, edge.config.FieldToField.ToField, fValue )
											for row := range res {
												//log.Infof("Found %#v", row)
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
				for _, vPrefix := range t.edgeSourceOrder {
					edgeList := t.inEdges[vPrefix]
					if strings.HasPrefix(r.ID, vPrefix) {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								if edge.config.EdgeTable != nil {
									//log.Infof("Using EdgeTable %s:%s to find %s", edge.config.EdgeTable.Collection, edge.config.EdgeTable.FromField, id)
									res, err := t.client.GetRowsByField(context.Background(),
										edge.config.EdgeTable.Source,
										edge.config.EdgeTable.Collection,
										edge.config.EdgeTable.FromField, id)
									if err == nil {
										for row := range res {
											//log.Infof("Found %s", row)
											data := protoutil.AsMap(row.Data)
											if dst, err := jsonpath.JsonPathLookup(data, edge.config.EdgeTable.ToField); err == nil {
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
								} else if edge.config.FieldToField != nil {
									cur := r.Ref.GetCurrent()
									fValue := ""
									if cur != nil && cur.ID == r.ID {
										if v, err := jsonpath.JsonPathLookup(cur.Data, edge.config.FieldToField.ToField); err == nil {
											if vStr, ok := v.(string); ok {
												fValue = vStr
											}
										} else {
											//log.Infof("Missing Field: %s", edge.config.FieldToField.ToField)
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
											edge.config.FieldToField.FromField, fValue)
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
									log.Errorf("Need to implement FieldToID")
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
				for _, vPrefix := range t.edgeSourceOrder {
					edgeList := t.outEdges[vPrefix]
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
														Gid:   edge.GenID(id, row.Id), //edge.prefix + row.Id,
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
								} else if edge.config.FieldToField != nil {
									cur := r.Ref.GetCurrent()
									fValue := ""
									if cur != nil && cur.ID == r.ID {
										if v, err := jsonpath.JsonPathLookup(cur.Data, edge.config.FieldToField.ToField); err == nil {
											if vStr, ok := v.(string); ok {
												fValue = vStr
											}
										} else {
											//log.Infof("Missing Field: %s", edge.config.FieldToField.ToField)
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
												o := gripql.Edge{
													Gid:   edge.GenID(id, row.Id),
													From:  edge.fromVertex.prefix + id,
													To:    edge.toVertex.prefix + row.Id,
													Label: edge.config.Label,
													Data:  row.Data,
												}
												el := gdbi.ElementLookup{ID: r.ID, Ref: r.Ref, Edge: &o}
												out <- el
											}
										} else {
											log.Errorf("Error doing FieldToField search: %s", err)
										}
									}

								} else if edge.config.FieldToID != nil {
									log.Errorf("Need to implement FieldToID")
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
				for _, vPrefix := range t.edgeSourceOrder {
					edgeList := t.inEdges[vPrefix]
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
														Gid:   edge.GenID(id, dstStr), //edge.prefix + row.Id,
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
								} else if edge.config.FieldToField != nil {
									//TODO: Check this
									cur := r.Ref.GetCurrent()
									fValue := ""
									if cur != nil && cur.ID == r.ID {
										if v, err := jsonpath.JsonPathLookup(cur.Data, edge.config.FieldToField.ToField); err == nil {
											if vStr, ok := v.(string); ok {
												fValue = vStr
											}
										} else {
											//log.Infof("Missing Field: %s", edge.config.FieldToField.ToField)
										}
									} else {
										//TODO: getting vertex out request without loading vertex
										//Trying to figure out if this can happen...
										log.Errorf("Source Vertex not in Ref")
									}
									if fValue != "" {
										res, err := t.client.GetRowsByField(context.Background(),
											edge.fromVertex.config.Source,
											edge.fromVertex.config.Collection,
											edge.config.FieldToField.FromField, fValue)
										if err == nil {
											for row := range res {
												o := gripql.Edge{
													Gid:   edge.GenID(row.Id, id),
													To:    edge.fromVertex.prefix + row.Id,
													From:  edge.toVertex.prefix + id,
													Label: edge.config.Label,
													Data:  row.Data,
												}
												el := gdbi.ElementLookup{ID: r.ID, Ref: r.Ref, Edge: &o}
												out <- el
											}
										} else {
											log.Errorf("Error doing FieldToField search: %s", err)
										}
									}
								} else if edge.config.FieldToID != nil {
									log.Errorf("Need to implement FieldToID")
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
