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
	"github.com/bmeg/grip/util/setcmp"
	"github.com/bmeg/jsonpath"
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
	client   *GripperClient
	vertices map[string]*VertexSource
	outEdges map[string][]*EdgeSource //outbound edges by vertex prefix
	inEdges  map[string][]*EdgeSource //inbound edges by vertex prefix

	vertexSourceOrder []string //order of vertex sources, because map key iteration changes order
	edgeSourceOrder   []string
}

func NewTabularGraph(conf GraphConfig, sources map[string]GRIPSourceClient) (*TabularGraph, error) {
	out := TabularGraph{}

	out.vertices = map[string]*VertexSource{}
	out.outEdges = map[string][]*EdgeSource{}
	out.inEdges = map[string][]*EdgeSource{}

	out.vertexSourceOrder = []string{}
	out.edgeSourceOrder = []string{}

	log.Info("Loading Graph Config")

	out.client = NewGripperClient(sources)

	//Check if vertex mapping match to sources
	for _, v := range conf.Vertices {
		_, err := out.client.GetCollectionInfo(context.Background(), v.Data.Source, v.Data.Collection)
		if err != nil {
			return nil, fmt.Errorf("Unable to get collection information %s : %s", v.Data.Source, v.Data.Collection)
		}
	}

	//Check if edges match sources
	for _, e := range conf.Edges {
		if _, ok := conf.Vertices[e.To]; !ok {
			return nil, fmt.Errorf("Edge ToVertex not found")
		}
		if _, ok := conf.Vertices[e.From]; !ok {
			return nil, fmt.Errorf("Edge ToVertex not found")
		}
		if e.Data.Collection != "" && e.Data.Source != "" && e.Data.FromField != "" && e.Data.ToField != "" {
			eTable, err := out.client.GetCollectionInfo(context.Background(),
				e.Data.Source, e.Data.Collection)
			if err != nil {
				return nil, fmt.Errorf("Unable to get collection information %s : %s",
					e.Data.Source, e.Data.Collection)
			}
			if !setcmp.ContainsString(eTable.SearchFields, e.Data.ToField) {
				return nil, fmt.Errorf("Edge 'To' Field not indexed: %s %s",
					e.Data.Collection,
					e.Data.ToField)
			}
			if !setcmp.ContainsString(eTable.SearchFields, e.Data.FromField) {
				return nil, fmt.Errorf("Edge 'From' Field not indexed: %s %s",
					e.Data.Collection,
					e.Data.FromField)
			}
		} else {
			return nil, fmt.Errorf("Edge missing config info")
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

		oConf = e
		//copy the edge config, but flip the field requests for the incoming edges
		iConf.From = oConf.To
		iConf.To = oConf.From
		iConf.Label = oConf.Label
		iConf.Data.Source = oConf.Data.Source
		iConf.Data.Collection = oConf.Data.Collection
		iConf.Data.ToField = oConf.Data.FromField
		iConf.Data.FromField = oConf.Data.ToField

		out.inEdges[e.To] = append(out.inEdges[e.To], &EdgeSource{
			prefix:     ePrefix,
			config:     &iConf,
			fromVertex: out.vertices[e.To],
			toVertex:   out.vertices[e.From],
			reverse:    true,
		})
		out.outEdges[e.From] = append(out.outEdges[e.From], &EdgeSource{
			prefix:     ePrefix,
			config:     &oConf,
			fromVertex: out.vertices[e.From],
			toVertex:   out.vertices[e.To],
			reverse:    false,
		})
	}

	// make sure inEdges and outEdges are balanced
	for e := range out.outEdges {
		if _, ok := out.inEdges[e]; !ok {
			out.inEdges[e] = []*EdgeSource{}
		}
	}
	for e := range out.inEdges {
		if _, ok := out.outEdges[e]; !ok {
			out.outEdges[e] = []*EdgeSource{}
		}
	}

	// generate a list of all vertices
	for e := range out.outEdges {
		out.edgeSourceOrder = append(out.edgeSourceOrder, e)
	}
	sort.Strings(out.edgeSourceOrder)

	return &out, nil
}

func (t *TabularGraph) Close() error {
	return nil
}

func (t *TabularGraph) AddVertex(vertex []*gdbi.Vertex) error {
	return fmt.Errorf("GRIPPER Graph is ReadOnly")
}

func (t *TabularGraph) AddEdge(edge []*gdbi.Edge) error {
	return fmt.Errorf("GRIPPER is ReadOnly")
}

func (t *TabularGraph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	return fmt.Errorf("GRIPPER is ReadOnly")
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

func (t *TabularGraph) GetVertex(key string, load bool) *gdbi.Vertex {
	for _, source := range t.vertexSourceOrder {
		v := t.vertices[source]
		if strings.HasPrefix(key, v.prefix) {
			id := key[len(v.prefix):]
			c := make(chan *RowRequest, 1)
			c <- &RowRequest{Id: id, RequestID: 0}
			close(c)
			if rowChan, err := t.client.GetRowsByID(context.Background(), v.config.Data.Source, v.config.Data.Collection, c); err == nil {
				var row *Row
				for i := range rowChan {
					row = i
				}
				if row != nil {
					o := gdbi.Vertex{ID: v.prefix + row.Id, Label: v.config.Label, Data: row.Data.AsMap(), Loaded: true}
					return &o
				}
			} else {
				log.Errorf("Row not read: %s", err)
			}
		}
	}
	return nil
}

func (t *TabularGraph) GetEdge(key string, load bool) *gdbi.Edge {
	src, dst, label, err := t.ParseEdge(key)
	if err != nil {
		return nil
	}
	for _, source := range t.edgeSourceOrder {
		edgeList := t.outEdges[source]
		for _, edge := range edgeList {
			if edge.config.Label == label {
				if strings.HasPrefix(src, edge.fromVertex.prefix) && strings.HasPrefix(dst, edge.toVertex.prefix) {
					srcID := strings.TrimPrefix(src, edge.fromVertex.prefix)
					dstID := strings.TrimPrefix(dst, edge.toVertex.prefix)

					res, err := t.client.GetRowsByField(context.Background(),
						edge.config.Data.Source,
						edge.config.Data.Collection,
						edge.config.Data.FromField, srcID)

					if err == nil {
						var out *gdbi.Edge
						for row := range res {
							data := row.Data.AsMap()
							if rowDst, err := jsonpath.JsonPathLookup(data, edge.config.Data.ToField); err == nil {
								if rowdDstStr, ok := rowDst.(string); ok {
									if dstID == rowdDstStr {
										o := gdbi.Edge{
											ID:     edge.GenID(srcID, dstID), //edge.prefix + row.Id,
											To:     edge.config.To + dstID,
											From:   edge.config.From + srcID,
											Label:  edge.config.Label,
											Data:   row.Data.AsMap(),
											Loaded: true,
										}
										out = &o
									}
								}
							}
						}
						return out
					}
					log.Errorf("Row Error: %s", err)
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
				for n := range t.client.GetIDs(ctx, v.config.Data.Source, v.config.Data.Collection) {
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

func (t *TabularGraph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	out := make(chan *gdbi.Vertex, 100)
	go func() {
		for _, source := range t.vertexSourceOrder {
			c := t.vertices[source]
			//log.Infof("Getting vertices from table: %s", c.config.Label)
			for row := range t.client.GetRows(ctx, c.config.Data.Source, c.config.Data.Collection) {
				v := gdbi.Vertex{
					ID:     c.prefix + row.Id,
					Label:  c.config.Label,
					Data:   row.Data.AsMap(),
					Loaded: true,
				}
				out <- &v
			}
		}
		defer close(out)
	}()
	return out
}

func (t *TabularGraph) GetEdgeList(ctx context.Context, load bool) <-chan *gdbi.Edge {
	out := make(chan *gdbi.Edge, 100)
	go func() {
		log.Infof("Getting edge list")
		defer close(out)
		for _, source := range t.edgeSourceOrder {
			edgeList := t.outEdges[source]
			for _, edge := range edgeList {
				if ctx.Err() == context.Canceled {
					return
				}
				res := t.client.GetRows(ctx,
					edge.config.Data.Source,
					edge.config.Data.Collection)
				for row := range res {
					data := row.Data.AsMap()
					if dst, err := jsonpath.JsonPathLookup(data, edge.config.Data.ToField); err == nil {
						if dstStr, ok := dst.(string); ok {
							if src, err := jsonpath.JsonPathLookup(data, edge.config.Data.FromField); err == nil {
								if srcStr, ok := src.(string); ok {
									e := gdbi.Edge{
										ID:     edge.GenID(srcStr, dstStr),
										To:     edge.toVertex.prefix + dstStr,
										From:   edge.fromVertex.prefix + srcStr,
										Label:  edge.config.Label,
										Data:   row.Data.AsMap(),
										Loaded: true,
									}
									out <- &e
								}
							}
						}
					}
				}
			}
		}
		log.Infof("Done with edgelist")
	}()
	return out
}

func rowRequestVertexPipeline(ctx context.Context, prefix string,
	label string, client *GripperClient, source string, collection string) (chan interface{}, chan interface{}) {
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
	if rowChan, err := client.GetRowsByID(ctx, source, collection, rowIn); err == nil {
		go func() {
			defer close(out)
			for r := range rowChan {
				o := gdbi.Vertex{ID: prefix + r.Id, Label: label, Data: r.Data.AsMap(), Loaded: true}
				reqSync.Lock()
				outReq, ok := reqMap[r.RequestID]
				if !ok {
					log.Error("Bad returned request ID from plugin") //TODO: Need to do something here to prevent error in processing
				}
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
				if strings.HasPrefix(r.ID, vPrefix) && ctx.Err() != context.Canceled {
					v := t.vertices[vPrefix]
					if x, ok := prefixMap[v.prefix]; ok {
						mux.Put(x, r)
					} else {
						in, out := rowRequestVertexPipeline(ctx, v.prefix, v.config.Label, t.client, v.config.Data.Source, v.config.Data.Collection)
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
					if strings.HasPrefix(r.ID, vPrefix) && ctx.Err() != context.Canceled {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								res, err := t.client.GetRowsByField(ctx,
									edge.config.Data.Source,
									edge.config.Data.Collection,
									edge.config.Data.FromField, id)
								if err == nil {
									for row := range res {
										data := row.Data.AsMap()
										if dst, err := jsonpath.JsonPathLookup(data, edge.config.Data.ToField); err == nil {
											if dstStr, ok := dst.(string); ok {
												dstID := edge.config.To + dstStr
												nReq := gdbi.ElementLookup{ID: dstID, Ref: r.Ref}
												vReqs <- nReq
											} else {
												log.Errorf("Type Error")
											}
										} else {
											log.Errorf("Lookup Error %s", err)
										}
									}
								} else {
									if ctx.Err() != context.Canceled {
										log.Errorf("Row Error: %s\n", err)
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
					if strings.HasPrefix(r.ID, vPrefix) && ctx.Err() != context.Canceled {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								res, err := t.client.GetRowsByField(ctx,
									edge.config.Data.Source,
									edge.config.Data.Collection,
									edge.config.Data.FromField, id)
								if err == nil {
									for row := range res {
										//log.Infof("Found %s", row)
										data := row.Data.AsMap()
										if dst, err := jsonpath.JsonPathLookup(data, edge.config.Data.ToField); err == nil {
											if dstStr, ok := dst.(string); ok {
												dstID := edge.config.To + dstStr
												nReq := gdbi.ElementLookup{ID: dstID, Ref: r.Ref}
												vReqs <- nReq
											}
										}
									}
								} else {
									if ctx.Err() != context.Canceled {
										log.Errorf("Row Error: %s", err)
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
					if strings.HasPrefix(r.ID, vPrefix) && ctx.Err() != context.Canceled {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								res, err := t.client.GetRowsByField(ctx,
									edge.config.Data.Source,
									edge.config.Data.Collection,
									edge.config.Data.FromField, id)
								if err == nil {
									for row := range res {
										data := row.Data.AsMap()
										if dst, err := jsonpath.JsonPathLookup(data, edge.config.Data.ToField); err == nil {
											if dstStr, ok := dst.(string); ok {
												o := gdbi.Edge{
													ID:     edge.GenID(id, dstStr),
													From:   edge.config.From + id,
													To:     edge.config.To + dstStr,
													Label:  edge.config.Label,
													Data:   row.Data.AsMap(),
													Loaded: true,
												}
												out <- gdbi.ElementLookup{Ref: r.Ref, Edge: &o}
											}
										}
									}
								} else {
									if ctx.Err() != context.Canceled {
										log.Errorf("Row Error: %s", err)
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
					if strings.HasPrefix(r.ID, vPrefix) && ctx.Err() != context.Canceled {
						id := r.ID[len(vPrefix):len(r.ID)]
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								res, err := t.client.GetRowsByField(ctx,
									edge.config.Data.Source,
									edge.config.Data.Collection,
									edge.config.Data.FromField, id)
								if err == nil {
									for row := range res {
										data := row.Data.AsMap()
										if dst, err := jsonpath.JsonPathLookup(data, edge.config.Data.ToField); err == nil {
											if dstStr, ok := dst.(string); ok {
												o := gdbi.Edge{
													ID:     edge.GenID(dstStr, id),
													From:   edge.toVertex.prefix + dstStr,
													To:     edge.fromVertex.prefix + id,
													Label:  edge.config.Label,
													Data:   row.Data.AsMap(),
													Loaded: true,
												}
												out <- gdbi.ElementLookup{Ref: r.Ref, Edge: &o}
											}
										}
									}
								} else {
									if ctx.Err() != context.Canceled {
										log.Errorf("Row Error: %s", err)
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
