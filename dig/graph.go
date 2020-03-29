package dig

import (
	"context"
	"fmt"
	"log"
	"sync"
	//"path/filepath"
	"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util/setcmp"
)

type VertexSource struct {
	prefix     string
	config     *VertexConfig
}

type EdgeSource struct {
	prefix     string
	config     *EdgeConfig
}


type TabularGraph struct {
	client   *DigClient
	vertices map[string]*VertexSource
	outEdges map[string][]*EdgeSource
	inEdges  map[string][]*EdgeSource
}

func NewTabularGraph(conf GraphConfig) (*TabularGraph, error) {
	out := TabularGraph{}

	out.vertices = map[string]*VertexSource{}
	out.outEdges = map[string][]*EdgeSource{}
	out.inEdges = map[string][]*EdgeSource{}

	log.Printf("Loading Graph Config")

	out.client = NewDigClient(conf.Sources)

	//Check if vertex mapping match to sources
	for _, v := range conf.Vertices {
		_, err := out.client.GetCollectionInfo(context.Background(), v.Source, v.Collection)
		if err != nil {
			return nil, fmt.Errorf("Unable to get collection information", v.Source, v.Collection)
		}
	}

	//add parameters to configs for the tables, based on how the edges will use them
	for _, e := range conf.Edges {
		toVertex := conf.Vertices[e.ToVertex]
		fromVertex := conf.Vertices[e.FromVertex]
		log.Printf("Edges: %s %s %s", fromVertex, toVertex, e.Label )
		/*

		if toTableOpts == nil || fromTableOpts == nil {
			return nil, fmt.Errorf("Trying to use undeclared table")
		}
		if e.FromField != fromTableOpts.PrimaryKey {
			if !setcmp.ContainsString(fromTableOpts.IndexedColumns, e.FromField) {
				fromTableOpts.IndexedColumns = append(fromTableOpts.IndexedColumns, e.FromField)
			}
		}
		if e.ToField != toTableOpts.PrimaryKey {
			if !setcmp.ContainsString(toTableOpts.IndexedColumns, e.ToField) {
				toTableOpts.IndexedColumns = append(toTableOpts.IndexedColumns, e.ToField)
			}
		}
		*/

	}

	//map the table drivers back onto the vertices that will use them
	for vPrefix, v := range conf.Vertices {
		vConf := v
		log.Printf("Adding vertex prefix: %s label: %s", vPrefix, v.Label)
		out.vertices[vPrefix] = &VertexSource{prefix: vPrefix, config: &vConf}
	}

	/*
	for ePrefix, e := range conf.Edges {
		if e.Label != "" {
			toVertex := conf.Vertices[e.ToVertex]
			fromVertex := conf.Vertices[e.FromVertex]
			fromDriver := driverMap[fromVertex.Table]
			toDriver := driverMap[toVertex.Table]
			es := EdgeSource{
				label:      e.Label,
				fromDriver: fromDriver,
				toDriver:   toDriver, prefix: ePrefix,
				fromVertex: e.FromVertex, toVertex: e.ToVertex,
				fromField: e.FromField, toField: e.ToField}
			if x, ok := out.outEdges[e.FromVertex]; ok {
				out.outEdges[e.FromVertex] = append(x, &es)
			} else {
				out.outEdges[e.FromVertex] = []*EdgeSource{&es}
			}
			if x, ok := out.inEdges[e.ToVertex]; ok {
				out.inEdges[e.ToVertex] = append(x, &es)
			} else {
				out.inEdges[e.ToVertex] = []*EdgeSource{&es}
			}
		}
		if e.BackLabel != "" {
			toVertex := conf.Vertices[e.ToVertex]
			fromVertex := conf.Vertices[e.FromVertex]
			fromDriver := driverMap[fromVertex.Table]
			toDriver := driverMap[toVertex.Table]
			es := EdgeSource{
				label:      e.BackLabel,
				fromDriver: toDriver,
				toDriver:   fromDriver, prefix: ePrefix,
				fromVertex: e.ToVertex, toVertex: e.FromVertex,
				fromField: e.ToField, toField: e.FromField}

			if x, ok := out.outEdges[e.ToVertex]; ok {
				out.outEdges[e.ToVertex] = append(x, &es)
			} else {
				out.outEdges[e.ToVertex] = []*EdgeSource{&es}
			}
			if x, ok := out.inEdges[e.FromVertex]; ok {
				out.inEdges[e.FromVertex] = append(x, &es)
			} else {
				out.inEdges[e.FromVertex] = []*EdgeSource{&es}
			}

		}
	}
	*/

	return &out, nil
}

func (t *TabularGraph) Close() error {
	return nil
}

func (t *TabularGraph) AddVertex(vertex []*gripql.Vertex) error {
	return fmt.Errorf("AddVertex not implemented")
}

func (t *TabularGraph) AddEdge(edge []*gripql.Edge) error {
	return fmt.Errorf("AddEdge not implemented")
}

func (t *TabularGraph) BulkAdd(stream <-chan *gripql.GraphElement) error {
	return fmt.Errorf("BulkAdd not implemented")
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
			c := make(chan RowRequest, 1)
			c <- RowRequest{Id:id, RequestID:0}
			close(c)
			if rowChan, err := t.client.GetRowsByID(context.Background(), v.config.Source, v.config.Collection, c); err == nil {
				var row *Row
				for i := range rowChan {
					row = i
				}
				o := gripql.Vertex{Gid: v.prefix + row.Id, Label: v.config.Label, Data:row.Data}
				return &o
			} else {
				log.Printf("Row not read")
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
	log.Printf("Calling GetVertexIndexList")
	return nil
}

func (t *TabularGraph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
	out := make(chan *gripql.Vertex, 100)
	go func() {
		for _, c := range t.vertices {
			log.Printf("Getting vertices from table: %s", c.config.Label)
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
	log.Printf("Calling GetEdgeList")
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

		inMap := map[string]chan RowRequest{}
		outMap := map[string]chan *Row{}
		reqMap := map[uint64]gdbi.ElementLookup{}
		var reqCount uint64
		reqSync := &sync.Mutex{} //sync access to reqMap, other maps are only access by outer thread

		for r := range req {
			for _, v := range t.vertices {
				if strings.HasPrefix(r.ID, v.prefix) {
					id := r.ID[len(v.prefix):len(r.ID)]

					var curIn chan RowRequest
					if x, ok := inMap[v.prefix]; ok {
						curIn = x
					} else {
						curIn = make(chan RowRequest, 10)
						if rowChan, err := t.client.GetRowsByID(ctx, v.config.Source, v.config.Collection, curIn); err == nil {
							inMap[v.prefix] = curIn
							outMap[v.prefix] = rowChan
							go func() {
								for r := range rowChan {
									o := gripql.Vertex{Gid: v.prefix + r.Id, Label: v.config.Label}
									o.Data = r.Data
									reqSync.Lock()
									outReq := reqMap[ r.RequestID ]
									delete(reqMap, r.RequestID)
									reqSync.Unlock()
									outReq.Vertex = &o
									out <- outReq
								}
							}()
						} else {
							log.Printf("Error opening streaming connection")
						}
					}
					if curIn != nil {
						reqSync.Lock()
						rNum := reqCount
						reqCount++
						reqMap[ rNum ] = r
						reqSync.Unlock()
						curIn <- RowRequest{ Id:id, RequestID:rNum }
					}
				}
			}
		}
	}()
	return out
}

func (t *TabularGraph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for r := range req {
			select {
			case <-ctx.Done():
			default:
				for vPrefix, edgeList := range t.outEdges {
					if strings.HasPrefix(r.ID, vPrefix) {
						for _, edge := range edgeList {
							if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, edge.config.Label) {
								/*
								log.Printf("Checkout edge %s %s", edge.fromVertex, edge.toVertex)
								id := r.ID[len(vPrefix):len(r.ID)]

								fromVertex := t.vertices[edge.fromVertex]

								joinVal := ""
								if edge.fromField == fromVertex.config.PrimaryKey {
									joinVal = id
								} else {
									elem := r.Ref.GetCurrent()
									joinVal = elem.Data[edge.fromField].(string)
								}
								toVertex := t.vertices[edge.toVertex]
								log.Printf("GetOutChannel: %s %#v", edgeLabels, toVertex)

								if edge.toField == toVertex.config.PrimaryKey {
									if row, err := edge.toDriver.GetRowByID(joinVal); err == nil {
										outV := gripql.Vertex{Gid: toVertex.prefix + row.Key, Label: toVertex.label}
										outV.Data = protoutil.AsStruct(row.Values)
										r.Vertex = &outV
										out <- r
									}
								} else {
									for row := range edge.toDriver.GetRowsByField(ctx, edge.toField, joinVal) {
										outV := gripql.Vertex{Gid: toVertex.prefix + row.Key, Label: toVertex.label}
										outV.Data = protoutil.AsStruct(row.Values)
										r.Vertex = &outV
										out <- r
									}
								}
								*/
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
	out := make(chan gdbi.ElementLookup, 10)
	go func () {
		defer close(out)
		for r := range req {
			out <- r
		}
	}()
	return out
}

func (t *TabularGraph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 10)
	go func () {
		defer close(out)
		for r := range req {
			out <- r
		}
	}()
	return out
}

func (t *TabularGraph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 10)
	go func () {
		defer close(out)
		for r := range req {
			out <- r
		}
	}()
	return out
}
