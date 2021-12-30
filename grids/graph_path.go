package grids

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/util/protoutil"
	"github.com/bmeg/grip/util/setcmp"
)

type RawPathProcessor struct {
	pipeline  RawPipeline
	db        *Graph
	inVertex  bool
	outVertex bool
}

type PathTraveler struct {
	current  *RawDataElement
	traveler *gdbi.Traveler
	path     []RawDataElementID
}

type RawDataElementID struct {
	IsVertex bool
	Gid      uint64
}

type RawDataElement struct {
	IsVertex bool
	Gid      uint64
	To       uint64
	From     uint64
	Label    uint64
}

// ElementLookup request to look up data
type RawElementLookup struct {
	ID      uint64
	Ref     *PathTraveler //interface{}
	Element *RawDataElement
}

func (rel *RawElementLookup) IsSignal() bool {
	if rel.Ref != nil {
		return rel.Ref.IsSignal()
	}
	return false
}

func SelectPath(stmts []*gripql.GraphStatement, path []int) []*gripql.GraphStatement {
	out := []*gripql.GraphStatement{}
	for _, p := range path {
		out = append(out, stmts[p])
	}
	return out
}

func NewPathTraveler(tr *gdbi.Traveler, isVertex bool, gg *Graph) *PathTraveler {
	el := RawDataElement{}
	cur := tr.GetCurrent()
	el.IsVertex = isVertex
	if isVertex {
		el.Gid, _ = gg.kdb.keyMap.GetVertexKey(gg.graphKey, cur.ID)
		el.Label, _ = gg.kdb.keyMap.GetLabelKey(gg.graphKey, cur.Label)
	} else {
		el.Gid, _ = gg.kdb.keyMap.GetEdgeKey(gg.graphKey, cur.ID)
		el.Label, _ = gg.kdb.keyMap.GetLabelKey(gg.graphKey, cur.Label)
		el.To, _ = gg.kdb.keyMap.GetVertexKey(gg.graphKey, cur.To)
		el.From, _ = gg.kdb.keyMap.GetVertexKey(gg.graphKey, cur.From)
	}
	return &PathTraveler{
		current:  &el,
		traveler: tr,
		path:     []RawDataElementID{},
	}
}

func (t *PathTraveler) IsSignal() bool {
	return t.traveler.IsSignal()
}

// AddCurrent creates a new copy of the travel with new 'current' value
func (t *PathTraveler) AddCurrent(r *RawDataElement) *PathTraveler {
	o := t.traveler.Copy()
	a := PathTraveler{current: r, traveler: o, path: append(t.path, RawDataElementID{IsVertex: r.IsVertex, Gid: r.Gid})}
	return &a
}

func (t *PathTraveler) ToVertexTraveler(gg *Graph) *gdbi.Traveler {
	o := t.traveler.AddCurrent(t.current.VertexDataElement(gg))
	o.Path = make([]gdbi.DataElementID, len(t.traveler.Path)+len(t.path))
	for i := range t.traveler.Path {
		o.Path[i] = t.traveler.Path[i]
	}
	for i := range t.path {
		if t.path[i].IsVertex {
			Gid, _ := gg.kdb.keyMap.GetVertexID(gg.graphKey, t.path[i].Gid)
			o.Path[len(t.traveler.Path)+i] = gdbi.DataElementID{Vertex: Gid}
		} else {
			Gid, _ := gg.kdb.keyMap.GetEdgeID(gg.graphKey, t.path[i].Gid)
			o.Path[len(t.traveler.Path)+i] = gdbi.DataElementID{Edge: Gid}
		}
	}
	return o
}

func (t *PathTraveler) ToEdgeTraveler(gg *Graph) *gdbi.Traveler {
	o := t.traveler.AddCurrent(t.current.EdgeDataElement(gg))
	o.Path = make([]gdbi.DataElementID, len(t.traveler.Path)+len(t.path))
	for i := range t.traveler.Path {
		o.Path[i] = t.traveler.Path[i]
	}
	for i := range t.path {
		if t.path[i].IsVertex {
			Gid, _ := gg.kdb.keyMap.GetVertexID(gg.graphKey, t.path[i].Gid)
			o.Path[len(t.traveler.Path)+i] = gdbi.DataElementID{Vertex: Gid}
		} else {
			Gid, _ := gg.kdb.keyMap.GetEdgeID(gg.graphKey, t.path[i].Gid)
			o.Path[len(t.traveler.Path)+i] = gdbi.DataElementID{Edge: Gid}
		}
	}
	return o
}

type RawProcessor interface {
	Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context
}

type RawPipeline []RawProcessor

func RawPathCompile(db *Graph, ps gdbi.PipelineState, stmts []*gripql.GraphStatement) (gdbi.Processor, error) {

	pipeline := RawPipeline{}
	firstType := ps.GetLastType()

	for _, s := range stmts {
		switch stmt := s.GetStatement().(type) {
		case *gripql.GraphStatement_V:
			ids := protoutil.AsStringList(stmt.V)
			ps.SetLastType(gdbi.VertexData)
			pipeline = append(pipeline, &PathVProc{db: db, ids: ids})
		case *gripql.GraphStatement_In:
			if ps.GetLastType() == gdbi.VertexData {
				labels := protoutil.AsStringList(stmt.In)
				ps.SetLastType(gdbi.VertexData)
				pipeline = append(pipeline, &PathInProc{db: db, labels: labels})
			} else if ps.GetLastType() == gdbi.EdgeData {
				ps.SetLastType(gdbi.VertexData)
				pipeline = append(pipeline, &PathInEdgeAdjProc{db: db})
			}
		case *gripql.GraphStatement_Out:
			if ps.GetLastType() == gdbi.VertexData {
				labels := protoutil.AsStringList(stmt.Out)
				ps.SetLastType(gdbi.VertexData)
				pipeline = append(pipeline, &PathOutProc{db: db, labels: labels})
			} else if ps.GetLastType() == gdbi.EdgeData {
				ps.SetLastType(gdbi.VertexData)
				pipeline = append(pipeline, &PathOutEdgeAdjProc{db: db})
			}
		case *gripql.GraphStatement_InE:
			labels := protoutil.AsStringList(stmt.InE)
			ps.SetLastType(gdbi.EdgeData)
			pipeline = append(pipeline, &PathInEProc{db: db, labels: labels})
		case *gripql.GraphStatement_OutE:
			labels := protoutil.AsStringList(stmt.OutE)
			ps.SetLastType(gdbi.EdgeData)
			pipeline = append(pipeline, &PathOutEProc{db: db, labels: labels})
		case *gripql.GraphStatement_HasLabel:
			labels := protoutil.AsStringList(stmt.HasLabel)
			pipeline = append(pipeline, &PathLabelProc{db: db, labels: labels})
		default:
			log.Printf("Unknown command: %T", s.GetStatement())
			return nil, fmt.Errorf("Unknown command: %T", s.GetStatement())
		}
	}
	return &RawPathProcessor{
		pipeline: pipeline, db: db,
		inVertex:  firstType == gdbi.VertexData,
		outVertex: ps.GetLastType() == gdbi.VertexData,
	}, nil
}

func (pc *RawPathProcessor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {

	bufsize := 100
	inR := make(chan *PathTraveler, bufsize)
	finalR := make(chan *PathTraveler, bufsize)
	outR := finalR
	for i := len(pc.pipeline) - 1; i >= 0; i-- {
		ctx = pc.pipeline[i].Process(ctx, inR, outR)
		outR = inR
		inR = make(chan *PathTraveler, bufsize)
	}

	go func() {
		defer close(outR)
		for tr := range in {
			outR <- NewPathTraveler(tr, pc.inVertex, pc.db)
		}
	}()

	go func() {
		defer close(out)
		if pc.outVertex {
			for tr := range finalR {
				o := tr.ToVertexTraveler(pc.db)
				out <- o
			}
		} else {
			for tr := range finalR {
				o := tr.ToEdgeTraveler(pc.db)
				out <- o
			}
		}
	}()

	return ctx
}

type PathVProc struct {
	db  *Graph
	ids []string
}

func (r *PathVProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	go func() {
		defer close(out)
		if len(r.ids) == 0 {
			for elem := range r.db.RawGetVertexList(ctx) {
				out <- &PathTraveler{current: elem}
			}
		} else {
			for _, i := range r.ids {
				if key, ok := r.db.kdb.keyMap.GetVertexKey(r.db.graphKey, i); ok {
					label := r.db.kdb.keyMap.GetVertexLabel(r.db.graphKey, key)
					out <- &PathTraveler{
						current: &RawDataElement{IsVertex: true, Gid: key, Label: label},
					}
				}
			}
		}
	}()
	return ctx
}

type PathOutProc struct {
	db     *Graph
	labels []string
}

func (r *PathOutProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- &RawElementLookup{
				ID:  i.current.Gid,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetOutChannel(queryChan, r.labels) {
			i := ov.Ref //.(*PathTraveler)
			out <- i.AddCurrent(ov.Element)
		}
	}()
	return ctx
}

type PathInProc struct {
	db     *Graph
	labels []string
}

func (r *PathInProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- &RawElementLookup{
				ID:  i.current.Gid,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetInChannel(queryChan, r.labels) {
			i := ov.Ref //.(*PathTraveler)
			out <- i.AddCurrent(ov.Element)
		}
	}()
	return ctx
}

type PathOutEProc struct {
	db     *Graph
	labels []string
}

func (r *PathOutEProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- &RawElementLookup{
				ID:  i.current.Gid,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetOutEdgeChannel(queryChan, r.labels) {
			i := ov.Ref //.(*PathTraveler)
			out <- i.AddCurrent(ov.Element)
			//fmt.Printf("Found : %d %s\n", ov.Element.Gid, r.db.kdb.keyMap.GetEdgeID(r.db.graphKey, ov.Element.Gid))
		}
	}()
	return ctx
}

// PathOutAdjEProc process edge to out
type PathOutEdgeAdjProc struct {
	db *Graph
}

func (r *PathOutEdgeAdjProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	//fmt.Printf("Running PathOutEdgeAdjProc\n")
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- &RawElementLookup{
				ID:  i.current.To,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetVertexChannel(queryChan) {
			i := ov.Ref //.(*PathTraveler)
			out <- i.AddCurrent(ov.Element)
		}
	}()
	return ctx
}

type PathInEProc struct {
	db     *Graph
	labels []string
}

func (r *PathInEProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- &RawElementLookup{
				ID:  i.current.Gid,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetInEdgeChannel(queryChan, r.labels) {
			i := ov.Ref //.(*PathTraveler)
			out <- i.AddCurrent(ov.Element)
		}
	}()
	return ctx
}

type PathInEdgeAdjProc struct {
	db     *Graph
	labels []string
}

func (r *PathInEdgeAdjProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	queryChan := make(chan *RawElementLookup, 100)
	go func() {
		defer close(queryChan)
		for i := range in {
			queryChan <- &RawElementLookup{
				ID:  i.current.From,
				Ref: i,
			}
		}
	}()
	go func() {
		defer close(out)
		for ov := range r.db.RawGetVertexChannel(queryChan) {
			i := ov.Ref //.(*PathTraveler)
			out <- i.AddCurrent(ov.Element)
		}
	}()
	return ctx
}

type PathLabelProc struct {
	db     *Graph
	labels []string
}

func (r *PathLabelProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	labels := []uint64{}
	for i := range r.labels {
		if j, ok := r.db.kdb.keyMap.GetLabelKey(r.db.graphKey, r.labels[i]); ok {
			labels = append(labels, j)
		}
	}
	go func() {
		defer close(out)
		for i := range in {
			if i.IsSignal() || setcmp.ContainsUint(labels, i.current.Label) {
				out <- i
			}
		}
	}()
	return ctx
}

func (rd *RawDataElement) VertexDataElement(ggraph *Graph) *gdbi.DataElement {
	Gid, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, rd.Gid)
	Label, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, rd.Label)
	return &gdbi.DataElement{ID: Gid, Label: Label, Data:map[string]interface{}{}}
}

func (rd *RawDataElement) EdgeDataElement(ggraph *Graph) *gdbi.DataElement {
	Gid, _ := ggraph.kdb.keyMap.GetEdgeID(ggraph.graphKey, rd.Gid)
	Label, _ := ggraph.kdb.keyMap.GetLabelID(ggraph.graphKey, rd.Label)
	To, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, rd.To)
	From, _ := ggraph.kdb.keyMap.GetVertexID(ggraph.graphKey, rd.From)
	return &gdbi.DataElement{ID: Gid, To: To, From: From, Label: Label, Data:map[string]interface{}{}}
}

func (ggraph *Graph) RawGetVertexList(ctx context.Context) <-chan *RawDataElement {
	o := make(chan *RawDataElement, 100)
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			vPrefix := VertexListPrefix(ggraph.graphKey)
			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, vkey := VertexKeyParse(keyValue)
				lkey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, vkey)
				o <- &RawDataElement{
					IsVertex: true,
					Gid:      vkey,
					Label:    lkey,
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetEdgeList(ctx context.Context) <-chan *RawDataElement {
	o := make(chan *RawDataElement, 100)
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			ePrefix := EdgeListPrefix(ggraph.graphKey)
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				_, ekey, srcvkey, dstvkey, lkey := EdgeKeyParse(keyValue)
				o <- &RawDataElement{
					IsVertex: false,
					Gid:      ekey,
					Label:    lkey,
					From:     srcvkey,
					To:       dstvkey,
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetVertexChannel(reqChan chan *RawElementLookup) <-chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	go func() {
		defer close(o)
		for req := range reqChan {
			if req.IsSignal() {
				o <- req
			} else {
				vkey := req.ID
				lkey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, vkey)
				o <- &RawElementLookup{
					Element: &RawDataElement{
						IsVertex: true,
						Gid:      vkey,
						Label:    lkey,
					},
					ID:  req.ID,
					Ref: req.Ref,
				}
			}
		}
	}()
	return o
}

func (ggraph *Graph) RawGetOutChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := SrcEdgePrefix(ggraph.graphKey, req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						_, _, _, dstvkey, lkey := SrcEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							dstlkey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, dstvkey)
							o <- &RawElementLookup{
								Element: &RawDataElement{
									IsVertex: true,
									Gid:      dstvkey,
									Label:    dstlkey,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetInChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := DstEdgePrefix(ggraph.graphKey, req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						_, _, srcvkey, _, lkey := DstEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							srclkey := ggraph.kdb.keyMap.GetVertexLabel(ggraph.graphKey, srcvkey)
							o <- &RawElementLookup{
								Element: &RawDataElement{
									IsVertex: true,
									Gid:      srcvkey,
									Label:    srclkey,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetOutEdgeChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := SrcEdgePrefix(ggraph.graphKey, req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						_, ekey, srcvkey, dstvkey, lkey := SrcEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							o <- &RawElementLookup{
								Element: &RawDataElement{
									IsVertex: false,
									Gid:      ekey,
									Label:    lkey,
									From:     srcvkey,
									To:       dstvkey,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetInEdgeChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(ggraph.graphKey, edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := DstEdgePrefix(ggraph.graphKey, req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						_, ekey, srcvkey, dstvkey, lkey := DstEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							o <- &RawElementLookup{
								Element: &RawDataElement{
									IsVertex: false,
									Gid:      ekey,
									Label:    lkey,
									From:     srcvkey,
									To:       dstvkey,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}
