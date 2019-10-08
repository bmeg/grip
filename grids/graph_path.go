package grids

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/kvi"
  "github.com/bmeg/grip/protoutil"
)

type RawPathProcessor struct {
  pipeline RawPipeline
}

type PathTraveler struct {
	current  *RawDataElement
	traveler *gdbi.Traveler
}

type RawDataElement struct {
	Gid   uint64
	To    uint64
	From  uint64
	Label uint64
}

// ElementLookup request to look up data
type RawElementLookup struct {
	ID   uint64
	Ref  interface{}
	Data *RawDataElement
}

// AddCurrent creates a new copy of the travel with new 'current' value
func (t *PathTraveler) AddCurrent(r *RawDataElement) *PathTraveler {
	o := t.traveler.AddCurrent(nil)
	a := PathTraveler{current: r, traveler: o}
	return &a
}

type RawProcessor interface {
	Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context
}

type RawPipeline []RawProcessor

func RawPathCompile(db *GridsGraph, stmts []*gripql.GraphStatement) gdbi.Processor {

	pipeline :=  RawPipeline{}

	for _, s := range stmts {
		switch stmt := s.GetStatement().(type) {
		case *gripql.GraphStatement_V:
      ids := protoutil.AsStringList(stmt.V)
      //ps.LastType = gdbi.VertexData
      pipeline = append(pipeline, &PathVProc{db: db, ids: ids})
		case *gripql.GraphStatement_In:
			fmt.Printf("In\n")
		case *gripql.GraphStatement_Out:
			fmt.Printf("Out\n")
		default:
			fmt.Printf("Unknown command: %T\n", s.GetStatement())
		}
	}
	return &RawPathProcessor{pipeline}
}

func (pc *RawPathProcessor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	return ctx
}

type PathVProc struct {
	db *GridsGraph
  ids []string
}

func (r *PathVProc) Process(ctx context.Context, in chan *PathTraveler, out chan *PathTraveler) context.Context {
	for elem := range r.db.RawGetVertexList(ctx) {
		out <- &PathTraveler{current: elem}
	}
	return ctx
}

type PathOutProc struct {
	db     *GridsGraph
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
			i := ov.Ref.(*PathTraveler)
			out <- i.AddCurrent(ov.Data)
		}
	}()
	return ctx
}


type PathInProc struct {
	db     *GridsGraph
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
			i := ov.Ref.(*PathTraveler)
			out <- i.AddCurrent(ov.Data)
		}
	}()
	return ctx
}


type PathOutEProc struct {
	db     *GridsGraph
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
			i := ov.Ref.(*PathTraveler)
			out <- i.AddCurrent(ov.Data)
		}
	}()
	return ctx
}

type PathInEProc struct {
	db     *GridsGraph
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
		for ov := range r.db.RawGetOutEdgeChannel(queryChan, r.labels) {
			i := ov.Ref.(*PathTraveler)
			out <- i.AddCurrent(ov.Data)
		}
	}()
	return ctx
}



func (rd *RawDataElement) VertexDataElement(ggraph *GridsGraph) *gdbi.DataElement {
	Gid := ggraph.kdb.keyMap.GetVertexID(rd.Gid)
	Label := ggraph.kdb.keyMap.GetLabelID(rd.Label)
	return &gdbi.DataElement{ID: Gid, Label: Label}
}

func (rd *RawDataElement) EdgeDataElement(ggraph *GridsGraph) *gdbi.DataElement {
	Gid := ggraph.kdb.keyMap.GetEdgeID(rd.Gid)
	Label := ggraph.kdb.keyMap.GetLabelID(rd.Label)
	To := ggraph.kdb.keyMap.GetEdgeID(rd.To)
	From := ggraph.kdb.keyMap.GetEdgeID(rd.From)
	return &gdbi.DataElement{ID: Gid, To: To, From: From, Label: Label}
}

func (ggraph *GridsGraph) RawGetVertexList(ctx context.Context) <-chan *RawDataElement {
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
				lkey := ggraph.kdb.keyMap.GetVertexLabel(vkey)
				o <- &RawDataElement{
					Gid:   vkey,
					Label: lkey,
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *GridsGraph) RawGetEdgeList(ctx context.Context) <-chan *RawDataElement {
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
					Gid:   ekey,
					Label: lkey,
					From:  srcvkey,
					To:    dstvkey,
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *GridsGraph) RawGetOutChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				ePrefix := SrcEdgePrefix(ggraph.graphKey, req.ID)
				for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
					keyValue := it.Key()
					_, _, _, dstvkey, lkey := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || containsUint(edgeLabelKeys, lkey) {
						dstlkey := ggraph.kdb.keyMap.GetVertexLabel(dstvkey)
						o <- &RawElementLookup{
							Data: &RawDataElement{
								Gid:   dstvkey,
								Label: dstlkey,
							},
							ID:  req.ID,
							Ref: req.Ref,
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *GridsGraph) RawGetInChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				ePrefix := DstEdgePrefix(ggraph.graphKey, req.ID)
				for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
					keyValue := it.Key()
					_, _, srcvkey, _, lkey := DstEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || containsUint(edgeLabelKeys, lkey) {
						srclkey := ggraph.kdb.keyMap.GetVertexLabel(srcvkey)
						o <- &RawElementLookup{
							Data: &RawDataElement{
								Gid:   srcvkey,
								Label: srclkey,
							},
							ID:  req.ID,
							Ref: req.Ref,
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *GridsGraph) RawGetOutEdgeChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				ePrefix := SrcEdgePrefix(ggraph.graphKey, req.ID)
				for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
					keyValue := it.Key()
					_, ekey, srcvkey, dstvkey, lkey := SrcEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || containsUint(edgeLabelKeys, lkey) {
						o <- &RawElementLookup{
							Data: &RawDataElement{
								Gid:   ekey,
								Label: lkey,
								From:  srcvkey,
								To:    dstvkey,
							},
							ID:  req.ID,
							Ref: req.Ref,
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *GridsGraph) RawGetInEdgeChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.kdb.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				ePrefix := DstEdgePrefix(ggraph.graphKey, req.ID)
				for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
					keyValue := it.Key()
					_, ekey, srcvkey, dstvkey, lkey := DstEdgeKeyParse(keyValue)
					if len(edgeLabels) == 0 || containsUint(edgeLabelKeys, lkey) {
						o <- &RawElementLookup{
							Data: &RawDataElement{
								Gid:   ekey,
								Label: lkey,
								From:  srcvkey,
								To:    dstvkey,
							},
							ID:  req.ID,
							Ref: req.Ref,
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}
