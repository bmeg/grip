package grids

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/setcmp"
	multierror "github.com/hashicorp/go-multierror"
)

const (
	VTABLE_PREFIX = "v_"
	ETABLE_PREFIX = "e_"
)

// GetTimestamp returns the update timestamp
func (ggraph *Graph) GetTimestamp() string {
	return ggraph.ts.Get(ggraph.graphID)
}

func insertVertex(tx *pebblebulk.PebbleBulk, vertex *gdbi.Vertex) error {
	if vertex.ID == "" {
		return fmt.Errorf("inserting null key vertex")
	}
	if err := tx.Set(VertexKey(vertex.ID, vertex.Label), nil, nil); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func (ggraph *Graph) indexVertex(vertex *gdbi.Vertex, tx *pebblebulk.PebbleBulk) error {
	vertexLabel := VTABLE_PREFIX + vertex.Label
	ggraph.bsonkv.Lock.Lock()
	table, ok := ggraph.bsonkv.Tables[vertexLabel]
	ggraph.bsonkv.Lock.Unlock()
	if !ok {
		log.Debugf("Creating new table %s for label %s on graph %s", vertexLabel, vertex.Label, ggraph.graphID)
		newTable, err := ggraph.bsonkv.New(vertexLabel, nil)
		if err != nil {
			return fmt.Errorf("indexVertex: %s", err)
		}
		ggraph.bsonkv.Lock.Lock()
		table = newTable.(*bsontable.BSONTable)
		ggraph.bsonkv.Tables[vertexLabel] = table
		ggraph.bsonkv.Lock.Unlock()
	}
	if err := table.AddRow(benchtop.Row{Id: []byte(vertex.ID), TableName: vertexLabel, Data: vertex.Data}, tx); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}

	_, fieldsExist := ggraph.bsonkv.Fields[vertexLabel]
	if fieldsExist {
		for field := range ggraph.bsonkv.Fields[vertexLabel] {
			if val := bsontable.PathLookup(vertex.Data, field); val != nil {
				tx.Set(benchtop.FieldKey(field, vertexLabel, val, []byte(vertex.ID)), []byte{}, nil)
			}
		}
	}

	return nil
}

func insertEdge(tx *pebblebulk.PebbleBulk, edge *completeEdge) error {
	if edge.OEdge.ID == "" || edge.OEdge.From == "" || edge.OEdge.To == "" || edge.OEdge.Label == "" {
		return fmt.Errorf("inserting null key edge")
	}
	err := tx.Set(EdgeKey(edge.OEdge.ID, edge.OEdge.From, edge.OEdge.To, edge.OEdge.Label), nil, nil)
	if err != nil {
		return err
	}
	err = tx.Set(DstEdgeKey(
		edge.OEdge.ID,
		edge.OEdge.From,
		edge.OEdge.To,
		edge.OEdge.Label,
		edge.FromLabel,
	), []byte{}, nil)
	if err != nil {
		return err
	}
	err = tx.Set(SrcEdgeKey(
		edge.OEdge.ID,
		edge.OEdge.From,
		edge.OEdge.To,
		edge.OEdge.Label,
		edge.ToLabel,
	), []byte{}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (ggraph *Graph) bulkGet(edges []*completeEdge) <-chan *completeEdge {
	resultsChan := make(chan *completeEdge, 100)
	go func() {
		defer close(resultsChan)
		err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for i, edge := range edges {
				err := it.Seek(VertexKeyPrefix(edge.OEdge.From))
				if err == nil {
					tmp := bytes.Split(it.Key(), []byte{0})
					edges[i].FromLabel = tmp[2]
				}
				err = it.Seek(VertexKeyPrefix(edge.OEdge.To))
				if err == nil {
					tmp := bytes.Split(it.Key(), []byte{0})
					edges[i].ToLabel = tmp[2]
				}
				resultsChan <- edges[i]
			}
			return nil
		})
		if err != nil {
			log.Errorf("Error in PebbleBulk BulkGet (ViewRange) %s", err)
		}
	}()

	return resultsChan
}

func (ggraph *Graph) indexEdge(edge *gdbi.Edge, tx *pebblebulk.PebbleBulk) error {
	edgeLabel := ETABLE_PREFIX + edge.Label
	ggraph.bsonkv.Lock.Lock()
	table, ok := ggraph.bsonkv.Tables[edgeLabel]
	ggraph.bsonkv.Lock.Unlock()

	if !ok {
		log.Debugf("Creating new table %s for label %s on graph %s", edgeLabel, edge.Label, ggraph.graphID)
		newTable, err := ggraph.bsonkv.New(edgeLabel, nil)
		if err != nil {
			return fmt.Errorf("indexEdge: bsonkv.New: %s", err)
		}
		ggraph.bsonkv.Lock.Lock()
		table = newTable.(*bsontable.BSONTable)
		ggraph.bsonkv.Tables[edgeLabel] = table
		ggraph.bsonkv.Lock.Unlock()
	}
	if err := table.AddRow(benchtop.Row{Id: []byte(edge.ID), TableName: edgeLabel, Data: edge.Data}, tx); err != nil {
		return fmt.Errorf("indexEdge: table.AddRow: %s", err)
	}

	_, fieldsExist := ggraph.bsonkv.Fields[edgeLabel]
	if fieldsExist {
		for field := range ggraph.bsonkv.Fields[edgeLabel] {
			if val := bsontable.PathLookup(edge.Data, field); val != nil {
				tx.Set(benchtop.FieldKey(field, edgeLabel, val, []byte(edge.ID)), []byte{}, nil)
			}
		}
	}
	return nil
}

func (ggraph *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(ggraph, GridsOptimizer, core.IndexStartOptimize)
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := insertVertex(tx, vert); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
				log.Errorf("AddVertex Error %s", err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})

	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, vert := range vertices {
			if err := ggraph.indexVertex(vert, tx); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
				log.Errorf("IndexVertex Error %s", err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (ggraph *Graph) AddEdge(edges []*gdbi.Edge) error {
	var err error = nil
	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		err = ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for _, edge := range edges {
				var fromLabel, toLabel []byte
				err = it.Seek(VertexKeyPrefix(edge.From))
				if err == nil {
					tmp := bytes.Split(it.Key(), []byte{0})
					fromLabel = tmp[2]
				}
				err = it.Seek(VertexKeyPrefix(edge.To))
				if err == nil {
					tmp := bytes.Split(it.Key(), []byte{0})
					toLabel = tmp[2]
				}
				err = insertEdge(tx, &completeEdge{OEdge: edge, FromLabel: fromLabel, ToLabel: toLabel})
				if err != nil {
					return err
				}
			}
			return err
		})
		ggraph.ts.Touch(ggraph.graphID)
		return err
	})
	if err != nil {
		return err
	}
	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		var bulkErr *multierror.Error
		for _, edge := range edges {
			if err := ggraph.indexEdge(edge, tx); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return bulkErr.ErrorOrNil()
	})
	return err

}

func (ggraph *Graph) BulkDel(data *gdbi.DeleteData) error {
	var bulkErr *multierror.Error
	for _, val := range data.Edges {
		err := ggraph.DelEdge(val)
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}
	for _, val := range data.Vertices {
		err := ggraph.DelVertex(val)
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}
	return bulkErr.ErrorOrNil()
}

type completeEdge struct {
	OEdge     *gdbi.Edge // Pointer to the original edge
	FromLabel []byte     // Looked up label for 'From'
	ToLabel   []byte     // Looked up label for 'To'
}

func (ggraph *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	var errs *multierror.Error
	insertStream := make(chan *gdbi.GraphElement, 100)
	indexStream := make(chan *benchtop.Row, 100)
	errChan := make(chan error, 2)

	var edgesToWrite []*completeEdge
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for elem := range insertStream {
				if elem.Vertex != nil {
					if err := insertVertex(tx, elem.Vertex); err != nil {
						return fmt.Errorf("vertex insert error: %v", err)
					}
				}
				if elem.Edge != nil {
					ce := &completeEdge{OEdge: elem.Edge}
					edgesToWrite = append(edgesToWrite, ce)
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("ERR in graph Bulk Add: %s", err)
			return
		}

		err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			for ce := range ggraph.bulkGet(edgesToWrite) {
				if err := insertEdge(tx, ce); err != nil {
					return fmt.Errorf("edge insert error: %v", err)
				}
			}
			ggraph.ts.Touch(ggraph.graphID)
			return nil
		})
		if err != nil {
			log.Errorf("ERR in BulkWrite: ", err)
			return
		}
	}()

	go func() {
		defer wg.Done()
		err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			if err := ggraph.bsonkv.BulkLoad(indexStream, tx); err != nil {
				return fmt.Errorf("bsonkv bulk load error: %v", err)
			}
			ggraph.ts.Touch(ggraph.graphID)
			return nil
		})
		errChan <- err
	}()

	go func() {
		defer func() {
			close(insertStream)
			close(indexStream)
		}()
		for elem := range stream {
			insertStream <- elem
			if elem.Vertex != nil {
				indexStream <- &benchtop.Row{
					Id:        []byte(elem.Vertex.ID),
					TableName: VTABLE_PREFIX + elem.Vertex.Label,
					Data:      elem.Vertex.Data,
				}
			}
			if elem.Edge != nil {
				indexStream <- &benchtop.Row{
					Id:        []byte(elem.Edge.ID),
					TableName: ETABLE_PREFIX + elem.Edge.Label,
					Data:      elem.Edge.Data,
				}
			}
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}

	// Return any accumulated errors
	return errs.ErrorOrNil()
}

func (ggraph *Graph) DelEdge(eid string) error {
	ekeyPrefix := EdgeKeyPrefix(eid)
	var ekey []byte
	ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})
	if ekey == nil {
		return fmt.Errorf("edge not found")
	}

	eid, sid, did, lbl := EdgeKeyParse(ekey)

	skey := SrcEdgeKeyPrefix(sid, did, eid, lbl)
	dkey := DstEdgeKeyPrefix(sid, did, eid, lbl)

	var bulkErr *multierror.Error
	err := ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(ekey, nil); err != nil {
			return err
		}
		if err := tx.DeletePrefix(skey); err != nil {
			return err
		}
		if err := tx.DeletePrefix(dkey); err != nil {
			return err
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	if err := ggraph.bsonkv.DeleteAnyRow([]byte(eid)); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	return bulkErr.ErrorOrNil()
}

// DelVertex deletes vertex with id `key`
func (ggraph *Graph) DelVertex(id string) error {

	vid := VertexKeyPrefix(id)
	skeyPrefix := SrcEdgePrefix(id)
	dkeyPrefix := DstEdgePrefix(id)

	delKeys := make([][]byte, 0, 1000)

	var bulkErr *multierror.Error

	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		var bulkErr *multierror.Error
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			// get edge ID from key
			eid, sid, did, label, _ := SrcEdgeKeyParse(skey)
			ekey := EdgeKey(eid, sid, did, label)
			dkey := DstEdgeKeyPrefix(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			if err := ggraph.bsonkv.DeleteAnyRow([]byte(eid)); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			// get edge ID from key
			eid, sid, did, label := DstEdgeKeyPrefixParse(dkey)
			ekey := EdgeKey(eid, sid, did, label)
			skey := SrcEdgeKeyPrefix(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			if err := ggraph.bsonkv.DeleteAnyRow([]byte(eid)); err != nil {
				bulkErr = multierror.Append(bulkErr, err)
			}
		}
		return bulkErr.ErrorOrNil()
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	err = ggraph.bsonkv.Pb.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.DeletePrefix(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k, nil); err != nil {
				return err
			}
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}
	return bulkErr.ErrorOrNil()
}

// GetEdgeList produces a channel of all edges in the graph
func (ggraph *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gdbi.Edge {
	o := make(chan *gdbi.Edge, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			ePrefix := EdgeListPrefix()
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				eid, sid, did, label := EdgeKeyParse(it.Key())
				e := &gdbi.Edge{ID: eid, Label: label, From: sid, To: did}
				if loadProp {
					var err error
					e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+label].GetRow([]byte(eid))
					if err != nil {
						log.Errorf("GetEdgeList: GetRow error: %v", err)
						continue
					}
					e.Loaded = true
				} else {
					e.Data = map[string]any{}
				}
				o <- e
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (ggraph *Graph) GetVertex(id string, loadProp bool) *gdbi.Vertex {
	var v *gdbi.Vertex
	rtasockey := benchtop.NewRowTableAsocKey([]byte(id))
	rtasocval, closer, err := ggraph.bsonkv.Pb.Db.Get(rtasockey)
	if err != nil {
		return nil
	}
	defer closer.Close()
	label := string(rtasocval)
	v = &gdbi.Vertex{
		ID:    id,
		Label: label[2:],
	}
	if loadProp {
		var err error
		v.Data, err = ggraph.bsonkv.Tables[label].GetRow([]byte(id))
		if err != nil {
			return nil
		}
		v.Loaded = true
	} else {
		v.Data = map[string]any{}
	}

	return v
}

type elementData struct {
	label string
	req   gdbi.ElementLookup
	data  []byte
}

func (ggraph *Graph) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for id := range ids {
				if id.IsSignal() {
					out <- id
				} else {
					if load {
						v := gdbi.Vertex{ID: id.ID}
						prefix := benchtop.NewRowTableAsocKey([]byte(id.ID))
						for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
							fetchLabel, err := it.Value()
							if err != nil {
								log.Errorf("GetVertexChannel: GetRow error for ID %s: %v", id.ID, err)
								continue
							}
							label := string(fetchLabel)
							v.Label = label[2:]
							v.Data, err = ggraph.bsonkv.Tables[label].GetRow([]byte(id.ID))
							if err != nil {
								log.Errorf("GetVertexChannel: GetRow error for ID %s: %v", id.ID, err)
								continue
							}
							v.Loaded = true
							break
						}
						id.Vertex = &v
						out <- id

					} else {
						id.Vertex = &gdbi.Vertex{ID: id.ID}
						out <- id
					}
				}
			}
			return nil
		})
	}()
	return out
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			var err error
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					skeyPrefix := SrcEdgePrefix(req.ID)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						_, _, dst, label, vLabel := SrcEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							v := &gdbi.Vertex{ID: dst, Label: vLabel}
							if load {
								v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+vLabel].GetRow([]byte(dst))
								if err != nil {
									log.Errorf("GetInChannel: GetRow error: %v", err)
									continue
								}
								v.Loaded = true
							} else {
								v.Data = map[string]any{}
							}
							req.Vertex = v
							o <- req
							found = true
						}
					}
					if !found && emitNull {
						req.Vertex = nil
						o <- req
					}

				}
			}
			return nil
		})
	}()

	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (ggraph *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					dkeyPrefix := DstEdgePrefix(req.ID)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						keyValue := it.Key()
						_, sid, _, label, vLabel := DstEdgeKeyParse(keyValue)
						log.Debugln("GET IN CHAN VLABEL: ", vLabel)
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							v := &gdbi.Vertex{ID: sid, Label: vLabel}
							if load {
								var err error

								v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+vLabel].GetRow([]byte(sid))
								if err != nil {
									log.Errorf("GetInChannel: GetRow error: %v", err)
									continue
								}
								v.Loaded = true
							} else {
								v.Data = map[string]any{}
							}
							req.Vertex = v
							o <- req
							found = true
						}
					}

					if !found && emitNull {
						req.Vertex = nil
						o <- req
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (ggraph *Graph) GetOutEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					skeyPrefix := SrcEdgePrefix(req.ID)
					for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
						eid, src, dst, label, _ := SrcEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							e := gdbi.Edge{
								From:  src,
								To:    dst,
								Label: label,
								ID:    eid,
							}
							if load {
								var err error

								e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+label].GetRow([]byte(e.ID))
								if err != nil {
									log.Errorf("GetOutEdgeChannel: GetRow error: %v", err)
									continue
								}
								e.Loaded = true
							} else {
								e.Data = map[string]any{}
							}
							req.Edge = &e
							o <- req
							found = true
						}
					}

					if !found && emitNull {
						req.Edge = nil
						o <- req
					}
				}
			}
			return nil
		})

	}()
	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (ggraph *Graph) GetInEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					found := false
					dkeyPrefix := DstEdgePrefix(req.ID)
					for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
						eid, src, dst, label, _ := DstEdgeKeyParse(it.Key())
						if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
							e := gdbi.Edge{
								ID:    eid,
								From:  src,
								To:    dst,
								Label: label,
							}
							if load {
								var err error
								e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+label].GetRow([]byte(e.ID))
								if err != nil {
									log.Errorf("GetInEdgeChannel: GetRow error: %v", err)
									continue
								}
								e.Loaded = true

							} else {
								e.Data = map[string]any{}
							}
							req.Edge = &e
							o <- req
							found = true
						}

					}
					if !found && emitNull {
						req.Edge = nil
						o <- req
					}
				}
			}
			return nil
		})

	}()
	return o
}

// GetEdge loads an edge given an id. It returns nil if not found
func (ggraph *Graph) GetEdge(id string, loadProp bool) *gdbi.Edge {
	ekeyPrefix := EdgeKeyPrefix(id)
	var e *gdbi.Edge
	err := ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			eid, src, dst, label := EdgeKeyParse(it.Key())
			e = &gdbi.Edge{
				ID:    eid,
				From:  src,
				To:    dst,
				Label: label,
			}

			if loadProp {
				var err error
				e.Data, err = ggraph.bsonkv.Tables[ETABLE_PREFIX+label].GetRow([]byte(id))
				if err != nil {
					log.Errorf("GetEdge: GetRow error: %v", err)
					continue
				}
				e.Loaded = true
			} else {
				e.Data = map[string]any{}
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return e
}

// GetVertexList produces a channel of all edges in the graph
func (ggraph *Graph) GetVertexList(ctx context.Context, loadProp bool) <-chan *gdbi.Vertex {
	o := make(chan *gdbi.Vertex, 100)
	go func() {
		defer close(o)
		ggraph.bsonkv.Pb.View(func(it *pebblebulk.PebbleIterator) error {
			vPrefix := VertexListPrefix()
			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}

				id, label := VertexKeyParse(it.Key())
				v := &gdbi.Vertex{
					ID:    id,
					Label: label,
				}
				if loadProp {
					var err error

					v.Data, err = ggraph.bsonkv.Tables[VTABLE_PREFIX+label].GetRow([]byte(v.ID))
					if err != nil {
						log.Errorf("GetVertexList: GetRow error: %v", err)
						continue
					}
					v.Loaded = true
				} else {
					v.Data = map[string]any{}
				}
				o <- v
			}
			return nil
		})
	}()
	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (ggraph *Graph) ListVertexLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.bsonkv.GetLabels(false) {
		labels = append(labels, i)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (ggraph *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for i := range ggraph.bsonkv.GetLabels(true) {
		labels = append(labels, i)
	}
	return labels, nil
}
