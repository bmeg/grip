package grids

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/driver"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
)

func insertVertex(tx *pebblebulk.PebbleBulk, vertex *gdbi.Vertex, loc *benchtop.RowLoc) error {
	if vertex.ID == "" {
		return fmt.Errorf("inserting null key vertex")
	}
	val := benchtop.EncodeVertexValue(vertex.Label, loc)
	if err := tx.Set(key.VertexKey(vertex.ID), val, nil); err != nil {
		return fmt.Errorf("AddVertex Error %s", err)
	}
	return nil
}

func (ggraph *Graph) indexVertices(vertices []*gdbi.Vertex, tx *pebblebulk.PebbleBulk) error {
	byLabel := make(map[string][]*gdbi.Vertex)
	for _, v := range vertices {
		byLabel[v.Label] = append(byLabel[v.Label], v)
	}

	for label, verts := range byLabel {
		vertexLabel := key.VertexTablePrefix + label
		tid, _ := ggraph.driver.TableDr.LookupTableID(vertexLabel)
		ggraph.driver.Lock.Lock()
		table, ok := ggraph.driver.TablesByID[tid]
		ggraph.driver.Lock.Unlock()

		if !ok {
			tStore, err := ggraph.driver.Get(vertexLabel)
			if err != nil {
				// Create new
				log.Debugf("Creating new table %s for label %s on graph %s", vertexLabel, label, ggraph.graphID)
				tStore, err = ggraph.driver.New(vertexLabel, nil)
				if err != nil {
					return fmt.Errorf("indexVertices: %s", err)
				}
			}
			table = tStore.(*driver.BackendTable)
		}

		rows := make([]benchtop.Row, len(verts))
		for i, v := range verts {
			rows[i] = benchtop.Row{
				Id:      []byte(v.ID),
				TableID: table.TableId,
				Data:    v.Data,
			}
		}

		locs, err := table.AddRows(rows)
		if err != nil {
			return fmt.Errorf("AddVertices Error %s", err)
		}

		if len(locs) != len(verts) {
			return fmt.Errorf("AddVertices returned %d locs for %d vertices", len(locs), len(verts))
		}

		for i, v := range verts {
			rowLoc := locs[i]
			// IMPORTANT: We still call AddTableEntryInfo for cache coherence,
			// but it's the Integrated structural key that is now authoritative.
			err = ggraph.driver.AddTableEntryInfo(tx, []byte(v.ID), rowLoc)
			if err != nil {
				return fmt.Errorf("AddVertex Error %s", err)
			}

			// Update the structural key with the location
			if err := insertVertex(tx, v, rowLoc); err != nil {
				return err
			}

			// Indices
			if len(table.Fields) > 0 {
				for field := range table.Fields {
					if val := tpath.PathLookup(v.Data, field); val != nil {
						err := tx.Set(benchtop.FieldKey(field, table.TableId, val, []byte(v.ID)), benchtop.EncodeRowLoc(rowLoc), nil)
						if err != nil {
							return err
						}
						Mval, err := sonic.ConfigFastest.Marshal(val)
						if err != nil {
							return err
						}
						err = tx.Set(benchtop.RFieldKey(table.TableId, field, v.ID), Mval, nil)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func insertEdge(tx *pebblebulk.PebbleBulk, edge *gdbi.Edge, loc *benchtop.RowLoc) error {
	if edge.ID == "" ||
		edge.From == "" ||
		edge.To == "" ||
		edge.Label == "" {
		log.Errorln("insertEdge Err: ", edge)
		return fmt.Errorf("inserting null key edge")
	}
	val := benchtop.EncodeEdgeValue(edge.Label, loc)
	err := tx.Set(key.EdgeKey(edge.ID, edge.From, edge.To, edge.Label), val, nil)
	if err != nil {
		return err
	}
	err = tx.Set(key.DstEdgeKey(
		edge.ID,
		edge.From,
		edge.To,
		edge.Label,
	), val, nil)
	if err != nil {
		return err
	}
	err = tx.Set(key.SrcEdgeKey(
		edge.ID,
		edge.From,
		edge.To,
		edge.Label,
	), val, nil)
	if err != nil {
		return err
	}
	return nil
}

func (ggraph *Graph) indexEdges(edges []*gdbi.Edge, tx *pebblebulk.PebbleBulk) error {
	byLabel := make(map[string][]*gdbi.Edge)
	for _, e := range edges {
		byLabel[e.Label] = append(byLabel[e.Label], e)
	}

	for label, batch := range byLabel {
		edgeLabel := key.EdgeTablePrefix + label
		tid, _ := ggraph.driver.TableDr.LookupTableID(edgeLabel)
		ggraph.driver.Lock.Lock()
		table, ok := ggraph.driver.TablesByID[tid]
		ggraph.driver.Lock.Unlock()

		if !ok {
			tStore, err := ggraph.driver.Get(edgeLabel)
			if err != nil {
				log.Debugf("Creating new table %s for edge label %s on graph %s", edgeLabel, label, ggraph.graphID)
				tStore, err = ggraph.driver.New(edgeLabel, nil)
				if err != nil {
					return fmt.Errorf("indexEdges: %s", err)
				}
			}
			table = tStore.(*driver.BackendTable)
		}

		rows := make([]benchtop.Row, len(batch))
		for i, e := range batch {
			rows[i] = benchtop.Row{
				Id:      []byte(e.ID),
				TableID: table.TableId,
				Data:    e.Data,
			}
		}

		locs, err := table.AddRows(rows)
		if err != nil {
			return fmt.Errorf("indexEdges: table.AddRows: %s", err)
		}

		for i, e := range batch {
			rowLoc := locs[i]
			err = ggraph.driver.AddTableEntryInfo(tx, []byte(e.ID), rowLoc)
			if err != nil {
				return fmt.Errorf("indexEdges: driver.AddTableEntryInfo: %s", err)
			}

			// Update the structural keys with the location
			if err := insertEdge(tx, e, rowLoc); err != nil {
				return err
			}

			if len(table.Fields) > 0 {
				for field := range table.Fields {
					if val := tpath.PathLookup(e.Data, field); val != nil {
						err := tx.Set(benchtop.FieldKey(field, table.TableId, val, []byte(e.ID)), benchtop.EncodeRowLoc(rowLoc), nil)
						if err != nil {
							return err
						}
						eMarsh, err := sonic.ConfigFastest.Marshal(val)
						if err != nil {
							return err
						}
						err = tx.Set(benchtop.RFieldKey(table.TableId, field, e.ID), eMarsh, nil)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (ggraph *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	// indexVertices now handles the authoritative integrated key write.
	return ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := ggraph.indexVertices(vertices, tx); err != nil {
			log.Errorf("IndexVertices Error %s", err)
			return err
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (ggraph *Graph) AddEdge(edges []*gdbi.Edge) error {
	// indexEdges now handles the authoritative integrated key write.
	return ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := ggraph.indexEdges(edges, tx); err != nil {
			log.Errorf("IndexEdges Error %s", err)
			return err
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})
}

func (ggraph *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type preparedItem struct {
		elem *gdbi.GraphElement
		row  *benchtop.Row
	}

	const bufSize = 8192
	work := make(chan *gdbi.GraphElement, bufSize)
	ready := make(chan *preparedItem, bufSize)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(work)
		for {
			select {
			case <-ctx.Done():
				return
			case elem, ok := <-stream:
				if !ok {
					return
				}
				if elem == nil {
					continue
				}

				// Move table creation/loading outside the writer loop
				var tName string
				if elem.Vertex != nil {
					tName = key.VertexTablePrefix + elem.Vertex.Label
				} else if elem.Edge != nil {
					tName = key.EdgeTablePrefix + elem.Edge.Label
				}

				if tName != "" {
					if _, err := ggraph.driver.GetOrLoadTable(tName); err != nil {
						if _, err := ggraph.driver.New(tName, nil); err != nil {
							log.Errorf("BulkAdd pre-warm failed for %s: %v", tName, err)
						}
					}
				}

				select {
				case <-ctx.Done():
					return
				case work <- elem:
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer close(ready)

		for elem := range work {
			var row *benchtop.Row
			if elem.Vertex != nil {
				tName := key.VertexTablePrefix + elem.Vertex.Label
				tid, err := ggraph.driver.TableDr.LookupTableID(tName)
				if err == nil {
					row = &benchtop.Row{
						Id:      []byte(elem.Vertex.ID),
						TableID: tid,
						Data:    elem.Vertex.Data,
					}
				}
			} else if elem.Edge != nil {
				tName := key.EdgeTablePrefix + elem.Edge.Label
				tid, err := ggraph.driver.TableDr.LookupTableID(tName)
				if err == nil {
					data := make(map[string]any, len(elem.Edge.Data)+2)
					maps.Copy(data, elem.Edge.Data)
					data["_from"] = elem.Edge.From
					data["_to"] = elem.Edge.To
					row = &benchtop.Row{
						Id:      []byte(elem.Edge.ID),
						TableID: tid,
						Data:    data,
					}
				}
			}

			if row != nil {
				select {
				case <-ctx.Done():
					return
				case ready <- &preparedItem{elem: elem, row: row}:
				}
			}
		}
	}()

	// ─────────────────────────────────────────────
	// 3. Writer: Batching and I/O (Main thread)
	// ─────────────────────────────────────────────
	const batchSize = 1000
	itemBuffer := make([]*preparedItem, 0, batchSize)

	snap := ggraph.driver.Pkv.Db.NewSnapshot()
	defer snap.Close()

	writeErr := ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		it, err := snap.NewIter(nil)
		if err != nil {
			return err
		}
		defer it.Close()

		seen := make(map[string]struct{}, bufSize)

		flush := func() error {
			if len(itemBuffer) == 0 {
				return nil
			}

			filteredItems := make([]*preparedItem, 0, len(itemBuffer))
			for _, item := range itemBuffer {
				id := ""
				var dbKey []byte
				if item.elem.Vertex != nil {
					id = item.elem.Vertex.ID
					dbKey = key.VertexKey(id)
				} else if item.elem.Edge != nil {
					id = item.elem.Edge.ID
					dbKey = key.EdgeKey(id, item.elem.Edge.From, item.elem.Edge.To, item.elem.Edge.Label)
				}

				if id == "" {
					continue
				}

				// 1. Session-level check
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}

				// 2. Database-level check (Snapshot)
				if dbKey != nil {
					if it.SeekGE(dbKey) && bytes.Equal(it.Key(), dbKey) {
						continue
					}
				}
				filteredItems = append(filteredItems, item)
			}

			if len(filteredItems) == 0 {
				itemBuffer = itemBuffer[:0]
				return nil
			}

			// Group rows for the driver
			rows := make([]*benchtop.Row, len(filteredItems))
			for i, item := range filteredItems {
				rows[i] = item.row
			}

			// Bulk Load JSON/Index rows (passing snap for further row-level filtering)
			// This will also update the structural Vertex/Edge keys with the found locations.
			if err := ggraph.driver.BulkLoadBatch(tx, rows, snap); err != nil {
				return err
			}

			itemBuffer = itemBuffer[:0]
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok := <-ready:
				if !ok {
					return flush()
				}
				itemBuffer = append(itemBuffer, item)
				if len(itemBuffer) >= batchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
	})

	wg.Wait()
	ggraph.ts.Touch(ggraph.graphID)
	return writeErr
}
