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

func insertVertex(tx *pebblebulk.PebbleBulk, id uint64, vertex *gdbi.Vertex, loc *benchtop.RowLoc) error {
	val := benchtop.EncodeVertexValue(vertex.Label, loc)
	if err := tx.Set(key.VertexKey(id), val, nil); err != nil {
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
				log.Debugf("Creating new table %s for label %s on graph %s", vertexLabel, label, ggraph.graphID)
				tStore, err = ggraph.driver.New(vertexLabel, nil)
				if err != nil {
					return fmt.Errorf("indexVertices: %s", err)
				}
			}
			table = tStore.(*driver.BackendTable)
		}

		rows := make([]benchtop.Row, len(verts))
		ids := make([]string, len(verts))
		for i, v := range verts {
			ids[i] = v.ID
			rows[i] = benchtop.Row{
				Id:      []byte(v.ID),
				Data:    v.Data,
				TableID: table.TableId,
			}
		}

		uids, err := ggraph.driver.GetIDs(ids)
		if err != nil {
			return err
		}

		rowLocs, err := table.AddRows(rows)
		if err != nil {
			return err
		}

		for i, v := range verts {
			if err := insertVertex(tx, uids[i], v, rowLocs[i]); err != nil {
				return err
			}
			// Index fields
			if len(table.Fields) > 0 {
				for field := range table.Fields {
					if val := tpath.PathLookup(v.Data, field); val != nil {
						err := tx.Set(benchtop.FieldKey(field, table.TableId, val, []byte(v.ID)), benchtop.EncodeRowLoc(rowLocs[i]), nil)
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

func insertEdge(tx *pebblebulk.PebbleBulk, eid, sid, did uint64, edge *gdbi.Edge, loc *benchtop.RowLoc) error {
	val := benchtop.EncodeEdgeValue(edge.Label, loc, edge.Data)
	if err := tx.Set(key.EdgeKey(eid, sid, did, edge.Label), val, nil); err != nil {
		return err
	}
	if err := tx.Set(key.DstEdgeKey(eid, sid, did, edge.Label), val, nil); err != nil {
		return err
	}
	if err := tx.Set(key.SrcEdgeKey(eid, sid, did, edge.Label), val, nil); err != nil {
		return err
	}
	return nil
}

func (ggraph *Graph) indexEdges(edges []*gdbi.Edge, tx *pebblebulk.PebbleBulk) error {
	byLabel := make(map[string][]*gdbi.Edge)
	for _, e := range edges {
		if e != nil {
			byLabel[e.Label] = append(byLabel[e.Label], e)
		}
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
		ids := make([]string, 0, len(batch)*3)
		for i, e := range batch {
			ids = append(ids, e.ID, e.From, e.To)
			data := make(map[string]any, len(e.Data)+2)
			for k, v := range e.Data {
				data[k] = v
			}
			data["_from"] = e.From
			data["_to"] = e.To
			rows[i] = benchtop.Row{
				Id:      []byte(e.ID),
				TableID: table.TableId,
				Data:    data,
			}
		}

		uids, err := ggraph.driver.GetIDs(ids)
		if err != nil {
			return err
		}

		locs, err := table.AddRows(rows)
		if err != nil {
			return fmt.Errorf("indexEdges: table.AddRows: %s", err)
		}

		for i, e := range batch {
			rowLoc := locs[i]
			eid, sid, did := uids[i*3], uids[i*3+1], uids[i*3+2]

			// Update the structural keys with the location AND inlined data
			if err := insertEdge(tx, eid, sid, did, e, rowLoc); err != nil {
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
		uid  uint64
		suid uint64
		duid uint64
	}

	const bufSize = 8192
	work := make(chan *gdbi.GraphElement, bufSize)
	ready := make(chan *preparedItem, bufSize)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(work)

		// ─── Worker Buffer & Batching ──────────────────────────
		const workerBatchSize = 1000
		batch := make([]*gdbi.GraphElement, 0, workerBatchSize)

		processBatch := func(b []*gdbi.GraphElement) error {
			if len(b) == 0 {
				return nil
			}

			// 1. Collect ALL unique IDs in this batch to resolve at once
			uniqueIDs := make(map[string]struct{})
			for _, elem := range b {
				if elem == nil {
					continue
				}
				if elem.Vertex != nil {
					uniqueIDs[elem.Vertex.ID] = struct{}{}
				} else if elem.Edge != nil {
					uniqueIDs[elem.Edge.ID] = struct{}{}
					uniqueIDs[elem.Edge.From] = struct{}{}
					uniqueIDs[elem.Edge.To] = struct{}{}
				}
			}

			// 2. Resolve IDs in bulk
			idList := make([]string, 0, len(uniqueIDs))
			for id := range uniqueIDs {
				idList = append(idList, id)
			}

			idVals, err := ggraph.driver.GetIDs(idList)
			if err != nil {
				return err
			}

			// 3. Map string -> uint64 for fast lookup
			idMap := make(map[string]uint64, len(idList))
			for i, s := range idList {
				idMap[s] = idVals[i]
			}

			// 4. Transform elements into preparedItems
			for _, elem := range b {
				if elem == nil {
					continue
				}

				// Determine table info
				var tName string
				if elem.Vertex != nil {
					tName = key.VertexTablePrefix + elem.Vertex.Label
				} else if elem.Edge != nil {
					tName = key.EdgeTablePrefix + elem.Edge.Label
				}

				var tid uint16
				if tName != "" {
					ts, err := ggraph.driver.GetOrLoadTable(tName)
					if err != nil {
						tStore, nerr := ggraph.driver.New(tName, nil)
						if nerr == nil && tStore != nil {
							if bt, ok := tStore.(*driver.BackendTable); ok {
								tid = bt.TableId
							}
						}
					} else if ts != nil {
						tid = ts.TableId
					}
				}

				var row *benchtop.Row
				var uid, suid, duid uint64
				if elem.Vertex != nil {
					uid = idMap[elem.Vertex.ID]
					row = &benchtop.Row{
						Id:      []byte(elem.Vertex.ID),
						TableID: tid,
						Data:    elem.Vertex.Data,
					}
				} else if elem.Edge != nil {
					uid = idMap[elem.Edge.ID]
					suid = idMap[elem.Edge.From]
					duid = idMap[elem.Edge.To]
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

				if row != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case ready <- &preparedItem{elem: elem, row: row, uid: uid, suid: suid, duid: duid}:
					}
				}
			}
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return
			case elem, ok := <-stream:
				if !ok {
					// Channel closed, flush remaining
					if len(batch) > 0 {
						_ = processBatch(batch)
					}
					return
				}
				batch = append(batch, elem)
				if len(batch) >= workerBatchSize {
					if err := processBatch(batch); err != nil {
						log.Errorf("BulkAdd worker error: %v", err)
						return
					}
					batch = batch[:0]
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer close(ready)

		for elem := range work {
			// This goroutine is now empty as its logic has been moved to the first goroutine.
			// It will just drain the 'work' channel and close 'ready'.
			// The actual work of preparing 'row' and resolving IDs is done in the first goroutine.
			// This goroutine can be removed or refactored if 'work' channel is no longer needed.
			// For now, keeping it to drain 'work' and close 'ready' as per original structure.
			_ = elem // Consume the element
		}
	}()

	// ─────────────────────────────────────────────
	// 3. Writer: Batching and I/O (Main thread)
	// ─────────────────────────────────────────────
	const batchSize = 1000
	itemBuffer := make([]*preparedItem, 0, batchSize)

	snap := ggraph.driver.Pkv.Db.NewSnapshot()
	defer snap.Close()

	// Use a shared iterator for the snapshot to avoid overhead
	it, err := snap.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()

	seen := make(map[string]struct{})

	processBatch := func(batch []*preparedItem) error {
		if len(batch) == 0 {
			return nil
		}

		return ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			filteredItems := make([]*preparedItem, 0, len(batch))
			for _, item := range batch {
				id := item.row.Id
				var dbKey []byte
				if item.elem.Vertex != nil {
					dbKey = key.VertexKey(item.uid)
				} else if item.elem.Edge != nil {
					dbKey = key.EdgeKey(item.uid, item.suid, item.duid, item.elem.Edge.Label)
				}

				if len(id) == 0 {
					continue
				}

				// 1. Session-level check
				if _, ok := seen[string(id)]; ok {
					continue
				}
				seen[string(id)] = struct{}{}

				// 2. Database-level check (Snapshot)
				if dbKey != nil {
					if it.SeekGE(dbKey) && bytes.Equal(it.Key(), dbKey) {
						continue
					}
				}
				filteredItems = append(filteredItems, item)
			}

			if len(filteredItems) == 0 {
				return nil
			}

			// Group rows for the driver
			rows := make([]*benchtop.Row, len(filteredItems))
			for i, item := range filteredItems {
				rows[i] = item.row
			}

			// Bulk Load JSON/Index rows (passing snap for further row-level filtering)
			if err := ggraph.driver.BulkLoadBatch(tx, rows, snap); err != nil {
				return err
			}
			return nil
		})
	}

	var writeErr error
	for item := range ready {
		itemBuffer = append(itemBuffer, item)
		if len(itemBuffer) >= batchSize {
			if err := processBatch(itemBuffer); err != nil {
				writeErr = err
				break
			}
			itemBuffer = itemBuffer[:0]
		}
	}

	if writeErr == nil && len(itemBuffer) > 0 {
		if err := processBatch(itemBuffer); err != nil {
			writeErr = err
		}
	}

	wg.Wait()
	ggraph.ts.Touch(ggraph.graphID)
	return writeErr
}
