package grids

import (
	"context"
	"fmt"
	"maps"
	"sort"
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

	snap := ggraph.driver.Pkv.Db.NewSnapshot()
	defer snap.Close()

	type preparedItem struct {
		elem  *gdbi.GraphElement
		row   *benchtop.Row
		uid   uint64
		suid  uint64
		duid  uint64
		dbKey []byte
	}

	const bufSize = 8192
	ready := make(chan *preparedItem, bufSize)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(ready)

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
			items := make([]*preparedItem, 0, len(b))

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
				var dbKey []byte

				if elem.Vertex != nil {
					uid = idMap[elem.Vertex.ID]
					dbKey = key.VertexKey(uid)
					row = &benchtop.Row{
						Id:      []byte(elem.Vertex.ID),
						TableID: tid,
						Data:    elem.Vertex.Data,
					}
				} else if elem.Edge != nil {
					uid = idMap[elem.Edge.ID]
					suid = idMap[elem.Edge.From]
					duid = idMap[elem.Edge.To]
					dbKey = key.EdgeKey(uid, suid, duid, elem.Edge.Label)
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
					items = append(items, &preparedItem{
						elem:  elem,
						row:   row,
						uid:   uid,
						suid:  suid,
						duid:  duid,
						dbKey: dbKey,
					})
				}
			}

			// 5. Sort items by UID to maximize Snapshot.Get locality (block cache efficiency)
			sort.Slice(items, func(i, j int) bool {
				return items[i].uid < items[j].uid
			})

			// 6. Check Snapshot and Emit
			for _, item := range items {
				if item.dbKey != nil {
					_, closer, err := snap.Get(item.dbKey)
					if err == nil {
						closer.Close()
						continue // Skip, graph element already exists
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ready <- item:
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

	// ─────────────────────────────────────────────
	// 3. Writer: Batching and I/O (Main thread)
	// ─────────────────────────────────────────────
	const batchSize = 1000
	itemBuffer := make([]*preparedItem, 0, batchSize)

	// Removed global snap and it, they will be created per batch.
	// snap := ggraph.driver.Pkv.Db.NewSnapshot()
	// defer snap.Close()
	//
	// it, err := snap.NewIter(nil)
	// if err != nil {
	// 	return err
	// }
	// defer it.Close()

	// Removed global 'seen' map.
	// seen := make(map[string]struct{})

	processBatch := func(batch []*preparedItem) error {
		if len(batch) == 0 {
			return nil
		}

		return ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
			// Group rows for the driver
			rows := make([]*benchtop.Row, len(batch))
			for i, item := range batch {
				rows[i] = item.row
			}

			// Bulk Load JSON/Index rows
			// Pass nil for snap to disable the redundant (and slower) check in the driver.
			// We have already verified uniqueness above using the optimized Get() check.
			if err := ggraph.driver.BulkLoadBatch(tx, rows, nil); err != nil {
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
