package grids

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/driver"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	multierror "github.com/hashicorp/go-multierror"
)

func (ggraph *Graph) DelVertex(id string) error {
	vid := key.VertexKey(id)
	skeyPrefix := key.SrcEdgePrefix(id)
	dkeyPrefix := key.DstEdgePrefix(id)

	delKeys := make([][]byte, 0, 1000)
	edgesToDelete := make(map[string]string)

	var bulkErr *multierror.Error
	err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			eid, sid, did, label := key.SrcEdgeKeyParse(skey)

			if ggraph.tempDeletedEdges != nil {
				if _, exists := ggraph.tempDeletedEdges[eid]; exists {
					continue
				}
			}
			if _, exists := edgesToDelete[eid]; exists {
				continue
			}

			ekey := key.EdgeKey(eid, sid, did, label)
			dkey := key.DstEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)
			edgesToDelete[eid] = label
		}

		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			eid, sid, did, label := key.DstEdgeKeyParse(dkey)

			if ggraph.tempDeletedEdges != nil {
				if _, exists := ggraph.tempDeletedEdges[eid]; exists {
					continue
				}
			}
			if _, exists := edgesToDelete[eid]; exists {
				continue
			}

			ekey := key.EdgeKey(eid, sid, did, label)
			skey := key.SrcEdgeKey(eid, sid, did, label)
			delKeys = append(delKeys, ekey, skey, dkey)
			edgesToDelete[eid] = label
		}
		return nil
	})

	if err != nil {
		return err
	}

	for eid, label := range edgesToDelete {
		if err := ggraph.DeleteAnyRow(eid, label, true); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}

		if ggraph.tempDeletedEdges != nil {
			ggraph.tempDeletedEdges[eid] = struct{}{}
		}
	}

	loc, err := ggraph.driver.LocCache.Get(context.Background(), id)
	if err != nil {
		return err
	}

	// Resolve table and label
	table, err := ggraph.driver.GetTableByID(loc.TableId)
	if err != nil {
		bulkErr = multierror.Append(bulkErr, fmt.Errorf("Failed to lookup table for ID %d: %v", loc.TableId, err))
		return bulkErr.ErrorOrNil()
	}
	label := table.Label
	if label == "" {
		bulkErr = multierror.Append(bulkErr, fmt.Errorf("Table label empty for TableId %d", loc.TableId))
		return bulkErr.ErrorOrNil()
	}
	if err := ggraph.DeleteAnyRow(id, label, false); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	err = ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.DeletePrefix(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.DeletePrefix(k); err != nil {
				log.Errorf("BulkWrite failed to delete key %s: %v", string(k), err)
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

func (ggraph *Graph) DelEdge(eid string) error {
	ekeyPrefix := key.EdgeKeyPrefix(eid)
	var ekey []byte
	err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = it.Key()
		}
		return nil
	})
	if err != nil {
		return err
	}

	if ekey == nil {
		log.Debugf("Edge %s not found", eid)
		return nil
	}

	_, sid, did, lbl := key.EdgeKeyParse(ekey)
	skey := key.SrcEdgeKey(eid, sid, did, lbl)
	dkey := key.DstEdgeKey(eid, sid, did, lbl)

	var bulkErr *multierror.Error
	err = ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(ekey, nil); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
		if err := tx.Delete(skey, nil); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
		if err := tx.Delete(dkey, nil); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
		ggraph.ts.Touch(ggraph.graphID)
		return nil
	})

	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	if err := ggraph.DeleteAnyRow(eid, lbl, true); err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	return bulkErr.ErrorOrNil()
}

// BulkDel deletes vertices and edges in bulk.
func (ggraph *Graph) BulkDel(data *gdbi.DeleteData) error {
	type keyBatch struct {
		singles [][]byte
		ranges  [][2][]byte
		posKeys [][]byte
	}

	type fieldInfo struct {
		field   string
		id      []byte
		rKey    []byte
		tableId uint16
	}

	type itemInfo struct {
		id      string
		label   string
		isEdge  bool
		tableId uint16
	}

	const shardSize = 64
	const bufferSize = 8192
	numCpus := runtime.NumCPU()
	ctx := context.Background()

	var bulkErr *multierror.Error
	addErr := func(err error) {
		if err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}

	// Sharded bitmap for edge deduplication (lock-free for reads)
	type shard struct {
		mu    sync.Mutex
		set   map[string]struct{}
		count uint32 // Atomic counter for seen edges
	}
	shards := make([]*shard, shardSize)
	for i := range shards {
		shards[i] = &shard{set: make(map[string]struct{}, bufferSize/shardSize)}
	}
	hasSeenEdge := func(eid string) bool {
		h := fnv32a(eid) % uint32(shardSize)
		shard := shards[h]
		shard.mu.Lock()
		defer shard.mu.Unlock()
		if _, exists := shard.set[eid]; exists {
			return true
		}
		shard.set[eid] = struct{}{}
		atomic.AddUint32(&shard.count, 1)
		return false
	}
	getSeenCount := func() uint64 {
		var total uint64
		for _, shard := range shards {
			total += uint64(atomic.LoadUint32(&shard.count))
		}
		return total
	}

	// Channels and wait groups
	itemChan := make(chan itemInfo, bufferSize)
	fieldChan := make(chan fieldInfo, bufferSize)
	keyChan := make(chan keyBatch, bufferSize)
	var prodWG, consWG, aggWG, fieldWG sync.WaitGroup

	// Aggregator for keys
	var singles [][]byte
	var ranges [][2][]byte
	var posKeys [][]byte
	aggWG.Add(1)
	go func() {
		defer aggWG.Done()
		for batch := range keyChan {
			select {
			case <-ctx.Done():
				return
			default:
				singles = append(singles, batch.singles...)
				ranges = append(ranges, batch.ranges...)
				posKeys = append(posKeys, batch.posKeys...)
			}
		}
	}()

	// Aggregator for fields
	var allFields []fieldInfo
	fieldWG.Add(1)
	go func() {
		defer fieldWG.Done()
		for fi := range fieldChan {
			allFields = append(allFields, fi)
		}
	}()

	// Workers for items
	consWG.Add(numCpus)
	for range numCpus {
		go func() {
			defer consWG.Done()
			localBatch := keyBatch{posKeys: make([][]byte, 0, bufferSize)}
			i := 0
			for item := range itemChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if i%100_000 == 0 && i != 0 {
					log.Debugf("[BulkDel worker] processed %d items", i)
				}
				i++

				// Fetch from page cache
				loc, err := ggraph.driver.LocCache.Get(ctx, item.id)
				if err != nil {
					if !errors.Is(err, driver.ErrNotFound) {
						addErr(err)
					}
					continue
				}

				// Resolve table and mark for deletion if it exists
				// Use GetOrLoadTable to handle case-sensitivity and on-demand loading
				// Resolve table from authoritative LocCache ID
				var table *driver.BackendTable
				ggraph.driver.Lock.RLock()
				table = ggraph.driver.TablesByID[loc.TableId]
				ggraph.driver.Lock.RUnlock()

				if table == nil {
					// Try to load
					if info, err := ggraph.driver.TableDr.GetTableInfo(loc.TableId); err == nil {
						table, _ = ggraph.driver.GetOrLoadTable(info.Name)
					}
				}

				hasTable := (table != nil)
				if hasTable && table.TableId != loc.TableId {
					// Should be impossible given we looked up by Loc.TableId
					log.Warningf("Logic error: table mismatch %d vs %d", table.TableId, loc.TableId)
				} else if !hasTable && item.tableId != 0 && item.tableId != loc.TableId {
					log.Warningf("index/row mismatch: index says %d, row says %d; using row", item.tableId, loc.TableId)
				}

				// Use authoritative ID
				currentTableId := loc.TableId

				// Position key matches the new P | TableId | rowID format
				localBatch.posKeys = append(localBatch.posKeys, benchtop.NewPosKey(currentTableId, []byte(item.id)))

				// Invalidate Grip's cache
				ggraph.driver.LocCache.Invalidate(item.id)

				// Invalidate Benchtop's table-aware cache
				ggraph.driver.TableDr.InvalidateLoc(currentTableId, item.id)

				// Deleting from the underlying table storage
				if hasTable {
					if err := table.DeleteRow(loc, []byte(item.id)); err != nil {
						addErr(fmt.Errorf("failed to delete row %s from table %s: %w", item.id, table.Name, err))
					}
				}

				// Send field infos
				if hasTable && len(table.Fields) > 0 {
					for field := range table.Fields {
						rKey := benchtop.RFieldKey(currentTableId, field, item.id)
						select {
						case fieldChan <- fieldInfo{rKey: rKey, field: field, tableId: currentTableId, id: []byte(item.id)}:
						case <-ctx.Done():
							return
						}
					}
				} else if !hasTable {
					// If table isn't loaded, we can't know which fields are indexed.
					// Scan the reverse index for all fields for this row and delete them.
					rPrefix := bytes.Join([][]byte{benchtop.RFieldPrefix, binary.LittleEndian.AppendUint16(nil, currentTableId)}, benchtop.FieldSep)
					err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
						for it.Seek(rPrefix); it.Valid() && bytes.HasPrefix(it.Key(), rPrefix); it.Next() {
							parts := bytes.Split(it.Key(), benchtop.FieldSep)
							if len(parts) >= 3 {
								field := string(parts[2])
								rKey := benchtop.RFieldKey(currentTableId, field, item.id)
								select {
								case fieldChan <- fieldInfo{rKey: rKey, field: field, tableId: currentTableId, id: []byte(item.id)}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
						}
						return nil
					})
					if err != nil {
						addErr(fmt.Errorf("failed to scan for orphan fields: %w", err))
					}
				}

				if len(localBatch.posKeys) >= 500_000 {
					keyChan <- localBatch
					localBatch = keyBatch{posKeys: make([][]byte, 0, bufferSize)}
				}
			}

			if len(localBatch.posKeys) > 0 {
				keyChan <- localBatch
			}
		}()
	}

	// Prepare vertex producers
	slices.Sort(data.Vertices)
	vertexSlices := make([][]string, numCpus)
	for i, vid := range data.Vertices {
		vertexSlices[i%numCpus] = append(vertexSlices[i%numCpus], vid)
	}
	for i := range vertexSlices {
		slices.Sort(vertexSlices[i])
	}

	for _, slice := range vertexSlices {
		if len(slice) == 0 {
			continue
		}
		prodWG.Add(1)
		go func(slice []string) {
			defer prodWG.Done()
			localBatch := keyBatch{singles: make([][]byte, 0, 256), ranges: make([][2][]byte, 0, 256)}

			err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, vid := range slice {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					sPrefix := key.SrcEdgePrefix(vid)
					if err := it.Seek(sPrefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), sPrefix) {
						nextPrefix := upperBound(sPrefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{sPrefix, nextPrefix})
						}
						for it.Valid() && bytes.HasPrefix(it.Key(), sPrefix) {
							eid, sid, did, lbl := key.SrcEdgeKeyParse(it.Key())
							if !hasSeenEdge(eid) {
								localBatch.singles = append(localBatch.singles,
									key.EdgeKey(eid, sid, did, lbl),
									bytes.Clone(it.Key()),
									key.DstEdgeKey(eid, sid, did, lbl))
								tid, _ := ggraph.driver.TableDr.LookupTableID("e_" + lbl)
								select {
								case itemChan <- itemInfo{id: eid, label: lbl, isEdge: true, tableId: tid}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
							it.Next()
						}
					}

					dPrefix := key.DstEdgePrefix(vid)
					if err := it.Seek(dPrefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), dPrefix) {
						nextPrefix := upperBound(dPrefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{dPrefix, nextPrefix})
						}
						for it.Valid() && bytes.HasPrefix(it.Key(), dPrefix) {
							eid, sid, did, lbl := key.DstEdgeKeyParse(it.Key())
							if !hasSeenEdge(eid) {
								localBatch.singles = append(localBatch.singles,
									key.EdgeKey(eid, sid, did, lbl),
									key.SrcEdgeKey(eid, sid, did, lbl),
									bytes.Clone(it.Key()))
								tid, _ := ggraph.driver.TableDr.LookupTableID("e_" + lbl)
								select {
								case itemChan <- itemInfo{id: eid, label: lbl, isEdge: true, tableId: tid}:
								case <-ctx.Done():
									return ctx.Err()
								}
							}
							it.Next()
						}
					}

					vkey := key.VertexKey(vid)
					if err := it.Seek(vkey); err != nil {
						return err
					}
					var label string
					if it.Valid() && bytes.Equal(it.Key(), vkey) {
						labelBytes, err := it.Value()
						if err != nil {
							return err
						}
						label = string(labelBytes)
					}
					localBatch.singles = append(localBatch.singles, vkey)
					if label != "" {
						tid, _ := ggraph.driver.TableDr.LookupTableID("v_" + label)
						select {
						case itemChan <- itemInfo{id: vid, label: label, isEdge: false, tableId: tid}:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				return nil
			})
			addErr(err)

			if len(localBatch.singles) > 0 || len(localBatch.ranges) > 0 {
				select {
				case keyChan <- localBatch:
				case <-ctx.Done():
				}
			}
		}(slice)
	}

	// Prepare edge producers
	slices.Sort(data.Edges)
	edgeSlices := make([][]string, numCpus)
	for i, eid := range data.Edges {
		edgeSlices[i%numCpus] = append(edgeSlices[i%numCpus], eid)
	}
	for i := range edgeSlices {
		slices.Sort(edgeSlices[i])
	}

	for _, slice := range edgeSlices {
		if len(slice) == 0 {
			continue
		}
		prodWG.Add(1)
		go func(slice []string) {
			defer prodWG.Done()
			localBatch := keyBatch{singles: make([][]byte, 0, 12), ranges: make([][2][]byte, 0, 12)}

			err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, eid := range slice {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					if hasSeenEdge(eid) {
						continue
					}

					prefix := key.EdgeKeyPrefix(eid)
					if err := it.Seek(prefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
						nextPrefix := upperBound(prefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{prefix, nextPrefix})
						}
						var label string
						for it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
							_, sid, did, lbl := key.EdgeKeyParse(it.Key())
							label = lbl
							localBatch.singles = append(localBatch.singles,
								key.SrcEdgeKey(eid, sid, did, lbl),
								key.DstEdgeKey(eid, sid, did, lbl))
							it.Next()
						}
						if label != "" {
							tid, _ := ggraph.driver.TableDr.LookupTableID("e_" + label)
							select {
							case itemChan <- itemInfo{id: eid, label: label, isEdge: true, tableId: tid}:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
					}
				}
				return nil
			})
			addErr(err)

			if len(localBatch.singles) > 0 || len(localBatch.ranges) > 0 {
				select {
				case keyChan <- localBatch:
				case <-ctx.Done():
				}
			}
		}(slice)
	}

	// Close channels and wait
	go func() {
		prodWG.Wait()
		close(itemChan)
	}()
	consWG.Wait()
	close(keyChan)
	aggWG.Wait()
	close(fieldChan)
	fieldWG.Wait()

	// Process field indices with single iterator
	var indexDelKeys [][]byte
	if len(allFields) > 0 {
		sort.Slice(allFields, func(i, j int) bool {
			return bytes.Compare(allFields[i].rKey, allFields[j].rKey) < 0
		})
		err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for _, fi := range allFields {
				if err := it.Seek(fi.rKey); err != nil {
					return err
				}
				if it.Valid() && bytes.Equal(it.Key(), fi.rKey) {
					valueBytes, err := it.Value()
					if err != nil {
						return err
					}
					var fieldValue any
					if err := sonic.ConfigFastest.Unmarshal(valueBytes, &fieldValue); err != nil {
						return err
					}
					if fieldValue != nil {
						fKey := benchtop.FieldKey(fi.field, fi.tableId, fieldValue, fi.id)
						indexDelKeys = append(indexDelKeys, fKey, fi.rKey)
					}
				}
			}
			return nil
		})
		addErr(err)
	}

	// Chunked deletes with Pebble batch
	chunked := func(singles [][]byte, ranges [][2][]byte, posKeys [][]byte, indexDelKeys [][]byte) error {
		batch := ggraph.driver.Pkv.Db.NewBatch()
		defer batch.Close()
		for _, k := range singles {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
		}
		for _, r := range ranges {
			if err := batch.DeleteRange(r[0], r[1], nil); err != nil {
				return err
			}
		}
		for _, k := range posKeys {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
		}
		for _, k := range indexDelKeys {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	}

	// Perform deletes
	ggraph.driver.PebbleLock.Lock()
	if err := chunked(singles, ranges, posKeys, indexDelKeys); err != nil {
		addErr(err)
	}
	ggraph.ts.Touch(ggraph.graphID)
	ggraph.driver.PebbleLock.Unlock()

	log.Debugf("Total edges seen: %d", getSeenCount())
	return bulkErr.ErrorOrNil()
}

// upperBound computes the tight upper bound for range delete
func upperBound(prefix []byte) []byte {
	ub := make([]byte, len(prefix))
	copy(ub, prefix)
	for i := len(ub) - 1; i >= 0; i-- {
		if ub[i] < 0xFF {
			ub[i]++
			return ub[:i+1]
		}
	}
	return nil
}

// fnv32a computes FNV-1a 32-bit hash
func fnv32a(s string) uint32 {
	var h uint32 = 2166136261
	for i := range s {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
