package grids

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	uid, _ := ggraph.driver.GetID(id)
	vid := key.VertexKey(uid)
	skeyPrefix := key.SrcEdgePrefix(uid)
	dkeyPrefix := key.DstEdgePrefix(uid)

	delKeys := make([][]byte, 0, 1000)
	type edgeDelInfo struct {
		label string
		loc   *benchtop.RowLoc
	}
	edgesToDelete := make(map[string]edgeDelInfo)

	var bulkErr *multierror.Error
	err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
			skey := it.Key()
			euid, suid, duid, label := key.SrcEdgeKeyParse(skey)
			eid, _ := ggraph.driver.TranslateID(euid)

			if ggraph.tempDeletedEdges != nil {
				if _, exists := ggraph.tempDeletedEdges[eid]; exists {
					continue
				}
			}
			if _, exists := edgesToDelete[eid]; exists {
				continue
			}

			ekey := key.EdgeKey(euid, suid, duid, label)
			dkey := key.DstEdgeKey(euid, suid, duid, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			eVal, _ := it.Value()
			_, loc, _ := benchtop.DecodeEdgeValue(eVal)
			edgesToDelete[eid] = edgeDelInfo{label: label, loc: loc}
		}

		for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
			dkey := it.Key()
			euid, suid, duid, label := key.DstEdgeKeyParse(dkey)
			eid, _ := ggraph.driver.TranslateID(euid)

			if ggraph.tempDeletedEdges != nil {
				if _, exists := ggraph.tempDeletedEdges[eid]; exists {
					continue
				}
			}
			if _, exists := edgesToDelete[eid]; exists {
				continue
			}

			ekey := key.EdgeKey(euid, suid, duid, label)
			skey := key.SrcEdgeKey(euid, suid, duid, label)
			delKeys = append(delKeys, ekey, skey, dkey)

			eVal, _ := it.Value()
			_, loc, _ := benchtop.DecodeEdgeValue(eVal)
			edgesToDelete[eid] = edgeDelInfo{label: label, loc: loc}
		}
		return nil
	})

	if err != nil {
		return err
	}

	for eid, info := range edgesToDelete {
		if err := ggraph.DeleteAnyRow(eid, info.label, true, info.loc); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}

		if ggraph.tempDeletedEdges != nil {
			ggraph.tempDeletedEdges[eid] = struct{}{}
		}
	}

	var vlbl string
	var vloc *benchtop.RowLoc
	_ = ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		val, err := it.Get(vid)
		if err == nil {
			vlbl, vloc = benchtop.DecodeVertexValue(val)
		}
		return nil
	})

	if vloc != nil {
		if err := ggraph.DeleteAnyRow(id, vlbl, false, vloc); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
	}

	err = ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(vid, nil); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	return bulkErr.ErrorOrNil()
}

func (ggraph *Graph) DelEdge(eid string) error {
	uid, _ := ggraph.driver.GetID(eid)
	ekeyPrefix := key.EdgeKeyPrefix(uid)
	var ekey []byte
	var eVal []byte
	err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
			ekey = bytes.Clone(it.Key())
			v, _ := it.Value()
			eVal = bytes.Clone(v)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if ekey == nil {
		return nil
	}

	euid, suid, duid, lbl := key.EdgeKeyParse(ekey)
	skey := key.SrcEdgeKey(euid, suid, duid, lbl)
	dkey := key.DstEdgeKey(euid, suid, duid, lbl)

	var bulkErr *multierror.Error
	err = ggraph.driver.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		tx.Delete(ekey, nil)
		tx.Delete(skey, nil)
		tx.Delete(dkey, nil)
		return nil
	})

	if err != nil {
		bulkErr = multierror.Append(bulkErr, err)
	}

	_, loc, _ := benchtop.DecodeEdgeValue(eVal)
	if loc != nil {
		if err := ggraph.DeleteAnyRow(eid, lbl, true, loc); err != nil {
			bulkErr = multierror.Append(bulkErr, err)
		}
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
		loc     *benchtop.RowLoc
	}
	type rowDeleteTask struct {
		table *driver.BackendTable
		id    string
		loc   *benchtop.RowLoc
	}

	const shardSize = 64
	const bufferSize = 8192
	numCpus := runtime.NumCPU()
	ctx := context.Background()
	start := time.Now()
	slices.Sort(data.Vertices)
	slices.Sort(data.Edges)
	log.Infof("BulkDel start graph=%s vertices=%d edges=%d", ggraph.graphID, len(data.Vertices), len(data.Edges))
	var stageMu sync.RWMutex
	stage := "enumerate"
	setStage := func(s string) {
		stageMu.Lock()
		stage = s
		stageMu.Unlock()
	}
	getStage := func() string {
		stageMu.RLock()
		defer stageMu.RUnlock()
		return stage
	}
	inlineRowGC := strings.EqualFold(strings.TrimSpace(os.Getenv("GRIDS_BULK_DELETE_ROW_GC_MODE")), "inline")
	if inlineRowGC {
		log.Infof("BulkDel row GC mode graph=%s mode=inline", ggraph.graphID)
	} else {
		log.Infof("BulkDel row GC mode graph=%s mode=deferred", ggraph.graphID)
	}
	verboseBulkDel := envTruthy("GRIDS_BULK_DELETE_DEBUG")
	stallStackDump := envTruthy("GRIDS_BULK_DELETE_STALL_STACK")
	bulkDelLogf := func(format string, args ...any) {
		if verboseBulkDel {
			log.Infof(format, args...)
			return
		}
		log.Debugf(format, args...)
	}

	var bulkErr *multierror.Error
	var bulkErrMu sync.Mutex
	var missingRowsMu sync.Mutex
	missingRowsByTable := map[uint16]map[string]struct{}{}
	var producerRuns int64
	var itemQueued int64
	var itemProcessed int64
	var keyBatchQueued int64
	var keyBatchProcessed int64
	var keySinglesProcessed int64
	var keyRangesProcessed int64
	var keyPosProcessed int64
	var fieldQueued int64
	var fieldProcessed int64
	var missingRowTargets int64
	var unknownTableScans int64
	var rowDeleteQueued int64
	var rowDeleteDone int64
	var rowDeleteInFlight int64
	var rowDeleteErr int64
	var rowDeleteSections int64
	var workersInLookup int64
	var workersInTableLoad int64
	var workersInFieldEmit int64
	// Channels and wait groups
	itemChan := make(chan itemInfo, bufferSize)
	fieldChan := make(chan fieldInfo, bufferSize)
	keyChan := make(chan keyBatch, bufferSize)
	var prodWG, consWG, aggWG, fieldWG sync.WaitGroup
	var rowDeleteTasksMu sync.Mutex
	rowDeleteTasks := make([]rowDeleteTask, 0, 1024)

	progressStop := make(chan struct{})
	defer close(progressStop)
	addErr := func(err error) {
		if err != nil {
			bulkErrMu.Lock()
			bulkErr = multierror.Append(bulkErr, err)
			bulkErrMu.Unlock()
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
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		var prevItems int64
		var prevBatches int64
		var prevFields int64
		stallTicks := 0
		for {
			select {
			case <-progressStop:
				return
			case <-ticker.C:
				curItems := atomic.LoadInt64(&itemProcessed)
				curBatches := atomic.LoadInt64(&keyBatchProcessed)
				curFields := atomic.LoadInt64(&fieldProcessed)
				if curItems == prevItems && curBatches == prevBatches && curFields == prevFields && atomic.LoadInt64(&producerRuns) > 0 {
					stallTicks++
				} else {
					stallTicks = 0
				}
				prevItems = curItems
				prevBatches = curBatches
				prevFields = curFields

				curStage := getStage()

				bulkDelLogf(
					"BulkDel progress graph=%s stage=%s elapsed=%s producers=%d items=%d/%d itemBacklog=%d keyBatches=%d/%d keyParts[s=%d r=%d p=%d] fields=%d/%d rowDelete[sections=%d q=%d done=%d inFlight=%d err=%d] missingRows=%d unknownScans=%d seenEdges=%d workerState[lookup=%d load=%d emit=%d] chanDepth[item=%d/%d key=%d/%d field=%d/%d] stallTicks=%d",
					ggraph.graphID,
					curStage,
					time.Since(start).Round(time.Second),
					atomic.LoadInt64(&producerRuns),
					curItems,
					atomic.LoadInt64(&itemQueued),
					atomic.LoadInt64(&itemQueued)-curItems,
					atomic.LoadInt64(&keyBatchProcessed),
					atomic.LoadInt64(&keyBatchQueued),
					atomic.LoadInt64(&keySinglesProcessed),
					atomic.LoadInt64(&keyRangesProcessed),
					atomic.LoadInt64(&keyPosProcessed),
					atomic.LoadInt64(&fieldQueued),
					curFields,
					atomic.LoadInt64(&rowDeleteSections),
					atomic.LoadInt64(&rowDeleteQueued),
					atomic.LoadInt64(&rowDeleteDone),
					atomic.LoadInt64(&rowDeleteInFlight),
					atomic.LoadInt64(&rowDeleteErr),
					atomic.LoadInt64(&missingRowTargets),
					atomic.LoadInt64(&unknownTableScans),
					getSeenCount(),
					atomic.LoadInt64(&workersInLookup),
					atomic.LoadInt64(&workersInTableLoad),
					atomic.LoadInt64(&workersInFieldEmit),
					len(itemChan),
					cap(itemChan),
					len(keyChan),
					cap(keyChan),
					len(fieldChan),
					cap(fieldChan),
					stallTicks,
				)
				if stallTicks >= 3 {
					log.Warningf(
						"BulkDel appears stalled graph=%s stage=%s stallTicks=%d producers=%d backlog=%d rowDelete[sections=%d q=%d done=%d inFlight=%d err=%d] workerState[lookup=%d load=%d emit=%d] chanDepth[item=%d/%d key=%d/%d field=%d/%d]",
						ggraph.graphID,
						curStage,
						stallTicks,
						atomic.LoadInt64(&producerRuns),
						atomic.LoadInt64(&itemQueued)-curItems,
						atomic.LoadInt64(&rowDeleteSections),
						atomic.LoadInt64(&rowDeleteQueued),
						atomic.LoadInt64(&rowDeleteDone),
						atomic.LoadInt64(&rowDeleteInFlight),
						atomic.LoadInt64(&rowDeleteErr),
						atomic.LoadInt64(&workersInLookup),
						atomic.LoadInt64(&workersInTableLoad),
						atomic.LoadInt64(&workersInFieldEmit),
						len(itemChan),
						cap(itemChan),
						len(keyChan),
						cap(keyChan),
						len(fieldChan),
						cap(fieldChan),
					)
					if stallStackDump && (stallTicks == 3 || stallTicks%6 == 0) {
						buf := make([]byte, 1<<20)
						n := runtime.Stack(buf, true)
						log.Warningf("BulkDel stall goroutine dump graph=%s stallTicks=%d\n%s", ggraph.graphID, stallTicks, string(buf[:n]))
					}
				}
			}
		}
	}()

	// Aggregator for keys
	var singles [][]byte
	var ranges [][2][]byte
	var posKeys [][]byte
	aggWG.Add(1)
	go func() {
		defer aggWG.Done()
		for batch := range keyChan {
			atomic.AddInt64(&keyBatchProcessed, 1)
			atomic.AddInt64(&keySinglesProcessed, int64(len(batch.singles)))
			atomic.AddInt64(&keyRangesProcessed, int64(len(batch.ranges)))
			atomic.AddInt64(&keyPosProcessed, int64(len(batch.posKeys)))
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
			atomic.AddInt64(&fieldProcessed, 1)
		}
	}()

	enqueueItem := func(item itemInfo, source string) error {
		const warnEvery = 10 * time.Second
		timer := time.NewTimer(warnEvery)
		defer timer.Stop()
		for {
			select {
			case itemChan <- item:
				atomic.AddInt64(&itemQueued, 1)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				log.Warningf(
					"BulkDel blocked enqueue item graph=%s source=%s id=%s label=%s isEdge=%t chanDepth=%d/%d backlog=%d workerState[lookup=%d load=%d emit=%d]",
					ggraph.graphID,
					source,
					item.id,
					item.label,
					item.isEdge,
					len(itemChan),
					cap(itemChan),
					atomic.LoadInt64(&itemQueued)-atomic.LoadInt64(&itemProcessed),
					atomic.LoadInt64(&workersInLookup),
					atomic.LoadInt64(&workersInTableLoad),
					atomic.LoadInt64(&workersInFieldEmit),
				)
				timer.Reset(warnEvery)
			}
		}
	}

	enqueueField := func(fi fieldInfo, source string) error {
		const warnEvery = 10 * time.Second
		timer := time.NewTimer(warnEvery)
		defer timer.Stop()
		for {
			select {
			case fieldChan <- fi:
				atomic.AddInt64(&fieldQueued, 1)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				log.Warningf(
					"BulkDel blocked enqueue field graph=%s source=%s tableID=%d field=%s chanDepth=%d/%d backlog=%d",
					ggraph.graphID,
					source,
					fi.tableId,
					fi.field,
					len(fieldChan),
					cap(fieldChan),
					atomic.LoadInt64(&fieldQueued)-atomic.LoadInt64(&fieldProcessed),
				)
				timer.Reset(warnEvery)
			}
		}
	}

	enqueueKeyBatch := func(batch keyBatch, source string) error {
		if len(batch.singles) == 0 && len(batch.ranges) == 0 && len(batch.posKeys) == 0 {
			return nil
		}
		const warnEvery = 10 * time.Second
		timer := time.NewTimer(warnEvery)
		defer timer.Stop()
		for {
			select {
			case keyChan <- batch:
				atomic.AddInt64(&keyBatchQueued, 1)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				log.Warningf(
					"BulkDel blocked enqueue keyBatch graph=%s source=%s parts[s=%d r=%d p=%d] chanDepth=%d/%d backlog=%d",
					ggraph.graphID,
					source,
					len(batch.singles),
					len(batch.ranges),
					len(batch.posKeys),
					len(keyChan),
					cap(keyChan),
					atomic.LoadInt64(&keyBatchQueued)-atomic.LoadInt64(&keyBatchProcessed),
				)
				timer.Reset(warnEvery)
			}
		}
	}

	// Workers for items
	consWG.Add(numCpus)
	for range numCpus {
		go func() {
			defer consWG.Done()
			localBatch := keyBatch{posKeys: make([][]byte, 0, bufferSize)}
			i := 0
			for item := range itemChan {
				atomic.AddInt64(&itemProcessed, 1)
				select {
				case <-ctx.Done():
					return
				default:
				}
				if i%100_000 == 0 && i != 0 {
					bulkDelLogf("[BulkDel worker] processed %d items", i)
				}
				i++

				// Use the RowLoc passed in itemInfo
				loc := item.loc
				if loc == nil {
					continue
				}

				// Resolve table and mark for deletion if it exists
				var table *driver.BackendTable
				atomic.AddInt64(&workersInLookup, 1)
				ggraph.driver.Lock.RLock()
				table = ggraph.driver.TablesByID[loc.TableId]
				ggraph.driver.Lock.RUnlock()
				atomic.AddInt64(&workersInLookup, -1)

				if table == nil {
					atomic.AddInt64(&workersInTableLoad, 1)
					// Try to load
					if info, err := ggraph.driver.TableDr.GetTableInfo(loc.TableId); err == nil {
						table, _ = ggraph.driver.GetOrLoadTable(info.Name)
					}
					atomic.AddInt64(&workersInTableLoad, -1)
				}

				hasTable := (table != nil)
				if hasTable && table.TableId != loc.TableId {
					log.Warningf("Logic error: table mismatch %d vs %d", table.TableId, loc.TableId)
				}

				// Use authoritative ID
				currentTableId := loc.TableId

				// Position key matches the new P | TableId | rowID format
				localBatch.posKeys = append(localBatch.posKeys, benchtop.NewPosKey(currentTableId, []byte(item.id)))

				// Deleting from the underlying table storage (dedicated stage for reliability)
				if hasTable && inlineRowGC {
					rowDeleteTasksMu.Lock()
					rowDeleteTasks = append(rowDeleteTasks, rowDeleteTask{table: table, id: item.id, loc: loc})
					rowDeleteTasksMu.Unlock()
					atomic.AddInt64(&rowDeleteQueued, 1)
				}

				// Send field infos
				if hasTable && len(table.Fields) > 0 {
					atomic.AddInt64(&workersInFieldEmit, 1)
					for field := range table.Fields {
						rKey := benchtop.RFieldKey(currentTableId, field, item.id)
						if err := enqueueField(fieldInfo{rKey: rKey, field: field, tableId: currentTableId, id: []byte(item.id)}, "item_worker"); err != nil {
							atomic.AddInt64(&workersInFieldEmit, -1)
							return
						}
					}
					atomic.AddInt64(&workersInFieldEmit, -1)
				} else if !hasTable {
					// Avoid O(rows * tableFields) scans: queue missing table+row and resolve
					// fields in one batched reverse-index scan per table after workers finish.
					missingRowsMu.Lock()
					if _, ok := missingRowsByTable[currentTableId]; !ok {
						missingRowsByTable[currentTableId] = map[string]struct{}{}
					}
					if _, exists := missingRowsByTable[currentTableId][item.id]; !exists {
						missingRowsByTable[currentTableId][item.id] = struct{}{}
						atomic.AddInt64(&missingRowTargets, 1)
					}
					missingRowsMu.Unlock()
				}

				if len(localBatch.posKeys) >= 500_000 {
					if err := enqueueKeyBatch(localBatch, "item_worker"); err != nil {
						return
					}
					localBatch = keyBatch{posKeys: make([][]byte, 0, bufferSize)}
				}
			}

			if len(localBatch.posKeys) > 0 {
				if err := enqueueKeyBatch(localBatch, "item_worker_flush"); err != nil {
					return
				}
			}
		}()
	}

	// Prepare vertex producers
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
			atomic.AddInt64(&producerRuns, 1)
			defer atomic.AddInt64(&producerRuns, -1)
			localBatch := keyBatch{singles: make([][]byte, 0, 256), ranges: make([][2][]byte, 0, 256)}

			err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for _, vid := range slice {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					uid, _ := ggraph.driver.GetID(vid)
					sPrefix := key.SrcEdgePrefix(uid)
					if err := it.Seek(sPrefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), sPrefix) {
						nextPrefix := upperBound(sPrefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{sPrefix, nextPrefix})
						}
						for it.Valid() && bytes.HasPrefix(it.Key(), sPrefix) {
							euid, suid, duid, lbl := key.SrcEdgeKeyParse(it.Key())
							eid, _ := ggraph.driver.TranslateID(euid)
							eVal, _ := it.Value()
							_, loc, _ := benchtop.DecodeEdgeValue(eVal)
							if !hasSeenEdge(eid) {
								localBatch.singles = append(localBatch.singles,
									key.EdgeKey(euid, suid, duid, lbl),
									bytes.Clone(it.Key()),
									key.DstEdgeKey(euid, suid, duid, lbl))
								tid, _ := ggraph.driver.TableDr.LookupTableID("e_" + lbl)
								if err := enqueueItem(itemInfo{id: eid, label: lbl, isEdge: true, tableId: tid, loc: loc}, "vertex_src_edge"); err != nil {
									return err
								}
							}
							it.Next()
						}
					}

					dPrefix := key.DstEdgePrefix(uid)
					if err := it.Seek(dPrefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), dPrefix) {
						nextPrefix := upperBound(dPrefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{dPrefix, nextPrefix})
						}
						for it.Valid() && bytes.HasPrefix(it.Key(), dPrefix) {
							euid, suid, duid, lbl := key.DstEdgeKeyParse(it.Key())
							eid, _ := ggraph.driver.TranslateID(euid)
							eVal, _ := it.Value()
							_, loc, _ := benchtop.DecodeEdgeValue(eVal)
							if !hasSeenEdge(eid) {
								localBatch.singles = append(localBatch.singles,
									key.EdgeKey(euid, suid, duid, lbl),
									key.SrcEdgeKey(euid, suid, duid, lbl),
									bytes.Clone(it.Key()))
								tid, _ := ggraph.driver.TableDr.LookupTableID("e_" + lbl)
								if err := enqueueItem(itemInfo{id: eid, label: lbl, isEdge: true, tableId: tid, loc: loc}, "vertex_dst_edge"); err != nil {
									return err
								}
							}
							it.Next()
						}
					}

					vkey := key.VertexKey(uid)
					if err := it.Seek(vkey); err != nil {
						return err
					}
					var vlabel string
					var vloc *benchtop.RowLoc
					if it.Valid() && bytes.Equal(it.Key(), vkey) {
						vBytes, err := it.Value()
						if err != nil {
							return err
						}
						vlabel, vloc = benchtop.DecodeVertexValue(vBytes)
					}
					localBatch.singles = append(localBatch.singles, vkey)
					if vlabel != "" {
						tid, _ := ggraph.driver.TableDr.LookupTableID("v_" + vlabel)
						if err := enqueueItem(itemInfo{id: vid, label: vlabel, isEdge: false, tableId: tid, loc: vloc}, "vertex_record"); err != nil {
							return err
						}
					}
				}
				return nil
			})
			addErr(err)

			if len(localBatch.singles) > 0 || len(localBatch.ranges) > 0 {
				if err := enqueueKeyBatch(localBatch, "vertex_producer_flush"); err != nil {
					addErr(err)
				}
			}
		}(slice)
	}

	// Prepare edge producers
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
			atomic.AddInt64(&producerRuns, 1)
			defer atomic.AddInt64(&producerRuns, -1)
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

					uid, _ := ggraph.driver.GetID(eid)
					prefix := key.EdgeKeyPrefix(uid)
					if err := it.Seek(prefix); err != nil {
						return err
					}
					if it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
						nextPrefix := upperBound(prefix)
						if nextPrefix != nil {
							localBatch.ranges = append(localBatch.ranges, [2][]byte{prefix, nextPrefix})
						}
						var eLabel string
						var eLoc *benchtop.RowLoc
						for it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
							euid, suid, duid, lbl := key.EdgeKeyParse(it.Key())
							eLabel = lbl
							eVal, _ := it.Value()
							_, eLoc, _ = benchtop.DecodeEdgeValue(eVal)
							localBatch.singles = append(localBatch.singles,
								key.SrcEdgeKey(euid, suid, duid, lbl),
								key.DstEdgeKey(euid, suid, duid, lbl))
							it.Next()
						}
						if eLabel != "" {
							tid, _ := ggraph.driver.TableDr.LookupTableID("e_" + eLabel)
							if err := enqueueItem(itemInfo{id: eid, label: eLabel, isEdge: true, tableId: tid, loc: eLoc}, "edge_producer"); err != nil {
								return err
							}
						}
					}
				}
				return nil
			})
			addErr(err)

			if len(localBatch.singles) > 0 || len(localBatch.ranges) > 0 {
				if err := enqueueKeyBatch(localBatch, "edge_producer_flush"); err != nil {
					addErr(err)
				}
			}
		}(slice)
	}

	// Close channels and wait
	go func() {
		prodWG.Wait()
		bulkDelLogf("BulkDel producers complete graph=%s queuedItems=%d queuedKeyBatches=%d", ggraph.graphID, atomic.LoadInt64(&itemQueued), atomic.LoadInt64(&keyBatchQueued))
		close(itemChan)
	}()
	bulkDelLogf("BulkDel waiting workers graph=%s", ggraph.graphID)
	consWG.Wait()
	bulkDelLogf("BulkDel item workers complete graph=%s processedItems=%d", ggraph.graphID, atomic.LoadInt64(&itemProcessed))
	close(keyChan)
	aggWG.Wait()
	bulkDelLogf("BulkDel key aggregation complete graph=%s singles=%d ranges=%d posKeys=%d", ggraph.graphID, len(singles), len(ranges), len(posKeys))

	// Phase 2: row tombstones (single writer per section for reliability).
	deleteTasks := make([]rowDeleteTask, 0)
	if inlineRowGC {
		setStage("row_delete")
		rowDeleteTasksMu.Lock()
		deleteTasks = append(deleteTasks, rowDeleteTasks...)
		rowDeleteTasksMu.Unlock()
		sort.Slice(deleteTasks, func(i, j int) bool {
			li := deleteTasks[i].loc
			lj := deleteTasks[j].loc
			if li.Section != lj.Section {
				return li.Section < lj.Section
			}
			if deleteTasks[i].table.TableId != deleteTasks[j].table.TableId {
				return deleteTasks[i].table.TableId < deleteTasks[j].table.TableId
			}
			return deleteTasks[i].id < deleteTasks[j].id
		})

		resumeFrom := 0
		atomic.StoreInt64(&rowDeleteQueued, int64(len(deleteTasks)))
		atomic.StoreInt64(&rowDeleteDone, int64(resumeFrom))

		var rowDeleteWG sync.WaitGroup
		var rowDeleteMu sync.Mutex
		rowDeleteBySection := make(map[uint16]chan rowDeleteTask)
		dispatchRowDelete := func(task rowDeleteTask) bool {
			sectionID := task.loc.Section
			rowDeleteMu.Lock()
			ch, ok := rowDeleteBySection[sectionID]
			if !ok {
				ch = make(chan rowDeleteTask, 1024)
				rowDeleteBySection[sectionID] = ch
				atomic.AddInt64(&rowDeleteSections, 1)
				rowDeleteWG.Add(1)
				go func(sectionID uint16, taskCh chan rowDeleteTask) {
					defer rowDeleteWG.Done()
					for task := range taskCh {
						atomic.AddInt64(&rowDeleteInFlight, 1)
						ggraph.driver.TableDr.InvalidateLoc(task.loc.TableId, task.id)
						t0 := time.Now()
						if err := task.table.DeleteRow(task.loc, []byte(task.id)); err != nil {
							atomic.AddInt64(&rowDeleteErr, 1)
							addErr(fmt.Errorf("failed to delete row %s from table %s section=%d: %w", task.id, task.table.Name, sectionID, err))
						}
						if d := time.Since(t0); d > 3*time.Second {
							log.Warningf("BulkDel slow row tombstone graph=%s section=%d table=%s id=%s duration=%s", ggraph.graphID, sectionID, task.table.Name, task.id, d)
						}
						atomic.AddInt64(&rowDeleteDone, 1)
						atomic.AddInt64(&rowDeleteInFlight, -1)
					}
				}(sectionID, ch)
			}
			rowDeleteMu.Unlock()
			const warnEvery = 10 * time.Second
			timer := time.NewTimer(warnEvery)
			defer timer.Stop()
			for {
				select {
				case ch <- task:
					return true
				case <-ctx.Done():
					return false
				case <-timer.C:
					log.Warningf(
						"BulkDel blocked row tombstone dispatch graph=%s section=%d chanDepth=%d/%d rowDelete[queued=%d done=%d inFlight=%d err=%d]",
						ggraph.graphID,
						sectionID,
						len(ch),
						cap(ch),
						atomic.LoadInt64(&rowDeleteQueued),
						atomic.LoadInt64(&rowDeleteDone),
						atomic.LoadInt64(&rowDeleteInFlight),
						atomic.LoadInt64(&rowDeleteErr),
					)
					timer.Reset(warnEvery)
				}
			}
		}

		for i := resumeFrom; i < len(deleteTasks); i++ {
			if !dispatchRowDelete(deleteTasks[i]) {
				addErr(ctx.Err())
				break
			}
		}
		rowDeleteMu.Lock()
		sectionCount := len(rowDeleteBySection)
		for _, ch := range rowDeleteBySection {
			close(ch)
		}
		rowDeleteMu.Unlock()
		bulkDelLogf("BulkDel waiting row tombstone workers graph=%s sections=%d", ggraph.graphID, sectionCount)
		rowDeleteWG.Wait()
		log.Infof(
			"BulkDel row tombstone stage complete graph=%s sections=%d queued=%d done=%d inFlight=%d err=%d",
			ggraph.graphID,
			atomic.LoadInt64(&rowDeleteSections),
			atomic.LoadInt64(&rowDeleteQueued),
			atomic.LoadInt64(&rowDeleteDone),
			atomic.LoadInt64(&rowDeleteInFlight),
			atomic.LoadInt64(&rowDeleteErr),
		)
	} else {
		setStage("row_gc_deferred")
		log.Infof("BulkDel row tombstone stage deferred graph=%s mode=deferred", ggraph.graphID)
	}

	// Resolve reverse index keys for rows whose table metadata was not loaded.
	setStage("resolve_missing_fields")
	missingRowsMu.Lock()
	missingTableCount := len(missingRowsByTable)
	missingRowsMu.Unlock()
	if missingTableCount > 0 {
		bulkDelLogf("BulkDel resolving unknown-table reverse indexes graph=%s tables=%d", ggraph.graphID, missingTableCount)
		missingRowsMu.Lock()
		for tableID, rowSet := range missingRowsByTable {
			if len(rowSet) == 0 {
				continue
			}
			atomic.AddInt64(&unknownTableScans, 1)
			tableScanStart := time.Now()
			matched := 0
			bulkDelLogf("BulkDel unknown-table scan start graph=%s tableID=%d rowTargets=%d", ggraph.graphID, tableID, len(rowSet))
			tableIDBytes := binary.LittleEndian.AppendUint16(nil, tableID)
			rPrefix := bytes.Join([][]byte{benchtop.RFieldPrefix, tableIDBytes}, benchtop.FieldSep)
			err := ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
				for it.Seek(rPrefix); it.Valid() && bytes.HasPrefix(it.Key(), rPrefix); it.Next() {
					keyBytes := bytes.Clone(it.Key())
					// key format: R <sep> tableID(2 bytes) <sep> field <sep> rowID
					if len(keyBytes) <= len(rPrefix) || keyBytes[len(rPrefix)] != benchtop.FieldSep[0] {
						continue
					}
					tail := keyBytes[len(rPrefix)+1:]
					fieldEnd := bytes.IndexByte(tail, benchtop.FieldSep[0])
					if fieldEnd < 0 || fieldEnd+1 >= len(tail) {
						continue
					}
					field := string(tail[:fieldEnd])
					rowID := string(tail[fieldEnd+1:])
					if _, ok := rowSet[rowID]; !ok {
						continue
					}
					matched++
					if err := enqueueField(fieldInfo{rKey: benchtop.RFieldKey(tableID, field, rowID), field: field, tableId: tableID, id: []byte(rowID)}, "unknown_table_scan"); err != nil {
						return err
					}
				}
				return nil
			})
			addErr(err)
			bulkDelLogf(
				"BulkDel unknown-table scan done graph=%s tableID=%d rowTargets=%d matched=%d duration=%s",
				ggraph.graphID,
				tableID,
				len(rowSet),
				matched,
				time.Since(tableScanStart),
			)
		}
		missingRowsMu.Unlock()
	}

	close(fieldChan)
	fieldWG.Wait()
	bulkDelLogf("BulkDel field aggregation complete graph=%s fields=%d", ggraph.graphID, len(allFields))

	// Process field indices with single iterator
	setStage("collect_index_keys")
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
	bulkDelLogf("BulkDel index key collection complete graph=%s indexDelKeys=%d", ggraph.graphID, len(indexDelKeys))

	// Chunked deletes with periodic commit so very large deletes don't block on one huge sync.
	chunked := func(singles [][]byte, ranges [][2][]byte, posKeys [][]byte, indexDelKeys [][]byte) error {
		const maxOpsPerBatch = 200_000

		batch := ggraph.driver.Pkv.Db.NewBatch()
		ops := 0
		commits := 0
		flush := func(force bool) error {
			if !force && ops < maxOpsPerBatch {
				return nil
			}
			if ops == 0 {
				return nil
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			if err := batch.Close(); err != nil {
				return err
			}
			commits++
			if commits%5 == 0 {
				bulkDelLogf("BulkDel chunk commit graph=%s commits=%d", ggraph.graphID, commits)
			}
			batch = ggraph.driver.Pkv.Db.NewBatch()
			ops = 0
			return nil
		}
		defer batch.Close()

		for _, k := range singles {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
			ops++
			if err := flush(false); err != nil {
				return err
			}
		}
		for _, r := range ranges {
			if err := batch.DeleteRange(r[0], r[1], nil); err != nil {
				return err
			}
			ops++
			if err := flush(false); err != nil {
				return err
			}
		}
		for _, k := range posKeys {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
			ops++
			if err := flush(false); err != nil {
				return err
			}
		}
		for _, k := range indexDelKeys {
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
			ops++
			if err := flush(false); err != nil {
				return err
			}
		}
		if err := flush(true); err != nil {
			return err
		}
		if commits > 0 {
			bulkDelLogf("BulkDel chunking complete graph=%s commits=%d", ggraph.graphID, commits)
		}
		return nil
	}

	// Perform deletes
	setStage("commit")
	bulkDelLogf("BulkDel acquiring pebble write lock graph=%s", ggraph.graphID)
	lockWaitStart := time.Now()
	ggraph.driver.PebbleLock.Lock()
	bulkDelLogf("BulkDel pebble write lock acquired graph=%s wait=%s", ggraph.graphID, time.Since(lockWaitStart))
	commitStart := time.Now()
	if err := chunked(singles, ranges, posKeys, indexDelKeys); err != nil {
		addErr(err)
	}
	ggraph.ts.Touch(ggraph.graphID)
	ggraph.driver.PebbleLock.Unlock()
	log.Infof("BulkDel commit complete graph=%s duration=%s", ggraph.graphID, time.Since(commitStart))

	bulkDelLogf("Total edges seen: %d", getSeenCount())
	outErr := bulkErr.ErrorOrNil()
	setStage("done")
	log.Infof("BulkDel done graph=%s totalDuration=%s err=%v", ggraph.graphID, time.Since(start), outErr)
	return outErr
}

func envTruthy(name string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
