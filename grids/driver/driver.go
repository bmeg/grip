package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/arrowdriver"
	"github.com/bmeg/benchtop/jsontable"
	"github.com/bmeg/benchtop/jsontable/tpath"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/benchtop/util"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

var ErrNotFound = errors.New("row not found in any table")

type IDInfo struct {
	Label string
	Loc   *benchtop.RowLoc
}

type BackendTable struct {
	Name    string
	Label   string
	TableId uint16
	Fields  map[string]struct{}
	Store   benchtop.TableStore
}

func (t *BackendTable) GetColumnDefs() []benchtop.ColumnDef { return t.Store.GetColumnDefs() }
func (t *BackendTable) HasField(field string) bool          { return t.Store.HasField(field) }

func (t *BackendTable) AddRow(elem benchtop.Row) (*benchtop.RowLoc, error) {
	return t.Store.AddRow(elem)
}

func (t *BackendTable) AddRows(elems []benchtop.Row) ([]*benchtop.RowLoc, error) {
	return t.Store.AddRows(elems)
}

func (t *BackendTable) GetRow(loc *benchtop.RowLoc) (map[string]any, error) {
	return t.Store.GetRow(loc)
}
func (t *BackendTable) GetRowLoc(id string) (*benchtop.RowLoc, error) { return t.Store.GetRowLoc(id) }
func (t *BackendTable) GetRows(locs []*benchtop.RowLoc) ([]map[string]any, []error) {
	return t.Store.GetRows(locs)
}
func (t *BackendTable) DeleteRow(loc *benchtop.RowLoc, id []byte) error {
	return t.Store.DeleteRow(loc, id)
}
func (t *BackendTable) MarkDeleteTable(loc *benchtop.RowLoc) error {
	return t.Store.MarkDeleteTable(loc)
}
func (t *BackendTable) ScanDoc(filter benchtop.RowFilter) chan map[string]any {
	return t.Store.ScanDoc(filter)
}
func (t *BackendTable) ScanDocProjected(fields []string, filter benchtop.RowFilter) chan map[string]any {
	return t.Store.ScanDocProjected(fields, filter)
}
func (t *BackendTable) ScanId(filter benchtop.RowFilter) chan string { return t.Store.ScanId(filter) }
func (t *BackendTable) ScanFull(filter benchtop.RowFilter) chan benchtop.RowLocData {
	return t.Store.ScanFull(filter)
}
func (t *BackendTable) Close() error { return t.Store.Close() }

type FieldInfo struct {
	Label string
	Field string
}

type GridKVDriver struct {
	Lock       sync.RWMutex
	PebbleLock sync.RWMutex
	Pkv        *pebblebulk.PebbleKV
	closePkv   func() error
	Tables     map[string]*BackendTable
	TablesByID map[uint16]*BackendTable
	TableDr    benchtop.TableDriver
}

func NewGridKVDriver(path string, driver string) (*GridKVDriver, error) {
	if driver == "" {
		driver = "jsontable"
	}
	driver = strings.ToLower(driver)

	var td benchtop.TableDriver
	var pkv *pebblebulk.PebbleKV
	var closePkv func() error
	var err error
	switch driver {
	case "jsontable", "json":
		td, err = jsontable.NewJSONDriver(path)
		if err != nil {
			return nil, err
		}
		rawKV := td.GetKV()
		typedKV, ok := rawKV.(*pebblebulk.PebbleKV)
		if !ok || typedKV == nil {
			td.Close()
			return nil, fmt.Errorf("jsontable driver returned unsupported KV type %T", rawKV)
		}
		pkv = typedKV
	case "arrow":
		pkv, err = pebblebulk.NewPebbleKV(path)
		if err != nil {
			return nil, err
		}
		closePkv = pkv.Close
		td, err = arrowdriver.NewArrowDriver(path)
		if err != nil {
			pkv.Close()
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported grids table driver %q; supported drivers: jsontable, arrow", driver)
	}

	d := &GridKVDriver{
		Lock:       sync.RWMutex{},
		PebbleLock: sync.RWMutex{},
		Pkv:        pkv,
		closePkv:   closePkv,
		Tables:     map[string]*BackendTable{},
		TablesByID: map[uint16]*BackendTable{},
		TableDr:    td,
	}

	// We no longer PreloadCache as locations are embedded in structural keys.
	// But we MUST discover which tables exist so label scans work.
	for _, tableName := range d.TableDr.List() {
		if _, err := d.GetOrLoadTable(tableName); err != nil {
			log.Errorf("Failed to discover table %s: %v", tableName, err)
		}
	}
	if err := d.LoadFields(); err != nil {
		d.Close()
		return nil, err
	}

	return d, nil
}

func (d *GridKVDriver) AddFieldIndex(label, field string) error {
	id, err := d.TableDr.LookupTableID(label)
	if err != nil {
		return err
	}
	return d.AddField(id, field)
}

func (d *GridKVDriver) RemoveFieldIndex(label, field string) error {
	id, err := d.TableDr.LookupTableID(label)
	if err != nil {
		return err
	}
	return d.RemoveField(id, field)
}

func (d *GridKVDriver) Close() {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	if d.TableDr != nil {
		d.TableDr.Close()
	}
	if d.closePkv != nil {
		_ = d.closePkv()
		d.closePkv = nil
	}
}

func (d *GridKVDriver) GetOrLoadTable(name string) (*BackendTable, error) {
	// Resolve ID from label name
	id, err := d.TableDr.LookupTableID(name)
	if err != nil {
		// Try case-insensitive lookup if direct lookup fails (optional, based on design)
		// But TableDr should handle canonicalization or we accept error.
		return nil, fmt.Errorf("table %s not found: %v", name, err)
	}

	d.Lock.RLock()
	if t, ok := d.Tables[name]; ok {
		d.Lock.RUnlock()
		return t, nil
	}
	d.Lock.RUnlock()

	d.Lock.Lock()
	defer d.Lock.Unlock()
	if t, ok := d.Tables[name]; ok {
		return t, nil
	}

	store, err := d.TableDr.Get(id)
	if err != nil {
		return nil, err
	}

	// Create wrapper
	// We need to know which fields are indexed?
	// BackendTable.Fields is map[string]struct{}.
	// We can populate it from store.GetColumnDefs()
	fields := make(map[string]struct{})
	for _, col := range store.GetColumnDefs() {
		fields[col.Key] = struct{}{}
	}

	var tableLabel string
	if strings.HasPrefix(name, "v_") {
		tableLabel = name[2:]
	} else if strings.HasPrefix(name, "e_") {
		tableLabel = name[2:]
	} else {
		tableLabel = name
	}

	bt := &BackendTable{
		Name:    name,
		Label:   tableLabel,
		TableId: id,
		Fields:  fields,
		Store:   store,
	}
	d.Tables[name] = bt
	d.TablesByID[id] = bt
	return bt, nil
}

func (d *GridKVDriver) GetTableByID(id uint16) (*BackendTable, error) {
	d.Lock.RLock()
	t, ok := d.TablesByID[id]
	d.Lock.RUnlock()
	if ok {
		return t, nil
	}

	// Try to load if not in memory
	info, err := d.TableDr.GetTableInfo(id)
	if err != nil {
		return nil, err
	}
	return d.GetOrLoadTable(info.Name)
}

func (d *GridKVDriver) New(name string, columns []benchtop.ColumnDef) (benchtop.TableStore, error) {
	// Check if already exists? GetOrLoad checks.
	// We can trust TableDr.New to handle existence or overwriting logic.

	store, err := d.TableDr.New(name, columns)
	if err != nil {
		return nil, err
	}

	// We need ID to store in cache.
	id, err := d.TableDr.LookupTableID(name)
	if err != nil {
		// Should not happen if New succeeded?
		return nil, fmt.Errorf("failed to lookup ID after New(%s): %v", name, err)
	}

	d.Lock.Lock()
	defer d.Lock.Unlock()

	// Check if already in map (race condition?)
	if t, ok := d.Tables[name]; ok {
		return t, nil
	}

	fields := make(map[string]struct{})
	for _, col := range store.GetColumnDefs() {
		fields[col.Key] = struct{}{}
	}

	var tableLabel string
	if strings.HasPrefix(name, "v_") {
		tableLabel = name[2:]
	} else if strings.HasPrefix(name, "e_") {
		tableLabel = name[2:]
	} else {
		tableLabel = name
	}

	t := &BackendTable{
		Name:    name,
		Label:   tableLabel,
		TableId: id,
		Fields:  fields,
		Store:   store,
	}
	d.Tables[name] = t
	d.TablesByID[id] = t
	return t, nil
}

func (d *GridKVDriver) Get(name string) (benchtop.TableStore, error) {
	return d.GetOrLoadTable(name)
}

func (d *GridKVDriver) List() []string { return d.TableDr.List() }

func (d *GridKVDriver) AddTableEntryInfo(tx *pebblebulk.PebbleBulk, rowID []byte, rowLoc *benchtop.RowLoc) error {
	return tx.Set(benchtop.NewPosKey(rowLoc.TableId, rowID), benchtop.EncodeRowLoc(rowLoc), nil)
}

func (d *GridKVDriver) BulkLoad(tableID uint16, rows chan *benchtop.Row) error {
	return d.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		return d.BulkLoadInternal(tableID, rows, tx)
	})
}

func (d *GridKVDriver) BulkLoadInternal(targetID uint16, inputs chan *benchtop.Row, tx *pebblebulk.PebbleBulk) error {
	var wg sync.WaitGroup
	tableChans := make(map[uint16]chan *benchtop.Row)

	// Global tracker for this specific bulk load session
	// This prevents duplicates from entering ANY table channel
	inFlight := sync.Map{}

	// We also need a snapshot here to check against the existing DB
	snap := d.Pkv.Db.NewSnapshot()
	defer snap.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for row := range inputs {
			if row == nil {
				continue
			}

			// 1. Check against the DB (Snapshot)
			pKey := benchtop.NewPosKey(row.TableID, row.Id)
			_, closer, err := snap.Get(pKey)
			if err == nil {
				closer.Close()
				continue // Already in DB
			}

			// 2. Check against "In-Flight" memory
			idStr := string(row.Id)
			if _, loaded := inFlight.LoadOrStore(idStr, struct{}{}); loaded {
				continue // Already being processed by a channel
			}

			// 3. Dispatch to table channel
			id := row.TableID
			ch, exists := tableChans[id]
			if !exists {
				// ... (your existing table info loading logic)
				ch = make(chan *benchtop.Row, 1024)
				tableChans[id] = ch
				wg.Add(1)
				go func(id uint16, c chan *benchtop.Row) {
					defer wg.Done()
					d.TableDr.BulkLoad(id, c)
				}(id, ch)
			}
			ch <- row
		}
		for _, ch := range tableChans {
			close(ch)
		}
	}()
	wg.Wait()
	return nil
}

func (d *GridKVDriver) RowIdsByHas(field string, value any, op query.Condition) chan benchtop.Index {
	return d.TableDr.RowIdsByHas(field, value, op)
}

func (d *GridKVDriver) RowIdsByLabelFieldValue(label, field string, value any, op query.Condition) chan benchtop.Index {
	d.Lock.RLock()
	var tids []uint16
	for _, t := range d.Tables {
		if t.Label == label || t.Name == label {
			tids = append(tids, t.TableId)
		}
	}
	d.Lock.RUnlock()

	out := make(chan benchtop.Index)
	go func() {
		defer close(out)
		var wg sync.WaitGroup
		for _, tid := range tids {
			wg.Add(1)
			go func(tid uint16) {
				defer wg.Done()
				for idx := range d.TableDr.RowIdsByTableFieldValue(tid, field, value, op) {
					out <- idx
				}
			}(tid)
		}
		wg.Wait()
	}()
	return out
}

func (d *GridKVDriver) GetLabels(edges bool, removePrefix bool) chan string {
	return d.TableDr.GetLabels(edges, removePrefix)
}

func (d *GridKVDriver) InvalidateLoc(tableID uint16, rowID string) {
	d.TableDr.InvalidateLoc(tableID, rowID)
}

func (d *GridKVDriver) GetIDsForLabel(label string) chan string {
	d.Lock.RLock()
	var tids []uint16
	for _, t := range d.Tables {
		if t.Label == label || t.Name == label {
			tids = append(tids, t.TableId)
		}
	}
	d.Lock.RUnlock()

	out := make(chan string)
	go func() {
		defer close(out)
		var wg sync.WaitGroup
		for _, tid := range tids {
			wg.Add(1)
			go func(tid uint16) {
				defer wg.Done()
				for id := range d.GetIDsForTable(tid) {
					out <- id
				}
			}(tid)
		}
		wg.Wait()
	}()
	return out
}

func (d *GridKVDriver) AddField(tableID uint16, field string) error {
	d.Lock.Lock()
	if t, ok := d.TablesByID[tableID]; ok {
		if t.Fields == nil {
			t.Fields = make(map[string]struct{})
		}
		t.Fields[field] = struct{}{}
	}
	d.Lock.Unlock()

	if err := d.Pkv.Set(benchtop.FieldKey(field, tableID, nil, nil), []byte{}, nil); err != nil {
		return err
	}

	idBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(idBytes, tableID)
	revPrefix := bytes.Join([][]byte{benchtop.RFieldPrefix, idBytes, []byte(field)}, benchtop.FieldSep)
	if err := d.Pkv.Set(revPrefix, []byte{}, nil); err != nil {
		return err
	}

	store, err := d.TableDr.Get(tableID)
	if err != nil {
		return err
	}
	return d.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		for r := range store.ScanFull(nil) {
			fieldValue := tpath.PathLookup(r.DataMap, field)
			rowID, ok := r.DataMap["_id"].(string)
			if !ok {
				continue
			}
			if err := tx.Set(benchtop.FieldKey(field, tableID, fieldValue, []byte(rowID)), benchtop.EncodeRowLoc(r.Loc), nil); err != nil {
				return err
			}
			if fieldValue != nil {
				mval, err := sonic.ConfigFastest.Marshal(fieldValue)
				if err != nil {
					return err
				}
				if err := tx.Set(benchtop.RFieldKey(tableID, field, rowID), mval, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (d *GridKVDriver) RemoveField(tableID uint16, field string) error {
	d.Lock.Lock()
	if t, ok := d.TablesByID[tableID]; ok && t.Fields != nil {
		delete(t.Fields, field)
	}
	d.Lock.Unlock()

	fieldPrefix := benchtop.FieldLabelKey(field, tableID)
	idBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(idBytes, tableID)
	revPrefix := bytes.Join([][]byte{benchtop.RFieldPrefix, idBytes, []byte(field)}, benchtop.FieldSep)
	return d.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.DeletePrefix(fieldPrefix); err != nil {
			return err
		}
		if err := tx.DeletePrefix(revPrefix); err != nil {
			return err
		}
		return nil
	})
}

func (d *GridKVDriver) DeleteRowField(tableID uint16, field, rowID string) error {
	// Deletes a singular row index field

	// Get the field value from the reverse index
	rowIndexKey := benchtop.RFieldKey(tableID, field, rowID)
	var fieldValueBytes []byte
	err := d.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		val, err := it.Get(rowIndexKey)
		if err != nil {
			return err
		}
		fieldValueBytes = make([]byte, len(val))
		copy(fieldValueBytes, val)
		return nil
	})

	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil // Already deleted or doesn't exist
		}
		return err
	}

	var fieldValue any
	if len(fieldValueBytes) > 0 {
		if err := sonic.ConfigFastest.Unmarshal(fieldValueBytes, &fieldValue); err != nil {
			return err
		}
	}

	// Delete both the forward and reverse index entries
	return d.Pkv.BulkWrite(func(tx *pebblebulk.PebbleBulk) error {
		if err := tx.Delete(benchtop.FieldKey(field, tableID, fieldValue, []byte(rowID)), nil); err != nil {
			return err
		}
		if err := tx.Delete(rowIndexKey, nil); err != nil {
			return err
		}
		return nil
	})
}

func (d *GridKVDriver) GetIDsForTable(tableID uint16) chan string {
	store, err := d.TableDr.Get(tableID)
	if err != nil {
		out := make(chan string)
		close(out)
		return out
	}
	// Use ScanId from store
	return store.ScanId(nil)
}

func (d *GridKVDriver) LoadFields() error {
	fPrefix := benchtop.FieldPrefix
	return d.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
		for it.Seek(fPrefix); it.Valid() && bytes.HasPrefix(it.Key(), fPrefix); it.Next() {
			field, tableID, _, _ := benchtop.FieldKeyParse(it.Key())
			info, err := d.TableDr.GetTableInfo(tableID)
			if err != nil {
				continue
			}
			table, err := d.GetOrLoadTable(info.Name)
			if err != nil {
				continue
			}
			if table.Fields == nil {
				table.Fields = map[string]struct{}{}
			}
			table.Fields[field] = struct{}{}
		}
		return nil
	})
}

func (d *GridKVDriver) ValuesWithin(v any) []any { return util.SliceToAny(v) }

func (d *GridKVDriver) BulkLoadBatch(tx *pebblebulk.PebbleBulk, entries []*benchtop.Row, snap *pebble.Snapshot) error {
	if len(entries) == 0 {
		return nil
	}

	var it *pebble.Iterator
	if snap != nil {
		it, _ = snap.NewIter(nil)
		defer it.Close()
	}

	// 1. Group rows by TableID
	byTable := make(map[uint16][]*benchtop.Row)
	for _, row := range entries {
		byTable[row.TableID] = append(byTable[row.TableID], row)
	}

	for tid, rows := range byTable {
		t, err := d.GetTableByID(tid)
		if err != nil {
			return err
		}

		// Sort rows by ID to maximize iterator spatial locality during existence check
		sort.Slice(rows, func(i, j int) bool {
			return bytes.Compare(rows[i].Id, rows[j].Id) < 0
		})

		// 2. Filter duplicates and existing rows
		var filteredRows []*benchtop.Row
		filteredRows = make([]*benchtop.Row, 0, len(rows))
		var lastID []byte
		for _, row := range rows {
			// Skip duplicates within the same sorted batch
			if lastID != nil && bytes.Equal(row.Id, lastID) {
				continue
			}
			lastID = row.Id

			// Database existence check (Snapshot)
			if it != nil {
				// Check Vertex and Edge keys as they are now authoritative
				vkey := key.VertexKey(string(row.Id))
				if it.SeekGE(vkey) && bytes.Equal(it.Key(), vkey) {
					continue
				}
				// For edges, we'd need Dst/Src prefix check, but VertexKey is often enough for unique IDs.
				// However, if we want to be thorough:
				ekeyPrefix := key.EdgeKeyPrefix(string(row.Id))
				if it.SeekGE(ekeyPrefix) && bytes.HasPrefix(it.Key(), ekeyPrefix) {
					continue
				}
			}
			filteredRows = append(filteredRows, row)
		}

		if len(filteredRows) == 0 {
			continue
		}

		// Prepare raw rows for the table driver (JSONTable.AddRows uses []Row)
		rawRows := make([]benchtop.Row, len(filteredRows))
		for i, r := range filteredRows {
			rawRows[i] = *r
		}

		// 3. Bulk add rows to the table storage (e.g. JSON/Pebble/Arrow)
		locs, err := t.AddRows(rawRows)
		if err != nil {
			return err
		}

		if len(locs) != len(filteredRows) {
			return fmt.Errorf("BulkLoadBatch: AddRows returned %d locs for %d rows", len(locs), len(filteredRows))
		}

		// 4. Process each row's index and metadata updates
		for i, row := range filteredRows {
			rowLoc := locs[i]
			idStr := string(row.Id)

			// Update the structural keys (Integrated Keys)
			// Check if it's a vertex or edge based on table name prefix
			if strings.HasPrefix(t.Name, key.VertexTablePrefix) {
				vkey := key.VertexKey(idStr)
				// We need the label. BackendTable has it.
				val := benchtop.EncodeVertexValue(t.Label, rowLoc)
				if err := tx.Set(vkey, val, nil); err != nil {
					return err
				}
			} else if strings.HasPrefix(t.Name, key.EdgeTablePrefix) {
				// For edges, we might need to update multi-keys.
				// This is a bit complex in driver if we don't have the From/To.
				// But we can check if data has them (BulkAdd puts them there).
				from, fOk := row.Data["_from"].(string)
				to, tOk := row.Data["_to"].(string)
				if fOk && tOk {
					val := benchtop.EncodeEdgeValue(t.Label, rowLoc)
					ekey := key.EdgeKey(idStr, from, to, t.Label)
					if err := tx.Set(ekey, val, nil); err != nil {
						return err
					}
					if err := tx.Set(key.SrcEdgeKey(idStr, from, to, t.Label), val, nil); err != nil {
						return err
					}
					if err := tx.Set(key.DstEdgeKey(idStr, from, to, t.Label), val, nil); err != nil {
						return err
					}
				}
			}

			// Primary Index partitioned by TableID (useful for scans)
			if err := tx.Set(benchtop.NewPosKey(tid, row.Id), benchtop.EncodeRowLoc(rowLoc), nil); err != nil {
				return err
			}

			// Secondary Index building
			if len(t.Fields) > 0 {
				for field := range t.Fields {
					if val := tpath.PathLookup(row.Data, field); val != nil {
						// Forward index: Field key -> Row location
						fKey := benchtop.FieldKey(field, tid, val, row.Id)
						if err := tx.Set(fKey, benchtop.EncodeRowLoc(rowLoc), nil); err != nil {
							return err
						}

						// Reverse index: Row ID -> Field value
						rKey := benchtop.RFieldKey(tid, field, string(row.Id))
						bVal, err := sonic.ConfigFastest.Marshal(val)
						if err == nil {
							if err := tx.Set(rKey, bVal, nil); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	return nil
}

func (d *GridKVDriver) ListFields() []FieldInfo {
	var out []FieldInfo
	// Explicitly load all tables to ensure we have their field info
	for _, name := range d.TableDr.List() {
		if _, err := d.GetOrLoadTable(name); err != nil {
			continue
		}
	}

	d.Lock.RLock()
	defer d.Lock.RUnlock()

	for _, table := range d.Tables {
		if table == nil {
			continue
		}
		info, err := d.TableDr.GetTableInfo(table.TableId)
		if err != nil {
			continue
		}
		for field := range table.Fields {
			out = append(out, FieldInfo{Label: info.Name, Field: field})
		}
	}
	return out
}

func (d *GridKVDriver) GetLocBatch(ctx context.Context, ids []string) (map[string]*IDInfo, error) {
	out := make(map[string]*IDInfo, len(ids))
	for _, id := range ids {
		// New path: check Vertex and Edge keys directly for RowLoc
		// 1. Check Vertex
		vkey := key.VertexKey(id)
		val, closer, err := d.Pkv.Get(vkey)
		if err == nil {
			defer closer.Close()
			vlbl, loc := benchtop.DecodeVertexValue(val)
			if loc != nil {
				out[id] = &IDInfo{Label: vlbl, Loc: loc}
				continue
			}
		} else if closer != nil {
			closer.Close()
		}

		// 2. Check Edges (if id might be an edge ID)
		ekeyPrefix := key.EdgeKeyPrefix(id)
		var eloc *benchtop.RowLoc
		var elbl string
		_ = d.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), ekeyPrefix); it.Next() {
				byteVal, _ := it.Value()
				_, eloc = benchtop.DecodeEdgeValue(byteVal)
				if eloc != nil {
					_, _, _, elbl = key.EdgeKeyParse(it.Key())
					return nil
				}
			}
			return nil
		})
		if eloc != nil {
			out[id] = &IDInfo{Label: elbl, Loc: eloc}
		}
	}
	return out, nil
}
