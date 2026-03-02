package grids

import (
	"context"
	"strings"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/arrowdriver"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/driver"
	"github.com/bmeg/grip/grids/filter"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// //////////////////////////////////////////////////////////////////////////////
// LookupVertexHasLabelCondIndex look up vertices has label

type lookupVertsHasLabelCondIndexStep struct {
	labels          []string
	expr            *gripql.HasExpression
	loadData        bool
	projectedFields []string
}

func (t lookupVertsHasLabelCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	return &lookupVertsHasLabelCondIndexProc{
		db:              graph,
		expr:            t.expr,
		labels:          t.labels,
		loadData:        ps.StepLoadData(),
		projectedFields: normalizeProjectedFields(ps.StepRequiredFields()),
	}, nil

}

func (t lookupVertsHasLabelCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsHasLabelCondIndexProc struct {
	db              *Graph
	labels          []string
	expr            *gripql.HasExpression
	loadData        bool
	projectedFields []string
}

type indexedCondition struct {
	key   string
	value any
	op    query.Condition
}

type rawDocScanner interface {
	ScanDocRaw(filter benchtop.RowFilter) chan arrowdriver.RawDoc
}

func normalizeProjectedFields(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := []string{}
	seen := map[string]struct{}{}
	for _, f := range in {
		if f == "" || f == "*" {
			return nil
		}
		if strings.HasPrefix(f, "$") {
			// keep current-step top-level paths only
			if strings.HasPrefix(f, "$.") {
				f = strings.TrimPrefix(f, "$.")
			} else if strings.HasPrefix(f, "$_current.") {
				f = strings.TrimPrefix(f, "$_current.")
			} else {
				continue
			}
		}
		if strings.Contains(f, ".") || strings.Contains(f, "[") {
			continue
		}
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	return out
}

func fetchRowsForLocs(table benchtop.TableStore, locs []*benchtop.RowLoc, fields []string) ([]map[string]any, []error) {
	if len(locs) == 0 {
		return []map[string]any{}, []error{}
	}
	if projectedGetter, ok := table.(projectedRowsGetter); ok {
		if projectedFields, ok := normalizeProjectedFetchFields(fields); ok {
			rows, errs := projectedGetter.GetRowsProjected(locs, projectedFields)
			if len(rows) == len(locs) && len(errs) == len(locs) {
				return rows, errs
			}
		}
	}
	return table.GetRows(locs)
}

func emitIndexedVertexBatches(ctx context.Context, table benchtop.TableStore, traveler gdbi.Traveler, label string, fields []string, in <-chan benchtop.Index, out gdbi.OutPipe) int {
	locs := make([]*benchtop.RowLoc, 0, resolveBatchSize)
	ids := make([]string, 0, resolveBatchSize)
	total := 0

	flush := func() bool {
		if len(locs) == 0 {
			return true
		}
		if len(fields) == 0 {
			if rawGetter, ok := table.(rawRowsGetter); ok {
				rows, errs := rawGetter.GetRowsRawPayload(locs)
				for i := range rows {
					if i >= len(errs) || errs[i] != nil {
						continue
					}
					v := gdbi.DataElement{
						ID:       ids[i],
						Label:    label,
						RawJSON:  rows[i],
						ModeHint: gdbi.RowModeRaw,
					}
					select {
					case <-ctx.Done():
						return false
					case out <- traveler.AddCurrent(&v):
					}
					total++
				}
				locs = locs[:0]
				ids = ids[:0]
				return true
			}
		}
		rows, errs := fetchRowsForLocs(table, locs, fields)
		for i := range rows {
			if i >= len(errs) || errs[i] != nil {
				continue
			}
			data := rows[i]
			if len(fields) > 0 {
				data = projectRowMap(rows[i], fields)
			}
			v := gdbi.Vertex{
				ID:     ids[i],
				Label:  label,
				Data:   data,
				Loaded: true,
			}
			select {
			case <-ctx.Done():
				return false
			case out <- traveler.AddCurrent(&v):
			}
			total++
		}
		locs = locs[:0]
		ids = ids[:0]
		return true
	}

	for entry := range in {
		if ctx.Err() != nil {
			return total
		}
		if entry.Loc == nil {
			continue
		}
		locs = append(locs, entry.Loc)
		ids = append(ids, string(entry.Key))
		if len(locs) >= resolveBatchSize {
			if !flush() {
				return total
			}
		}
	}
	flush()
	return total
}

func emitIndexedVertexMapBatches(ctx context.Context, table benchtop.TableStore, traveler gdbi.Traveler, label string, fields []string, idsToLoc map[string]*benchtop.RowLoc, out gdbi.OutPipe) int {
	locs := make([]*benchtop.RowLoc, 0, resolveBatchSize)
	ids := make([]string, 0, resolveBatchSize)
	total := 0

	flush := func() bool {
		if len(locs) == 0 {
			return true
		}
		if len(fields) == 0 {
			if rawGetter, ok := table.(rawRowsGetter); ok {
				rows, errs := rawGetter.GetRowsRawPayload(locs)
				for i := range rows {
					if i >= len(errs) || errs[i] != nil {
						continue
					}
					v := gdbi.DataElement{
						ID:       ids[i],
						Label:    label,
						RawJSON:  rows[i],
						ModeHint: gdbi.RowModeRaw,
					}
					select {
					case <-ctx.Done():
						return false
					case out <- traveler.AddCurrent(&v):
					}
					total++
				}
				locs = locs[:0]
				ids = ids[:0]
				return true
			}
		}
		rows, errs := fetchRowsForLocs(table, locs, fields)
		for i := range rows {
			if i >= len(errs) || errs[i] != nil {
				continue
			}
			data := rows[i]
			if len(fields) > 0 {
				data = projectRowMap(rows[i], fields)
			}
			v := gdbi.Vertex{
				ID:     ids[i],
				Label:  label,
				Data:   data,
				Loaded: true,
			}
			select {
			case <-ctx.Done():
				return false
			case out <- traveler.AddCurrent(&v):
			}
			total++
		}
		locs = locs[:0]
		ids = ids[:0]
		return true
	}

	for id, loc := range idsToLoc {
		if ctx.Err() != nil {
			return total
		}
		if loc == nil {
			continue
		}
		locs = append(locs, loc)
		ids = append(ids, id)
		if len(locs) >= resolveBatchSize {
			if !flush() {
				return total
			}
		}
	}
	flush()
	return total
}

func flattenAndConditions(expr *gripql.HasExpression) ([]*gripql.HasCondition, bool) {
	if expr == nil || expr.Expression == nil {
		return nil, false
	}
	switch ex := expr.Expression.(type) {
	case *gripql.HasExpression_Condition:
		if ex.Condition == nil {
			return nil, false
		}
		return []*gripql.HasCondition{ex.Condition}, true
	case *gripql.HasExpression_And:
		out := make([]*gripql.HasCondition, 0, len(ex.And.Expressions))
		for _, sub := range ex.And.Expressions {
			conds, ok := flattenAndConditions(sub)
			if !ok {
				return nil, false
			}
			out = append(out, conds...)
		}
		return out, len(out) > 0
	default:
		return nil, false
	}
}

func buildIndexedConditions(expr *gripql.HasExpression) ([]indexedCondition, []string, bool) {
	conds, ok := flattenAndConditions(expr)
	if !ok || len(conds) == 0 {
		return nil, nil, false
	}
	out := make([]indexedCondition, 0, len(conds))
	labels := []string{}
	for _, cond := range conds {
		if cond == nil {
			return nil, nil, false
		}
		if cond.Key == "_label" && filter.ToQueryCondition(cond.Condition) == query.EQ {
			// _label == "X" is handled as a table-name filter, not a field index
			if label, ok := cond.Value.AsInterface().(string); ok {
				labels = append(labels, label)
			}
			continue
		}
		out = append(out, indexedCondition{
			key:   cond.Key,
			value: cond.Value.AsInterface(),
			op:    filter.ToQueryCondition(cond.Condition),
		})
	}
	return out, labels, true
}

func allIndexedForLabel(table *driver.BackendTable, conds []indexedCondition) bool {
	if table == nil || len(conds) == 0 {
		return false
	}
	for _, cond := range conds {
		if _, ok := table.Fields[cond.key]; !ok {
			return false
		}
	}
	return true
}

type tableTypeCache struct {
	driver   *driver.GridKVDriver
	isVertex map[uint16]bool
}

func newTableTypeCache(d *driver.GridKVDriver) *tableTypeCache {
	return &tableTypeCache{
		driver:   d,
		isVertex: map[uint16]bool{},
	}
}

func (c *tableTypeCache) isVertexLoc(loc *benchtop.RowLoc) bool {
	if c == nil || loc == nil {
		return false
	}
	if ok, seen := c.isVertex[loc.TableId]; seen {
		return ok
	}
	table, err := c.driver.GetTableByID(loc.TableId)
	if err != nil || table == nil {
		c.isVertex[loc.TableId] = false
		return false
	}
	isVertex := strings.HasPrefix(table.Name, key.VertexTablePrefix)
	c.isVertex[loc.TableId] = isVertex
	return isVertex
}

func streamHasMatchesForTraveler(ctx context.Context, d *driver.GridKVDriver, t gdbi.Traveler, cond indexedCondition, fields []string, out chan<- gdbi.ElementLookup, tcache *tableTypeCache, requireVertex bool) (int, bool) {
	matches := 0
	for entry := range d.RowIdsByHas(cond.key, cond.value, cond.op) {
		if requireVertex && (tcache == nil || !tcache.isVertexLoc(entry.Loc)) {
			continue
		}
		e := gdbi.ElementLookup{
			ID:   string(entry.Key),
			Ref:  t,
			Meta: gdbi.LookupMeta{Opaque: entry.Loc, Fields: fields},
		}
		select {
		case <-ctx.Done():
			return matches, false
		case out <- e:
		}
		matches++
	}
	return matches, true
}

func emitGlobalIndexedVertexBatches(ctx context.Context, d *driver.GridKVDriver, traveler gdbi.Traveler, cond indexedCondition, fields []string, tcache *tableTypeCache, out gdbi.OutPipe) int {
	tableCache := map[uint16]*driver.BackendTable{}
	locBuckets := map[uint16][]*benchtop.RowLoc{}
	idBuckets := map[uint16][]string{}
	total := 0

	flushTable := func(tid uint16) bool {
		locs := locBuckets[tid]
		if len(locs) == 0 {
			return true
		}
		table := tableCache[tid]
		if table == nil {
			var err error
			table, err = d.GetTableByID(tid)
			if err != nil || table == nil || !strings.HasPrefix(table.Name, key.VertexTablePrefix) {
				locBuckets[tid] = locs[:0]
				idBuckets[tid] = idBuckets[tid][:0]
				return true
			}
			tableCache[tid] = table
		}

		ids := idBuckets[tid]
		label := strings.TrimPrefix(table.Name, key.VertexTablePrefix)
		if len(fields) == 0 {
			if rawGetter, ok := table.TableStore.(rawRowsGetter); ok {
				rows, errs := rawGetter.GetRowsRawPayload(locs)
				for i := range rows {
					if i >= len(errs) || errs[i] != nil {
						continue
					}
					v := gdbi.DataElement{
						ID:       ids[i],
						Label:    label,
						RawJSON:  rows[i],
						ModeHint: gdbi.RowModeRaw,
					}
					select {
					case <-ctx.Done():
						return false
					case out <- traveler.AddCurrent(&v):
					}
					total++
				}
				locBuckets[tid] = locs[:0]
				idBuckets[tid] = ids[:0]
				return true
			}
		}

		rows, errs := fetchRowsForLocs(table.TableStore, locs, fields)
		for i := range rows {
			if i >= len(errs) || errs[i] != nil {
				continue
			}
			data := rows[i]
			if len(fields) > 0 {
				data = projectRowMap(rows[i], fields)
			}
			v := gdbi.Vertex{
				ID:     ids[i],
				Label:  label,
				Data:   data,
				Loaded: true,
			}
			select {
			case <-ctx.Done():
				return false
			case out <- traveler.AddCurrent(&v):
			}
			total++
		}
		locBuckets[tid] = locs[:0]
		idBuckets[tid] = ids[:0]
		return true
	}

	for entry := range d.RowIdsByHas(cond.key, cond.value, cond.op) {
		if ctx.Err() != nil {
			return total
		}
		if tcache == nil || !tcache.isVertexLoc(entry.Loc) {
			continue
		}
		tid := entry.Loc.TableId
		locBuckets[tid] = append(locBuckets[tid], entry.Loc)
		idBuckets[tid] = append(idBuckets[tid], string(entry.Key))
		if len(locBuckets[tid]) >= resolveBatchSize {
			if !flushTable(tid) {
				return total
			}
		}
	}

	for tid := range locBuckets {
		if !flushTable(tid) {
			return total
		}
	}
	return total
}

func (l *lookupVertsHasLabelCondIndexProc) intersectLabelIndexRows(label string, conds []indexedCondition) map[string]*benchtop.RowLoc {
	if len(conds) == 0 {
		return nil
	}
	var matches map[string]*benchtop.RowLoc
	for i, cond := range conds {
		next := map[string]*benchtop.RowLoc{}
		for entry := range l.db.driver.RowIdsByLabelFieldValue(label, cond.key, cond.value, cond.op) {
			id := string(entry.Key)
			if i == 0 {
				next[id] = entry.Loc
				continue
			}
			if prevLoc, ok := matches[id]; ok {
				if prevLoc != nil {
					next[id] = prevLoc
				} else {
					next[id] = entry.Loc
				}
			}
		}
		matches = next
		if len(matches) == 0 {
			break
		}
	}
	return matches
}

func (l *lookupVertsHasLabelCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	loadData := l.loadData
	var exists = true
	indexConds, _, indexed := buildIndexedConditions(l.expr)
	if indexed {
		for _, label := range l.labels {
			tID, err := l.db.driver.TableDr.LookupTableID(label)
			if err != nil {
				exists = false
				break
			}
			l.db.driver.Lock.RLock()
			tabel, ok := l.db.driver.TablesByID[tID]
			l.db.driver.Lock.RUnlock()
			if !ok {
				// Table loaded?
				// If not loaded, we don't know if field is indexed.
				// But fields are loaded at startup. So if table not in Tables, maybe fields are not loaded.
				// driver.Tables should contain all tables with fields?
				// driver.LoadFields() populates Tables for any table with fields.
				// So if not in Tables, implies no fields indexed?
				exists = false
				break
			}
			if !allIndexedForLabel(tabel, indexConds) {
				exists = false
				break
			}
		}
	}
	count := 0
	if !exists || !indexed {
		log.Debugln("Using base case processor lookupVertsHasLabelCondIndexProc")
		go func() {
			defer close(out)
			for t := range in {
				for _, label := range l.labels {
					// Use GetOrLoadTable
					tableFound, err := l.db.driver.GetOrLoadTable(label)
					if err != nil {
						log.Debugf("Table for label '%s' not found: %v", label, err)
						continue
					}
					if loadData {
						filter := &filter.GripQLFilter{Expression: l.expr}
						if len(l.projectedFields) == 0 {
							if rawScanner, ok := tableFound.TableStore.(rawDocScanner); ok {
								for doc := range rawScanner.ScanDocRaw(filter) {
									v := gdbi.DataElement{
										Label:    strings.TrimPrefix(label, key.VertexTablePrefix),
										ID:       doc.ID,
										RawJSON:  doc.Payload,
										ModeHint: gdbi.RowModeRaw,
									}
									count++
									out <- t.AddCurrent(&v)
								}
								continue
							}
						}
						stream := tableFound.ScanDoc(filter)
						if len(l.projectedFields) > 0 {
							stream = tableFound.ScanDocProjected(l.projectedFields, filter)
						}
						for roMaps := range stream {
							v := gdbi.Vertex{
								Label:  strings.TrimPrefix(label, key.VertexTablePrefix),
								Loaded: loadData,
								ID:     roMaps["_id"].(string),
							}
							if len(l.projectedFields) > 0 {
								v.Data = projectRowMap(roMaps, l.projectedFields)
							} else {
								v.Data = roMaps
							}
							count += 1
							out <- t.AddCurrent(&v)
						}
					} else {
						for roMaps := range tableFound.ScanId(&filter.GripQLFilter{Expression: l.expr}) {
							v := gdbi.Vertex{
								Label:  strings.TrimPrefix(label, key.VertexTablePrefix),
								Loaded: loadData,
								ID:     roMaps,
								Data:   map[string]any{},
							}
							count += 1
							out <- t.AddCurrent(&v)
						}
					}
				}
			}
		}()
	} else {
		log.Debugln("Using optimized custom processor lookupVertsHasLabelCondIndexProc")
		if loadData {
			go func() {
				defer close(out)
				for t := range in {
					if ctx.Err() != nil {
						return
					}
					for _, label := range l.labels {
						tableFound, err := l.db.driver.GetOrLoadTable(label)
						if err != nil {
							continue
						}
						if len(indexConds) == 1 {
							emitIndexedVertexBatches(
								ctx,
								tableFound,
								t,
								strings.TrimPrefix(label, key.VertexTablePrefix),
								l.projectedFields,
								l.db.driver.RowIdsByLabelFieldValue(strings.TrimPrefix(label, key.VertexTablePrefix), indexConds[0].key, indexConds[0].value, indexConds[0].op),
								out,
							)
							continue
						}
						emitIndexedVertexMapBatches(
							ctx,
							tableFound,
							t,
							strings.TrimPrefix(label, key.VertexTablePrefix),
							l.projectedFields,
							l.intersectLabelIndexRows(strings.TrimPrefix(label, key.VertexTablePrefix), indexConds),
							out,
						)
					}
				}
			}()
			return ctx
		}
		queryChan := make(chan gdbi.ElementLookup, 100)
		go func() {
			defer close(queryChan)
			for t := range in {
				for _, label := range l.labels {
					if len(indexConds) == 1 {
						for entry := range l.db.driver.RowIdsByLabelFieldValue(strings.TrimPrefix(label, key.VertexTablePrefix), indexConds[0].key, indexConds[0].value, indexConds[0].op) {
							queryChan <- gdbi.ElementLookup{
								ID:   string(entry.Key),
								Ref:  t,
								Meta: gdbi.LookupMeta{Opaque: entry.Loc, Fields: l.projectedFields},
							}
						}
						continue
					}
					for id, loc := range l.intersectLabelIndexRows(strings.TrimPrefix(label, key.VertexTablePrefix), indexConds) {
						queryChan <- gdbi.ElementLookup{
							ID:   id,
							Ref:  t,
							Meta: gdbi.LookupMeta{Opaque: loc, Fields: l.projectedFields},
						}
					}
				}
			}
		}()
		go func() {
			defer close(out)
			for v := range l.db.GetVertexChannel(ctx, queryChan, loadData) {
				i := v.Ref
				out <- i.AddCurrent(v.Vertex)
			}
		}()
	}
	return ctx
}

// //////////////////////////////////////////////////////////////////////////////
// LookupVertsCondIndexTraverse fused seed-filter + first-hop adjacency
type lookupVertsCondIndexTraverseStep struct {
	labels     []string
	expr       *gripql.HasExpression
	edgeLabels []string
	inbound    bool
	emitNull   bool
}

func (t lookupVertsCondIndexTraverseStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	return &lookupVertsCondIndexTraverseProc{
		db:         graph,
		labels:     t.labels,
		expr:       t.expr,
		edgeLabels: t.edgeLabels,
		inbound:    t.inbound,
		emitNull:   t.emitNull,
		loadData:   ps.StepLoadData(),
	}, nil
}

func (t lookupVertsCondIndexTraverseStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsCondIndexTraverseProc struct {
	db         *Graph
	labels     []string
	expr       *gripql.HasExpression
	edgeLabels []string
	inbound    bool
	emitNull   bool
	loadData   bool
}

const fusedSeedBatchSize = 4096

type seedLabelPlan struct {
	label    string
	table    *driver.BackendTable
	useIndex bool
}

type seedCandidate struct {
	traveler gdbi.Traveler
	id       string
	label    string
}

func (l *lookupVertsCondIndexTraverseProc) normalizeSeedLabels(exprLabels []string) []string {
	out := make([]string, 0, len(l.labels))
	seen := map[string]struct{}{}

	add := func(label string) {
		label = strings.TrimSpace(label)
		if label == "" {
			return
		}
		if strings.HasPrefix(label, key.VertexTablePrefix) {
			label = strings.TrimPrefix(label, key.VertexTablePrefix)
		}
		if _, ok := seen[label]; ok {
			return
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}

	if len(l.labels) > 0 {
		for _, label := range l.labels {
			add(label)
		}
	} else if len(exprLabels) > 0 {
		for _, label := range exprLabels {
			add(label)
		}
	} else {
		for label := range l.db.driver.GetLabels(false, true) {
			add(label)
		}
	}

	if len(exprLabels) == 0 {
		return out
	}

	exprSet := map[string]struct{}{}
	for _, label := range exprLabels {
		exprSet[label] = struct{}{}
	}

	filtered := make([]string, 0, len(out))
	for _, label := range out {
		if _, ok := exprSet[label]; ok {
			filtered = append(filtered, label)
		}
	}
	return filtered
}

func (l *lookupVertsCondIndexTraverseProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 2048)
	indexConds, exprLabels, indexed := buildIndexedConditions(l.expr)

	seedLabels := l.normalizeSeedLabels(exprLabels)
	plans := make([]seedLabelPlan, 0, len(seedLabels))
	for _, label := range seedLabels {
		table, err := l.db.driver.GetOrLoadTable(key.VertexTablePrefix + label)
		if err != nil {
			continue
		}
		useIndex := indexed && len(indexConds) > 0 && allIndexedForLabel(table, indexConds)
		plans = append(plans, seedLabelPlan{
			label:    label,
			table:    table,
			useIndex: useIndex,
		})
	}

	go func() {
		defer close(queryChan)
		seedFilter := &filter.GripQLFilter{Expression: l.expr}
		candidates := make([]seedCandidate, 0, fusedSeedBatchSize)

		flushCandidates := func() bool {
			if len(candidates) == 0 {
				return true
			}
			ids := make([]string, len(candidates))
			for i := range candidates {
				ids[i] = candidates[i].id
			}
			uids, err := l.db.driver.GetIDs(ids)
			if err != nil {
				log.Errorf("lookupVertsCondIndexTraverseProc: failed to resolve %d seed IDs: %v", len(ids), err)
				uids = nil
			}
			for i := range candidates {
				c := candidates[i]
				seed := &gdbi.Vertex{ID: c.id, Label: c.label}
				req := gdbi.ElementLookup{
					ID:  c.id,
					Ref: c.traveler.AddCurrent(seed),
				}
				if i < len(uids) && uids[i] != 0 {
					req.Meta = gdbi.LookupMeta{UID: uids[i]}
				}
				select {
				case <-ctx.Done():
					return false
				case queryChan <- req:
				}
			}
			candidates = candidates[:0]
			return true
		}

		addCandidate := func(t gdbi.Traveler, id string, label string) bool {
			if id == "" {
				return true
			}
			candidates = append(candidates, seedCandidate{
				traveler: t,
				id:       id,
				label:    label,
			})
			if len(candidates) >= fusedSeedBatchSize {
				return flushCandidates()
			}
			return true
		}

		for t := range in {
			if ctx.Err() != nil {
				return
			}
			for _, plan := range plans {
				if ctx.Err() != nil {
					return
				}
				if plan.useIndex {
					if len(indexConds) == 1 {
						for entry := range l.db.driver.RowIdsByLabelFieldValue(plan.label, indexConds[0].key, indexConds[0].value, indexConds[0].op) {
							if !addCandidate(t, string(entry.Key), plan.label) {
								return
							}
						}
						continue
					}
					for id := range (&lookupVertsHasLabelCondIndexProc{db: l.db}).intersectLabelIndexRows(plan.label, indexConds) {
						if !addCandidate(t, id, plan.label) {
							return
						}
					}
					continue
				}
				for id := range plan.table.ScanId(seedFilter) {
					if !addCandidate(t, id, plan.label) {
						return
					}
				}
			}
			if !flushCandidates() {
				return
			}
		}
		flushCandidates()
	}()

	go func() {
		defer close(out)
		var adjChan chan gdbi.ElementLookup
		if l.inbound {
			adjChan = l.db.GetInChannel(ctx, queryChan, l.loadData, l.emitNull, l.edgeLabels)
		} else {
			adjChan = l.db.GetOutChannel(ctx, queryChan, l.loadData, l.emitNull, l.edgeLabels)
		}
		for v := range adjChan {
			if v.Ref == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- v.Ref.AddCurrent(v.Vertex):
			}
		}
	}()
	return ctx
}

// //////////////////////////////////////////////////////////////////////////////
// LookupVertsCondIndex look up vertices by indexed
type lookupVertsCondIndexStep struct {
	expr *gripql.HasExpression
}

func (t lookupVertsCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	return &lookupVertsCondIndexProc{
		db:              graph,
		expr:            t.expr,
		loadData:        ps.StepLoadData(),
		projectedFields: normalizeProjectedFields(ps.StepRequiredFields()),
	}, nil
}

func (t lookupVertsCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsCondIndexProc struct {
	db              *Graph
	expr            *gripql.HasExpression
	loadData        bool
	projectedFields []string
	fallback        bool
}

func (l *lookupVertsCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	loadData := l.loadData
	log.Debugln("Entering lookupVertsCondIndexProc custom processor")
	indexConds, exprLabels, indexed := buildIndexedConditions(l.expr)

	/* Optimized indexing only works for Simple filters.
	   If compound filter or index doesn't exist, use backup method */
	if indexed {
		log.Debugln("Chose index optimized V().Has() statement path")
		if loadData {
			// Fast path for a single global index predicate with no explicit label constraints.
			// If expression carries _label predicates, we must stay on label-aware paths below.
			if len(indexConds) == 1 && len(exprLabels) == 0 && len(l.projectedFields) == 0 {
				go func() {
					defer close(out)
					start := time.Now()
					cond := indexConds[0]
					tcache := newTableTypeCache(l.db.driver)
					var produced int
					for t := range in {
						if ctx.Err() != nil {
							return
						}
						produced += emitGlobalIndexedVertexBatches(ctx, l.db.driver, t, cond, l.projectedFields, tcache, out)
						if ctx.Err() != nil {
							return
						}
					}
					log.Debugf("lookupVertsCondIndexProc global direct emit rows=%d elapsed=%s", produced, time.Since(start).Round(time.Millisecond))
				}()
				return ctx
			}
			go func() {
				defer close(out)
				start := time.Now()
				var produced int
				for t := range in {
					if ctx.Err() != nil {
						return
					}
					// Determine which labels to scan. If the AND expression contained
					// _label == X conditions, restrict to those labels; otherwise iterate all.
					labelsToScan := exprLabels
					if len(labelsToScan) == 0 {
						for label := range l.db.driver.GetLabels(false, true) {
							labelsToScan = append(labelsToScan, label)
						}
					}
					for _, label := range labelsToScan {
						table, err := l.db.driver.GetOrLoadTable(key.VertexTablePrefix + label)
						if err != nil {
							continue
						}
						if len(indexConds) == 0 {
							// Only _label conditions — use full expression via GripQLFilter scan.
							for doc := range table.TableStore.(rawDocScanner).ScanDocRaw(&filter.GripQLFilter{Expression: l.expr}) {
								v := &gdbi.DataElement{
									Label:    label,
									ID:       doc.ID,
									RawJSON:  doc.Payload,
									ModeHint: gdbi.RowModeRaw,
								}
								select {
								case <-ctx.Done():
									return
								case out <- t.AddCurrent(v):
								}
								produced++
							}
							continue
						}
						if len(indexConds) == 1 {
							produced += emitIndexedVertexBatches(
								ctx,
								table,
								t,
								label,
								l.projectedFields,
								l.db.driver.RowIdsByLabelFieldValue(label, indexConds[0].key, indexConds[0].value, indexConds[0].op),
								out,
							)
							continue
						}
						if !allIndexedForLabel(table, indexConds) {
							continue
						}
						produced += emitIndexedVertexMapBatches(
							ctx,
							table,
							t,
							label,
							l.projectedFields,
							(&lookupVertsHasLabelCondIndexProc{db: l.db}).intersectLabelIndexRows(label, indexConds),
							out,
						)
					}
				}
				log.Debugf("lookupVertsCondIndexProc direct emit completed rows=%d elapsed=%s", produced, time.Since(start).Round(time.Millisecond))
			}()
			return ctx
		}
		queryChan := make(chan gdbi.ElementLookup, 100)
		vertexLabels := []string{}
		for label := range l.db.driver.GetLabels(false, true) {
			vertexLabels = append(vertexLabels, label)
		}

		// Stream index matches per input traveler to avoid building large in-memory
		// caches that can stall under backpressure.
		go func() {
			defer close(queryChan)
			start := time.Now()
			var travelers int
			var totalMatches int
			tcache := newTableTypeCache(l.db.driver)
			for t := range in {
				if ctx.Err() != nil {
					return
				}
				travelers++
				matches := 0
				if len(indexConds) == 1 {
					if len(exprLabels) > 0 {
						cond := indexConds[0]
						for _, label := range exprLabels {
							for entry := range l.db.driver.RowIdsByLabelFieldValue(label, cond.key, cond.value, cond.op) {
								e := gdbi.ElementLookup{
									ID:   string(entry.Key),
									Ref:  t,
									Meta: gdbi.LookupMeta{Opaque: entry.Loc, Fields: l.projectedFields},
								}
								select {
								case <-ctx.Done():
									return
								case queryChan <- e:
								}
								matches++
								totalMatches++
							}
						}
					} else {
						cond := indexConds[0]
						count, ok := streamHasMatchesForTraveler(ctx, l.db.driver, t, cond, l.projectedFields, queryChan, tcache, true)
						matches += count
						totalMatches += count
						if !ok {
							return
						}
					}
				} else if len(vertexLabels) > 0 {
					for _, label := range vertexLabels {
						table, err := l.db.driver.GetOrLoadTable(key.VertexTablePrefix + label)
						if err != nil {
							continue
						}
						if !allIndexedForLabel(table, indexConds) {
							continue
						}
						for id, loc := range (&lookupVertsHasLabelCondIndexProc{db: l.db}).intersectLabelIndexRows(label, indexConds) {
							e := gdbi.ElementLookup{
								ID:   id,
								Ref:  t,
								Meta: gdbi.LookupMeta{Opaque: loc, Fields: l.projectedFields},
							}
							select {
							case <-ctx.Done():
								return
							case queryChan <- e:
							}
							matches++
							totalMatches++
						}
					}
				} else {
					// No label metadata available; preserve behavior with first-condition global index.
					cond := indexConds[0]
					count, ok := streamHasMatchesForTraveler(ctx, l.db.driver, t, cond, l.projectedFields, queryChan, tcache, true)
					matches += count
					totalMatches += count
					if !ok {
						return
					}
				}
				log.Debugf("Index lookup streamed %d rows for traveler=%d", matches, travelers)
			}
			log.Debugf("Index lookup completed travelers=%d totalMatches=%d elapsed=%s", travelers, totalMatches, time.Since(start).Round(time.Millisecond))
		}()
		// Process queryChan with GetVertexChannel for indexed case
		go func() {
			defer close(out)
			start := time.Now()
			var produced int
			for v := range l.db.GetVertexChannel(ctx, queryChan, loadData) {
				if ctx.Err() != nil {
					return
				}
				if v.Ref == nil || v.Vertex == nil {
					continue
				}
				i := v.Ref
				select {
				case <-ctx.Done():
					return
				case out <- i.AddCurrent(v.Vertex):
				}
				produced++
				if produced%10000 == 0 {
					log.Debugf("lookupVertsCondIndexProc emit progress rows=%d elapsed=%s", produced, time.Since(start).Round(time.Millisecond))
				}
			}
			log.Debugf("lookupVertsCondIndexProc emit completed rows=%d elapsed=%s", produced, time.Since(start).Round(time.Millisecond))
		}()
	} else {
		log.Debugf("Base case GetVertexList is used. No indexing")
		go func() {
			defer close(out)
			for t := range in {
				for _, tLabel := range l.db.driver.List() {
					if strings.HasPrefix(tLabel, key.VertexTablePrefix) {
						table, err := l.db.driver.GetOrLoadTable(tLabel)
						if err != nil {
							continue
						}
						if !loadData {
							for id := range table.ScanId(&filter.GripQLFilter{Expression: l.expr}) {
								vertex := gdbi.Vertex{
									ID:     id,
									Label:  strings.TrimPrefix(tLabel, key.VertexTablePrefix),
									Data:   map[string]any{},
									Loaded: false,
								}
								out <- t.AddCurrent(&vertex)
							}
							continue
						}
						filterExpr := &filter.GripQLFilter{Expression: l.expr}
						if len(l.projectedFields) == 0 {
							if rawScanner, ok := table.TableStore.(rawDocScanner); ok {
								for doc := range rawScanner.ScanDocRaw(filterExpr) {
									vertex := gdbi.DataElement{
										ID:       doc.ID,
										Label:    strings.TrimPrefix(tLabel, key.VertexTablePrefix),
										RawJSON:  doc.Payload,
										ModeHint: gdbi.RowModeRaw,
									}
									out <- t.AddCurrent(&vertex)
								}
								continue
							}
						}
						stream := table.ScanDoc(filterExpr)
						if len(l.projectedFields) > 0 {
							stream = table.ScanDocProjected(l.projectedFields, filterExpr)
						}
						for v := range stream {
							data := v
							if len(l.projectedFields) > 0 {
								data = projectRowMap(v, l.projectedFields)
							}
							vertex := gdbi.Vertex{
								ID:     v["_id"].(string),
								Label:  strings.TrimPrefix(tLabel, key.VertexTablePrefix),
								Data:   data,
								Loaded: true,
							}
							out <- t.AddCurrent(&vertex)
						}
					}
				}
			}
		}()
	}
	return ctx
}
