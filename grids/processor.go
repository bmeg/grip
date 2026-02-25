package grids

import (
	"context"
	"strings"
	"time"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/grip/gdbi"
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

func emitIndexedVertexBatches(ctx context.Context, table benchtop.TableStore, traveler gdbi.Traveler, label string, fields []string, in <-chan benchtop.Index, out gdbi.OutPipe) int {
	locs := make([]*benchtop.RowLoc, 0, resolveBatchSize)
	ids := make([]string, 0, resolveBatchSize)
	total := 0

	flush := func() bool {
		if len(locs) == 0 {
			return true
		}
		rows, errs := table.GetRows(locs)
		for i := range rows {
			if i >= len(errs) || errs[i] != nil {
				continue
			}
			v := gdbi.Vertex{
				ID:     ids[i],
				Label:  label,
				Data:   projectRowMap(rows[i], fields),
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

func (l *lookupVertsHasLabelCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	loadData := l.loadData
	var exists = true
	// Here if one of l.labels doesn't exist then not going to be querying all the data so leave it like this.
	cond := l.expr.GetCondition()
	// If condition is simple, we check if field is indexed.
	// But how to check without loading all tables?
	// We iterate labels. For each label, resolve ID, get table, check if field is indexed.
	if cond != nil {
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
			_, exists = tabel.Fields[cond.Key]
			if !exists {
				break
			}
		}
	}
	count := 0
	if !exists || (l.expr == nil && cond == nil) {
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
						stream := tableFound.ScanDoc(filter)
						if len(l.projectedFields) > 0 {
							stream = tableFound.ScanDocProjected(l.projectedFields, filter)
						}
						for roMaps := range stream {
							v := gdbi.Vertex{
								Label:  label[2:],
								Loaded: loadData,
								ID:     roMaps["_id"].(string),
							}
							delete(roMaps, "_id")
							v.Data = roMaps
							count += 1
							out <- t.AddCurrent(&v)
						}
					} else {
						for roMaps := range tableFound.ScanId(&filter.GripQLFilter{Expression: l.expr}) {
							v := gdbi.Vertex{
								Label:  label[2:],
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
					cond := l.expr.GetCondition()
					for _, label := range l.labels {
						tableFound, err := l.db.driver.GetOrLoadTable(label)
						if err != nil {
							continue
						}
						emitIndexedVertexBatches(
							ctx,
							tableFound,
							t,
							strings.TrimPrefix(label, key.VertexTablePrefix),
							l.projectedFields,
							l.db.driver.RowIdsByLabelFieldValue(label[2:], cond.Key, cond.Value.AsInterface(), filter.ToQueryCondition(cond.Condition)),
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
				cond := l.expr.GetCondition()
				for _, label := range l.labels {
					for entry := range l.db.driver.RowIdsByLabelFieldValue(label[2:], cond.Key, cond.Value.AsInterface(), filter.ToQueryCondition(cond.Condition)) {
						queryChan <- gdbi.ElementLookup{
							ID:   string(entry.Key),
							Ref:  t,
							Priv: lookupPriv{loc: entry.Loc, fields: l.projectedFields},
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
	cond := l.expr.GetCondition()

	/* Optimized indexing only works for Simple filters.
	   If compound filter or index doesn't exist, use backup method */
	if cond != nil {
		log.Debugln("Chose index optimized V().Has() statement path")
		if loadData {
			go func() {
				defer close(out)
				start := time.Now()
				var produced int
				for t := range in {
					if ctx.Err() != nil {
						return
					}
					for label := range l.db.driver.GetLabels(false, true) {
						table, err := l.db.driver.GetOrLoadTable(key.VertexTablePrefix + label)
						if err != nil {
							continue
						}
						produced += emitIndexedVertexBatches(
							ctx,
							table,
							t,
							label,
							l.projectedFields,
							l.db.driver.RowIdsByLabelFieldValue(
								label,
								cond.Key,
								cond.Value.AsInterface(),
								filter.ToQueryCondition(cond.Condition),
							),
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
			for t := range in {
				if ctx.Err() != nil {
					return
				}
				travelers++
				matches := 0
				if len(vertexLabels) == 0 {
					for entry := range l.db.driver.RowIdsByHas(
						cond.Key,
						cond.Value.AsInterface(),
						filter.ToQueryCondition(cond.Condition),
					) {
						e := gdbi.ElementLookup{
							ID:   string(entry.Key),
							Ref:  t,
							Priv: lookupPriv{loc: entry.Loc, fields: l.projectedFields},
						}
						select {
						case <-ctx.Done():
							return
						case queryChan <- e:
						}
						matches++
						totalMatches++
					}
				} else {
					for _, label := range vertexLabels {
						for entry := range l.db.driver.RowIdsByLabelFieldValue(
							label,
							cond.Key,
							cond.Value.AsInterface(),
							filter.ToQueryCondition(cond.Condition),
						) {
							e := gdbi.ElementLookup{
								ID:   string(entry.Key),
								Ref:  t,
								Priv: lookupPriv{loc: entry.Loc, fields: l.projectedFields},
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
						stream := table.ScanDoc(filterExpr)
						if len(l.projectedFields) > 0 {
							stream = table.ScanDocProjected(l.projectedFields, filterExpr)
						}
						for v := range stream {
							vertex := gdbi.Vertex{
								ID:     v["_id"].(string),
								Label:  strings.TrimPrefix(tLabel, key.VertexTablePrefix),
								Data:   v,
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
