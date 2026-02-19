package grids

import (
	"context"
	"strings"

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

func (l *lookupVertsHasLabelCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
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
					if l.loadData {
						filter := &filter.GripQLFilter{Expression: l.expr}
						stream := tableFound.ScanDoc(filter)
						if len(l.projectedFields) > 0 {
							stream = tableFound.ScanDocProjected(l.projectedFields, filter)
						}
						for roMaps := range stream {
							v := gdbi.Vertex{
								Label:  label[2:],
								Loaded: l.loadData,
								ID:     roMaps["_id"].(string),
							}
							delete(roMaps, "_id")
							v.Data = roMaps
							count += 1
							out <- t.AddCurrent(v.Copy())
						}
					} else {
						for roMaps := range tableFound.ScanId(&filter.GripQLFilter{Expression: l.expr}) {
							v := gdbi.Vertex{
								Label:  label[2:],
								Loaded: l.loadData,
								ID:     roMaps,
								Data:   map[string]any{},
							}
							count += 1
							out <- t.AddCurrent(v.Copy())
						}
					}
				}
			}
		}()
	} else {
		log.Debugln("Using optimized custom processor lookupVertsHasLabelCondIndexProc")
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
			for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
				i := v.Ref
				out <- i.AddCurrent(v.Vertex.Copy())
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
	log.Debugln("Entering lookupVertsCondIndexProc custom processor")
	cond := l.expr.GetCondition()

	/* Indexing only works if every vertex label is indexed for that specific field and it's only a condition Filter
	   otherwise this lookup will not fetch everything that was asked for */
	allMatch := cond != nil
	if allMatch {
		// Check across all vertex labels
		for _, tableName := range l.db.driver.List() {
			if !strings.HasPrefix(tableName, key.VertexTablePrefix) {
				continue
			}
			// Check if field is indexed
			tID, err := l.db.driver.TableDr.LookupTableID(tableName)
			if err != nil {
				allMatch = false
				break
			}
			l.db.driver.Lock.RLock()
			table, exists := l.db.driver.TablesByID[tID]
			l.db.driver.Lock.RUnlock()

			if exists {
				if _, ok := table.Fields[cond.Key]; !ok {
					allMatch = false
					break
				}
			} else {
				// Not in Tables map means no indexed fields loaded?
				allMatch = false
				break
			}
		}
	}

	/* Optimized indexing only works for Simple filters.
	   If compound filter or index doesn't exist, use backup method */
	if cond != nil && allMatch {
		log.Debugln("Chose index optimized V().Has() statement path")
		queryChan := make(chan gdbi.ElementLookup, 100)
		go func() {
			defer close(queryChan)
			for t := range in {
				for entry := range l.db.driver.RowIdsByHas(
					cond.Key,
					cond.Value.AsInterface(),
					filter.ToQueryCondition(cond.Condition),
				) {
					queryChan <- gdbi.ElementLookup{
						ID:   string(entry.Key),
						Ref:  t,
						Priv: lookupPriv{loc: entry.Loc, fields: l.projectedFields},
					}
				}
			}
		}()
		// Process queryChan with GetVertexChannel for indexed case
		go func() {
			defer close(out)
			for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
				i := v.Ref
				out <- i.AddCurrent(v.Vertex.Copy())
			}
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
						filter := &filter.GripQLFilter{Expression: l.expr}
						stream := table.ScanDoc(filter)
						if l.loadData && len(l.projectedFields) > 0 {
							stream = table.ScanDocProjected(l.projectedFields, filter)
						}
						for v := range stream {
							vertex := gdbi.Vertex{
								ID:     v["_id"].(string),
								Label:  strings.TrimPrefix(tLabel, key.VertexTablePrefix), // Extract label from table name
								Data:   v,                                                 // Use full data from ScanDoc
								Loaded: l.loadData,                                        // Set Loaded based on l.loadData
							}
							// Send directly to out channel
							out <- t.AddCurrent(vertex.Copy())
						}
					}
				}
			}
		}()
	}
	return ctx
}
