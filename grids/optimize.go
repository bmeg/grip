package grids

import (
	"context"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/bsontable"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/protoutil"
)

type OptimizationRule struct {
	Match   func(pipe []*gripql.GraphStatement) bool
	Replace func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement
}

func expandHasAnd(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	expanded := []*gripql.GraphStatement{}
	for _, step := range pipe {
		if has, ok := step.GetStatement().(*gripql.GraphStatement_Has); ok {
			if and := has.Has.GetAnd(); and != nil {
				for _, expr := range and.Expressions {
					expanded = append(expanded, &gripql.GraphStatement{Statement: &gripql.GraphStatement_Has{Has: expr}})
				}
			} else {
				expanded = append(expanded, step)
			}
		} else {
			expanded = append(expanded, step)
		}
	}
	return expanded
}

func GripOptimizer(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
	// Preprocess to handle Has with And expressions
	pipe = expandHasAnd(pipe)
	// Apply the first matching optimization rule
	for _, rule := range startOptimizations {
		if rule.Match(pipe) {
			return rule.Replace(pipe)
		}
	}
	// Return the original pipeline if no optimizations apply
	return pipe
}

var startOptimizations = []OptimizationRule{
	{
		Match: func(pipe []*gripql.GraphStatement) bool {
			if len(pipe) < 2 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if has, ok := pipe[1].GetStatement().(*gripql.GraphStatement_Has); ok {
				return has.Has.GetCondition() != nil
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			cond := pipe[1].GetHas().GetCondition()
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids Has Level Indexing",
						Custom: lookupVertsCondIndexStep{
							key:   cond.Key,
							value: cond.Value.AsInterface(),
							op:    MapConditionToOperator(cond.GetCondition())},
					},
				},
			}

			return append(optimized, pipe[2:]...)
		},
	},
	{
		Match: func(pipe []*gripql.GraphStatement) bool {
			if len(pipe) < 3 {
				return false
			}
			if _, ok := pipe[0].GetStatement().(*gripql.GraphStatement_V); !ok {
				return false
			}
			if _, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasLabel); !ok {
				return false
			}
			if has, ok := pipe[2].GetStatement().(*gripql.GraphStatement_Has); ok {
				return has.Has.GetCondition() != nil
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[2].GetHas()
			labels := protoutil.AsStringList(pipe[1].GetHasLabel())
			for i, label := range labels {
				if label[:2] != VTABLE_PREFIX {
					labels[i] = VTABLE_PREFIX + label
				}
			}
			cond := has.GetCondition()
			var optimized = []*gripql.GraphStatement{
				{
					Statement: &gripql.GraphStatement_EngineCustom{
						Desc: "Grids Has Level Indexing",
						Custom: lookupVertsHasLabelCondIndexStep{
							key:    cond.Key,
							value:  cond.Value.AsInterface(),
							labels: labels,
							op:     MapConditionToOperator(cond.GetCondition()),
						},
					},
				},
			}

			return append(optimized, pipe[3:]...)
		},
	},
}

// //////////////////////////////////////////////////////////////////////////////
// LookupVertexHasLabelCondIndex look up vertices has label

type lookupVertsHasLabelCondIndexStep struct {
	key    string
	labels []string
	value  any
	op     benchtop.OperatorType
}

func (t lookupVertsHasLabelCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	// If Field is indexed, use the special processor
	for _, fields := range graph.bsonkv.Fields {
		for field := range fields {
			if field == t.key {
				return &lookupVertsHasLabelCondIndexProc{
					db:       graph,
					key:      t.key,
					value:    t.value,
					labels:   t.labels,
					op:       t.op,
					fallback: false,
					loadData: true}, nil
			}
		}
	}
	return &lookupVertsHasLabelCondIndexProc{
		db:       graph,
		key:      t.key,
		value:    t.value,
		labels:   t.labels,
		op:       t.op,
		fallback: true,
		loadData: true}, nil
}

func (t lookupVertsHasLabelCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsHasLabelCondIndexProc struct {
	db       *Graph
	key      string
	value    any
	labels   []string
	op       benchtop.OperatorType
	loadData bool
	fallback bool
}

func (l *lookupVertsHasLabelCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	log.Debugln("Entering lookupVertsHasLabelCondIndexProc custom processor")
	queryChan := make(chan gdbi.ElementLookup, 100)
	if l.fallback {
		log.Debugf("lookupVertsHasLabelCondIndexProc: No index found for %s falling back to GetVertexList", l.key)
		go func() {
			defer close(queryChan)
			for t := range in {
				for _, label := range l.labels {
					tableFound, ok := l.db.bsonkv.Tables[label]
					if !ok {
						log.Errorf("BSONTable for label '%s' is nil. Cannot scan.", label)
						continue
					}
					rowChan, err := tableFound.Scan(
						true,
						[]benchtop.FieldFilter{
							{Field: l.key, Value: l.value, Operator: l.op},
						},
					)
					if err != nil {
						log.Errorln("Scan Process Err: ", err)
					}
					for v := range rowChan {
						id, idExists := v["_key"].(string)
						if idExists {
							queryChan <- gdbi.ElementLookup{ID: id, Ref: t}
						}
					}
				}
			}
		}()
	} else {
		go func() {
			defer close(queryChan)
			for t := range in {
				for _, label := range l.labels {
					for id := range l.db.bsonkv.RowIdsByLabelFieldValue(label, l.key, l.value, l.op) {
						queryChan <- gdbi.ElementLookup{
							ID:  id,
							Ref: t,
						}
					}
				}

			}
		}()
	}

	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			out <- i.AddCurrent(v.Vertex.Copy())
		}
	}()
	return ctx

}

// //////////////////////////////////////////////////////////////////////////////
// LookupVertsCondIndex look up vertices by indexed
type lookupVertsCondIndexStep struct {
	key   string
	value any
	op    benchtop.OperatorType
}

func (t lookupVertsCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	// If Field is indexed, use the special processor
	for _, fields := range graph.bsonkv.Fields {
		for field := range fields {
			if field == t.key {
				return &lookupVertsCondIndexProc{
					db:       graph,
					key:      t.key,
					value:    t.value,
					op:       t.op,
					fallback: false,
					loadData: true}, nil
			}
		}
	}
	return &lookupVertsCondIndexProc{
		db:       graph,
		key:      t.key,
		value:    t.value,
		op:       t.op,
		fallback: true,
		loadData: true}, nil
}

func (t lookupVertsCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsCondIndexProc struct {
	db       *Graph
	key      string
	value    any
	op       benchtop.OperatorType
	loadData bool
	fallback bool
}

func AddSpecialFields(v *gdbi.Vertex, path string) any {
	switch tpath.NormalizePath(path) {
	case "$_current._label":
		v.Data["_label"] = v.Label
	case "$_current._id":
		v.Data["_id"] = v.ID
	}
	return v.Data
}

func (l *lookupVertsCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	log.Debugln("Entering lookupVertsCondIndexProc custom processor")
	queryChan := make(chan gdbi.ElementLookup, 100)
	if l.fallback {
		log.Debugf("lookupVertsCondIndexProc: No index found for %s falling back to GetVertexList", l.key)
		go func() {
			defer close(queryChan)
			for t := range in {
				for v := range l.db.GetVertexList(ctx, true) {
					if bsontable.PassesFilters(
						AddSpecialFields(v, l.key),
						[]benchtop.FieldFilter{
							{Field: l.key, Value: l.value, Operator: l.op},
						}) {
						queryChan <- gdbi.ElementLookup{ID: v.ID, Ref: t}
					}

				}
			}
		}()
	} else {
		go func() {
			defer close(queryChan)
			for t := range in {
				for id := range l.db.bsonkv.RowIdsByHas(l.key, l.value, l.op) {
					queryChan <- gdbi.ElementLookup{
						ID:  id,
						Ref: t,
					}
				}

			}
		}()
	}

	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			out <- i.AddCurrent(v.Vertex.Copy())
		}
	}()
	return ctx

}
