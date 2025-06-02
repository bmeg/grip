package grids

import (
	"context"
	"fmt"

	"github.com/bmeg/benchtop"
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
				cond := has.Has.GetCondition()
				return cond != nil && cond.Condition == gripql.Condition_EQ
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[1].GetHas()
			cond := has.GetCondition()
			path := tpath.NormalizePath(cond.Key)
			value := cond.Value.GetStringValue()
			var optimized []*gripql.GraphStatement
			switch path {
			case "$_current._id":
				optimized = []*gripql.GraphStatement{
					{Statement: &gripql.GraphStatement_V{V: protoutil.NewListFromStrings([]string{value})}},
				}
			case "$_current._label":
				optimized = []*gripql.GraphStatement{
					{Statement: &gripql.GraphStatement_LookupVertsLabelIndex{Labels: []string{value}}},
				}
			default:
				optimized = []*gripql.GraphStatement{
					{
						Statement: &gripql.GraphStatement_EngineCustom{
							Desc:   "Grids Has Level Indexing",
							Custom: lookupVertsCondIndexStep{key: cond.Key, value: value},
						},
					},
				}
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
			if hasLabel, ok := pipe[1].GetStatement().(*gripql.GraphStatement_HasLabel); ok {
				return len(hasLabel.HasLabel.GetValues()) > 0
			}
			if has, ok := pipe[2].GetStatement().(*gripql.GraphStatement_Has); ok {
				cond := has.Has.GetCondition()
				return cond != nil && cond.Condition == gripql.Condition_EQ
			}
			return false
		},
		Replace: func(pipe []*gripql.GraphStatement) []*gripql.GraphStatement {
			has := pipe[2].GetHas()
			cond := has.GetCondition()
			path := tpath.NormalizePath(cond.Key)
			value := cond.Value.GetStringValue()
			var optimized []*gripql.GraphStatement
			switch path {
			default:
				optimized = []*gripql.GraphStatement{
					{
						Statement: &gripql.GraphStatement_EngineCustom{
							Desc:   "Grids Has Level Indexing",
							Custom: lookupVertsHasLabelCondIndexStep{key: cond.Key, value: value},
						},
					},
				}
			}
			return append(optimized, pipe[3:]...)
		},
	},
}

// //////////////////////////////////////////////////////////////////////////////
// LookupVertexHasLabelCondIndex look up vertices has label
type lookupVertsHasLabelCondIndexStep struct {
	key   string
	label string
	value string
	op    benchtop.OperatorType
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
					label:    t.label,
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
		label:    t.label,
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
	value    string
	label    string
	op       benchtop.OperatorType
	loadData bool
	fallback bool
}

func (l *lookupVertsHasLabelCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	if l.fallback {
		log.Debugf("No index found for %s falling back to GetVertexList", l.key)
		go func() {
			defer close(queryChan)
			for t := range in {
				rowChan, err := l.db.bsonkv.Tables[l.label].Scan(
					true,
					[]benchtop.FieldFilter{
						{Field: l.key, Value: l.value, Operator: benchtop.OpEqual},
					},
				)
				if err != nil {
					log.Errorln("Scan Process Err: ", err)
				}
				for v := range rowChan {
					val, keyExists := v[l.key]
					id, idExists := v["_id"].(string)
					// In cases where eq comparisons to 'None' values are made
					if l.value == "" && (!keyExists && idExists || val == nil) {
						queryChan <- gdbi.ElementLookup{ID: id, Ref: t}
					} else if (keyExists && idExists) && (val == l.value || fmt.Sprintf("%v", val) == l.value) {
						queryChan <- gdbi.ElementLookup{ID: id, Ref: t}
					}
				}
			}
		}()
	} else {
		go func() {
			defer close(queryChan)
			for t := range in {
				rowChan, err := l.db.VertexFilterLabelScan(ctx, l.label, l.key, l.value)
				if err != nil {
					log.Errorln("VertexFilterLabelScan Process Err: ", err)
				}
				for id := range rowChan {
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

// //////////////////////////////////////////////////////////////////////////////
// LookupVertsCondIndex look up vertices by indexed
type lookupVertsCondIndexStep struct {
	key   string
	value string
}

func (t lookupVertsCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	// If Field is indexed, use the special processor
	for _, fields := range graph.bsonkv.Fields {
		for field := range fields {
			if field == t.key {
				return &lookupVertsCondIndexProc{db: graph, key: t.key, value: t.value, fallback: false, loadData: true}, nil
			}
		}
	}
	return &lookupVertsCondIndexProc{db: graph, key: t.key, value: t.value, fallback: true, loadData: true}, nil
}

func (t lookupVertsCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsCondIndexProc struct {
	db       *Graph
	key      string
	value    string
	loadData bool
	fallback bool
}

func (l *lookupVertsCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	if l.fallback {
		log.Debugf("No index found for %s falling back to GetVertexList", l.key)
		go func() {
			defer close(queryChan)
			for t := range in {
				for v := range l.db.GetVertexList(ctx, true) {
					val, keyExists := v.Data[l.key]
					// In cases where eq comparisons to 'None' values are made
					if l.value == "" && (!keyExists || val == nil) {
						queryChan <- gdbi.ElementLookup{ID: v.ID, Ref: t}
					} else if keyExists && (val == l.value || fmt.Sprintf("%v", val) == l.value) {
						queryChan <- gdbi.ElementLookup{ID: v.ID, Ref: t}
					}
				}
			}
		}()
	} else {
		go func() {
			defer close(queryChan)
			for t := range in {
				for id := range l.db.VertexHasConditionScan(ctx, l.key, l.value) {
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
