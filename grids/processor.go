package grids

import (
	"context"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

// //////////////////////////////////////////////////////////////////////////////
// LookupVertexHasLabelCondIndex look up vertices has label

type lookupVertsHasLabelCondIndexStep struct {
	labels []string
	expr   *gripql.HasExpression
}

func (t lookupVertsHasLabelCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	return &lookupVertsHasLabelCondIndexProc{
		db:       graph,
		expr:     t.expr,
		labels:   t.labels,
		loadData: true,
	}, nil

}

func (t lookupVertsHasLabelCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsHasLabelCondIndexProc struct {
	db       *Graph
	labels   []string
	expr     *gripql.HasExpression
	loadData bool
}

func (l *lookupVertsHasLabelCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	log.Debugln("Entering lookupVertsHasLabelCondIndexProc custom processor")
	queryChan := make(chan gdbi.ElementLookup, 100)
	var exists = false
	if len(l.db.bsonkv.Fields) > 0 {
		for _, label := range l.labels {
			log.Debugln("Checking indexed fields %v", l.db.bsonkv.Fields, "LABEL: ", label)
			_, exists = l.db.bsonkv.Fields[label]
			if exists {
				break
			}
		}
	}

	if l.expr.GetCondition() == nil || !exists {
		log.Debugf("cond == nil || !exists: ", l.expr.GetCondition(), exists)
		go func() {
			defer close(queryChan)
			for t := range in {
				for _, label := range l.labels {
					tableFound, ok := l.db.bsonkv.Tables[label]
					if !ok {
						log.Errorf("BSONTable for label '%s' is nil. Cannot scan.", label)
						continue
					}
					for id := range tableFound.Scan(true, &GripQLFilter{Expression: l.expr}) {
						queryChan <- gdbi.ElementLookup{ID: id.(string), Ref: t}
					}
				}
			}
		}()
	} else {
		go func() {
			defer close(queryChan)
			for t := range in {
				cond := l.expr.GetCondition()
				for _, label := range l.labels {
					for id := range l.db.bsonkv.RowIdsByLabelFieldValue(label, cond.Key, cond.Value.AsInterface(), MapConditionToOperator(cond.Condition)) {
						queryChan <- gdbi.ElementLookup{ID: id, Ref: t}
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
	expr *gripql.HasExpression
}

func (t lookupVertsCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	return &lookupVertsCondIndexProc{
		db:       graph,
		expr:     t.expr,
		loadData: true}, nil
}

func (t lookupVertsCondIndexStep) GetType() gdbi.DataType {
	return gdbi.VertexData
}

type lookupVertsCondIndexProc struct {
	db       *Graph
	expr     *gripql.HasExpression
	loadData bool
	fallback bool
}

func (l *lookupVertsCondIndexProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	log.Debugln("Entering lookupVertsCondIndexProc custom processor", l.expr.Expression)
	queryChan := make(chan gdbi.ElementLookup, 100)
	cond := l.expr.GetCondition()
	var exists = false
	if len(l.db.bsonkv.Fields) > 0 {
		_, exists = l.db.bsonkv.Fields[cond.Key]
	}
	/*  Optimized indexing only works for Simple filters.			  /
	/ 	If compound filter or index doesn't exist use backup method */
	if cond == nil || !exists {
		log.Debugf("lookupVertsCondIndexProc: falling back to GetVertexList since filter is not basic Condition filter")
		go func() {
			defer close(queryChan)
			for t := range in {
				for v := range l.db.GetVertexList(ctx, true) {
					if MatchesHasExpression(
						AddSpecialFields(v),
						l.expr,
					) {
						queryChan <- gdbi.ElementLookup{ID: v.ID, Ref: t}
					}

				}
			}
		}()
	} else {
		go func() {
			defer close(queryChan)
			for t := range in {
				cond := l.expr.GetCondition()
				for id := range l.db.bsonkv.RowIdsByHas(
					cond.Key,
					cond.Value.AsInterface(),
					MapConditionToOperator(cond.Condition),
				) {
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

func AddSpecialFields(v *gdbi.Vertex) any {
	v.Data["_label"] = v.Label
	v.Data["_id"] = v.ID
	return v.Data
}
