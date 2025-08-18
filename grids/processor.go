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
	labels   []string
	expr     *gripql.HasExpression
	loadData bool
}

func (t lookupVertsHasLabelCondIndexStep) GetProcessor(db gdbi.GraphInterface, ps gdbi.PipelineState) (gdbi.Processor, error) {
	graph := db.(*Graph)
	return &lookupVertsHasLabelCondIndexProc{
		db:       graph,
		expr:     t.expr,
		labels:   t.labels,
		loadData: ps.StepLoadData(),
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
	log.Debugln("Entering lookupVertsHasLabelCondIndexProc custom processor", l.loadData)
	var exists = true
	// Here if one of l.labels doesn't exist then not going to be querying all the data so leave it like this.
	cond :=  l.expr.GetCondition()
	exists = len(l.db.bsonkv.Fields) > 0 && cond != nil
	if exists {
		for _, iterLabel := range l.labels {
			label, ok := l.db.bsonkv.Fields[iterLabel]
			if !ok {
				exists = false
				break
			}
			_, exists = label[cond.Key]
			if !exists{
				break
			}
		}
	}

	count :=0
	if !exists || (l.expr == nil && cond == nil) {
		go func() {
			defer close(out)
			for t := range in {
				for _, label := range l.labels {
					tableFound, ok := l.db.bsonkv.Tables[label]
					if !ok {
						log.Debugf("BSONTable for label '%s' is nil. Cannot scan.", label)
						continue
					}
					for roMaps := range tableFound.Scan(l.loadData, NewGripQLFilter(l.expr)) {
						v := gdbi.Vertex{
							Label:  label[2:],
							Loaded: l.loadData,
						}
						if l.loadData {
							v.ID = roMaps.(map[string]any)["_id"].(string)
							delete(roMaps.(map[string]any), "_id")
							v.Data = roMaps.(map[string]any)
						} else {
							v.ID = roMaps.(string)
							v.Data = map[string]any{}
						}
						count += 1
						out <- t.AddCurrent(v.Copy())
					}
				}
			}
		}()
	} else {
		queryChan := make(chan gdbi.ElementLookup, 100)
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
		db:       graph,
		expr:     t.expr,
		loadData: ps.StepLoadData()}, nil
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
	log.Debugln("Entering lookupVertsCondIndexProc custom processor")
	queryChan := make(chan gdbi.ElementLookup, 100)
	cond := l.expr.GetCondition()
	
	/*  Indexing only works if every vertex label is indexed for that specific field and it's only a condition Filter
	 otherwise this lookup will not fetch everything that was asked for */
	allMatch := len(l.db.bsonkv.Fields) > 0 && cond != nil
	if allMatch {
		for lbl := range l.db.bsonkv.GetLabels(false, false) {
			if val, exists := l.db.bsonkv.Fields[lbl]; exists {
				if _, ok := val[cond.Key]; !ok {
					allMatch = false
					break
				}
			} else {
				allMatch = false
				break
			}
		}
	} 
	/*  Optimized indexing only works for Simple filters.			  /
	/ 	If compound filter or index doesn't exist use backup method */
	if cond != nil && allMatch {
		log.Debugln("Chose index optimized V().Has() statement path")
		go func() {
			defer close(queryChan)
			for t := range in {
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
	} else {
		log.Debugf("Base case GetVertexList is used. No indexing")
		go func() {
			defer close(queryChan)
			filt := NewGripQLFilter(l.expr)
			for t := range in {
				for v := range l.db.GetVertexList(ctx, true) {
					log.Debugln("FILT: ", filt)
					if filt.Matches(
						AddSpecialFields(v),
					) {
						queryChan <- gdbi.ElementLookup{ID: v.ID, Ref: t}
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
