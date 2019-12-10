package grids

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/protoutil"
	log "github.com/sirupsen/logrus"
)

func (kgraph *GDB) setupGraphIndex(graph string) error {
	err := kgraph.idx.AddField(fmt.Sprintf("%s.v.label", graph))
	if err != nil {
		return fmt.Errorf("failed to setup index on vertex label")
	}
	err = kgraph.idx.AddField(fmt.Sprintf("%s.e.label", graph))
	if err != nil {
		return fmt.Errorf("failed to setup index on edge label")
	}
	return nil
}

func (kgraph *GDB) deleteGraphIndex(graph string) {
	fields := kgraph.idx.ListFields()
	for _, f := range fields {
		t := strings.Split(f, ".")
		if t[0] == graph {
			kgraph.idx.RemoveField(f)
		}
	}
}

func normalizePath(path string) string {
	path = jsonpath.GetJSONPath(path)
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "data.")
	return path
}

func vertexIdxStruct(v *gripql.Vertex) map[string]interface{} {
	k := map[string]interface{}{
		"v": map[string]interface{}{
			"label": v.Label,
			v.Label: protoutil.AsMap(v.Data),
		},
	}
	return k
}

func edgeIdxStruct(e *gripql.Edge) map[string]interface{} {
	k := map[string]interface{}{
		"e": map[string]interface{}{
			"label": e.Label,
			e.Label: protoutil.AsMap(e.Data),
		},
	}
	return k
}

//AddVertexIndex add index to vertices
func (ggraph *Graph) AddVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Adding vertex index")
	field = normalizePath(field)
	//TODO kick off background process to reindex existing data
	return ggraph.kdb.idx.AddField(fmt.Sprintf("%s.v.%s", ggraph.graphID, field))
}

//DeleteVertexIndex delete index from vertices
func (ggraph *Graph) DeleteVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Deleting vertex index")
	field = normalizePath(field)
	return ggraph.kdb.idx.RemoveField(fmt.Sprintf("%s.v.%s", ggraph.graphID, field))
}

//GetVertexIndexList lists out all the vertex indices for a graph
func (ggraph *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)
	go func() {
		defer close(out)
		fields := ggraph.kdb.idx.ListFields()
		for _, f := range fields {
			t := strings.Split(f, ".")
			if len(t) > 2 {
				out <- &gripql.IndexID{Graph: ggraph.graphID, Field: t[2]}
			}
		}
	}()
	return out
}

//VertexLabelScan produces a channel of all vertex ids in a graph
//that match a given label
func (ggraph *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	//TODO: Make this work better
	out := make(chan string, 100)
	go func() {
		defer close(out)
		//log.Printf("Searching %s %s", fmt.Sprintf("%s.label", ggraph.graph), label)
		for i := range ggraph.kdb.idx.GetTermMatch(ctx, fmt.Sprintf("%s.v.label", ggraph.graphID), label, 0) {
			//log.Printf("Found: %s", i)
			out <- i
		}
	}()
	return out
}

/*
//GetVertexTermAggregation get count of every term across vertices
func (ggraph *Graph) GetVertexTermAggregation(ctx context.Context, label string, field string, size uint32) (*gripql.AggregationResult, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "size": size}).Debug("Running GetVertexTermAggregation")
	out := &gripql.AggregationResult{
		Buckets: []*gripql.AggregationResultBucket{},
	}

	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = normalizePath(field)

	for tcount := range ggraph.kdb.idx.FieldTermCounts(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field)) {
		var t *structpb.Value
		if tcount.String != "" {
			t = protoutil.WrapValue(tcount.String)
		} else {
			t = protoutil.WrapValue(tcount.Number)
		}
		out.SortedInsert(&gripql.AggregationResultBucket{Key: t, Value: float64(tcount.Count)})
		if size > 0 {
			if len(out.Buckets) > int(size) {
				out.Buckets = out.Buckets[:size]
			}
		}
	}

	return out, nil
}

//GetVertexHistogramAggregation get binned counts of a term across vertices
func (ggraph *Graph) GetVertexHistogramAggregation(ctx context.Context, label string, field string, interval uint32) (*gripql.AggregationResult, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "interval": interval}).Debug("Running GetVertexHistogramAggregation")
	out := &gripql.AggregationResult{
		Buckets: []*gripql.AggregationResultBucket{},
	}

	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = normalizePath(field)

	min := ggraph.kdb.idx.FieldTermNumberMin(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field))
	max := ggraph.kdb.idx.FieldTermNumberMax(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field))

	i := float64(interval)
	for bucket := math.Floor(min/i) * i; bucket <= max; bucket += i {
		var count uint64
		for tcount := range ggraph.kdb.idx.FieldTermNumberRange(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field), bucket, bucket+i) {
			count += tcount.Count
		}
		out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: protoutil.WrapValue(bucket), Value: float64(count)})
	}

	return out, nil
}

//GetVertexPercentileAggregation get percentiles of a term across vertices
func (ggraph *Graph) GetVertexPercentileAggregation(ctx context.Context, label string, field string, percents []float64) (*gripql.AggregationResult, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "percents": percents}).Debug("Running GetVertexPercentileAggregation")
	out := &gripql.AggregationResult{
		Buckets: []*gripql.AggregationResultBucket{},
	}

	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = normalizePath(field)

	td := tdigest.New()
	for val := range ggraph.kdb.idx.FieldNumbers(fmt.Sprintf("%s.v.%s.%s", ggraph.graphID, label, field)) {
		td.Add(val, 1)
	}
	for _, p := range percents {
		q := td.Quantile(p / 100)
		out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: protoutil.WrapValue(p), Value: q})
	}

	return out, nil
}
*/
