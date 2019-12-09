package mongo

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// AddVertexIndex add index to vertices
func (mg *Graph) AddVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Adding vertex index")
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	session := mg.ar.session.Copy()
	defer session.Close()
	session.ResetIndexCache()
	c := mg.ar.VertexCollection(session, mg.graph)
	return c.EnsureIndex(mgo.Index{
		Key:        []string{field},
		Unique:     false,
		DropDups:   false,
		Sparse:     true,
		Background: true,
	})
}

// DeleteVertexIndex delete index from vertices
func (mg *Graph) DeleteVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Deleting vertex index")
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	session := mg.ar.session.Copy()
	defer session.Close()
	c := mg.ar.VertexCollection(session, mg.graph)
	return c.DropIndex(field)
}

// GetVertexIndexList lists indices
func (mg *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)

	go func() {
		session := mg.ar.session.Copy()
		defer session.Close()
		defer close(out)

		c := mg.ar.VertexCollection(session, mg.graph)

		// list indexed fields
		idxList, err := c.Indexes()
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: listing indices")
			return
		}

		for _, idx := range idxList {
			if len(idx.Key) > 1 {
				f := strings.TrimPrefix(idx.Key[1], "data.")
				out <- &gripql.IndexID{Graph: mg.graph, Field: f}
			}
		}
	}()

	return out
}

// GetVertexTermAggregation get count of every term across vertices
func (mg *Graph) GetVertexTermAggregation(ctx context.Context, label string, field string, size uint32) (*gripql.AggregationResult, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "size": size}).Debug("Running GetVertexTermAggregation")
	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	out := &gripql.AggregationResult{
		Buckets: []*gripql.AggregationResultBucket{},
	}

	ag := []bson.M{
		{
			"$match": bson.M{
				"label": label,
				field:   bson.M{"$exists": true},
			},
		},
		{
			"$sortByCount": "$" + field,
		},
	}
	if size > 0 {
		ag = append(ag, bson.M{"$limit": size})
	}

	session := mg.ar.session.Copy()
	defer session.Close()
	vcol := mg.ar.VertexCollection(session, mg.graph)
	pipe := vcol.Pipe(ag)
	iter := pipe.Iter()
	defer iter.Close()
	result := map[string]interface{}{}
	for iter.Next(&result) {
		term := protoutil.WrapValue(result["_id"])
		count, ok := result["count"].(int)
		if !ok {
			return nil, fmt.Errorf("failed to cast count result to integer")
		}
		out.SortedInsert(&gripql.AggregationResultBucket{Key: term, Value: float64(count)})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating: %v", err)
	}
	return out, nil
}

// GetVertexHistogramAggregation get binned counts of a term across vertices
func (mg *Graph) GetVertexHistogramAggregation(ctx context.Context, label string, field string, interval uint32) (*gripql.AggregationResult, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "interval": interval}).Debug("Running GetVertexHistogramAggregation")
	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	out := &gripql.AggregationResult{
		Buckets: []*gripql.AggregationResultBucket{},
	}

	ag := []bson.M{
		{
			"$match": bson.M{
				"label": label,
				field:   bson.M{"$exists": true},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"$multiply": []interface{}{interval, bson.M{"$floor": bson.M{"$divide": []interface{}{"$" + field, interval}}}},
				},
				"count": bson.M{"$sum": 1},
			},
		},
		{
			"$sort": bson.M{"_id": 1},
		},
	}

	session := mg.ar.session.Copy()
	defer session.Close()
	vcol := mg.ar.VertexCollection(session, mg.graph)
	pipe := vcol.Pipe(ag)
	iter := pipe.Iter()
	defer iter.Close()
	result := map[string]interface{}{}
	for iter.Next(&result) {
		term := protoutil.WrapValue(result["_id"])
		count, ok := result["count"].(int)
		if !ok {
			return nil, fmt.Errorf("failed to cast count result to integer")
		}
		out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: term, Value: float64(count)})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating: %v", err)
	}
	return out, nil
}

// GetVertexPercentileAggregation get percentiles of a term across vertices
func (mg *Graph) GetVertexPercentileAggregation(ctx context.Context, label string, field string, percents []float64) (*gripql.AggregationResult, error) {
	log.WithFields(log.Fields{"label": label, "field": field, "percents": percents}).Debug("Running GetVertexPercentileAggregation")
	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	out := &gripql.AggregationResult{
		Buckets: []*gripql.AggregationResultBucket{},
	}

	stmt := []bson.M{
		{
			"$match": bson.M{
				"label": label,
				field:   bson.M{"$exists": true},
			},
		},
		{
			"$sort": bson.M{field: 1},
		},
		{
			"$group": bson.M{
				"_id":    "null",
				"values": bson.M{"$push": "$" + field},
			},
		},
	}
	percentiles := []interface{}{}
	for _, p := range percents {
		pName := strings.Replace(fmt.Sprintf("%v", p), ".", "_", -1)
		percentile := bson.M{}
		percentile["_id"] = pName
		percentile["count"] = percentileCalc(p)
		percentiles = append(percentiles, percentile)
	}
	stmt = append(stmt, bson.M{"$project": bson.M{"results": percentiles}})
	stmt = append(stmt, bson.M{"$unwind": "$results"})
	stmt = append(stmt, bson.M{"$project": bson.M{"_id": "$results._id", "count": "$results.count"}})

	session := mg.ar.session.Copy()
	defer session.Close()
	vcol := mg.ar.VertexCollection(session, mg.graph)
	pipe := vcol.Pipe(stmt)
	iter := pipe.Iter()
	defer iter.Close()
	result := map[string]interface{}{}
	for iter.Next(&result) {
		bid := strings.Replace(result["_id"].(string), "_", ".", -1)
		f, err := strconv.ParseFloat(bid, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse percentile aggregation result key: %v", err)
		}
		term := protoutil.WrapValue(f)
		val, ok := result["count"].(float64)
		if !ok {
			return nil, fmt.Errorf("error occurred parsing mongo output: %v", result)
		}
		out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: term, Value: val})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating: %v", err)
	}

	return out, nil
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (mg *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	out := make(chan string, 100)
	go func() {
		defer close(out)
		session := mg.ar.session.Copy()
		defer session.Close()
		selection := map[string]interface{}{
			"label": label,
		}
		vcol := mg.ar.VertexCollection(session, mg.graph)
		iter := vcol.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			id := result["_id"]
			if idb, ok := id.(bson.ObjectId); ok {
				out <- idb.String()
			} else {
				out <- id.(string)
			}
		}
		if err := iter.Close(); err != nil {
			log.Errorln("VertexLabelScan error:", err)
		}

	}()
	return out
}
