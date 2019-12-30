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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
)

// AddVertexIndex add index to vertices
func (mg *Graph) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()

	_, err := idx.CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys: []string{"label", field},
			Options: options.Index().SetUnique(false).SetSparse(true).SetBackground(true),
	})
	if err != nil {
		return fmt.Errorf("failed create index %s %s %s: %v", label, field, err)
	}
	return nil
}

// DeleteVertexIndex delete index from vertices
func (mg *Graph) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()
	cursor, err := idx.List(context.TODO())
	var results []bson.M
	if err = cursor.All(context.TODO(), &results); err != nil {
	   return err
	}
	for _, rec := range results {
		recKeys := rec["key"].(bson.M)
		if _, ok := recKeys["label"]; ok {
			if _, ok := recKeys[field]; ok {
				idx.DropOne(context.TODO(), rec["name"].(string))
			}
		}
	}
	return nil
}

// GetVertexIndexList lists indices
func (mg *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)

	go func() {
		defer close(out)
		c := mg.ar.VertexCollection(mg.graph)

		// get all unique labels
		outLabels, err := c.Distinct(context.TODO(), "label", nil)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: finding distinct labels")
		}
		labels := make([]string, len(outLabels))
		for i := range outLabels {
			labels[i] = outLabels[i].(string)
		}

		// list indexed fields
		idx := c.Indexes()
		cursor, err := idx.List(context.TODO())
		var idxList []bson.M
		if err = cursor.All(context.TODO(), &idxList); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: finding indexed fields")
		}
		for _, rec := range idxList {
			recKeys := rec["key"].(bson.M)
			if len(recKeys) > 1 {
				if _, ok := recKeys["label"]; ok {
					key := ""
					for k := range recKeys {
						if k != "label" {
							key = k
						}
					}
					if len(key) > 0 {
						f := strings.TrimPrefix(key, "data.")
						for _, l := range labels {
							out <- &gripql.IndexID{Graph: mg.graph, Label: l, Field: f}
						}
					}
				}
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

	vcol := mg.ar.VertexCollection(mg.graph)
	cursor, err := vcol.Aggregate(context.TODO(), ag)
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{}
	for cursor.Next(context.TODO()) {
		cursor.Decode(&result)
		term := protoutil.WrapValue(result["_id"])
		count, ok := result["count"].(int)
		if !ok {
			return nil, fmt.Errorf("failed to cast count result to integer")
		}
		out.SortedInsert(&gripql.AggregationResultBucket{Key: term, Value: float64(count)})
	}
	if err := cursor.Close(context.TODO()); err != nil {
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

	vcol := mg.ar.VertexCollection(mg.graph)
	cursor, err := vcol.Aggregate(context.TODO(), ag)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())
	result := map[string]interface{}{}
	for cursor.Next(context.TODO()) {
		cursor.Decode(&result)
		term := protoutil.WrapValue(result["_id"])
		count, ok := result["count"].(int)
		if !ok {
			return nil, fmt.Errorf("failed to cast count result to integer")
		}
		out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: term, Value: float64(count)})
	}
	if err := cursor.Close(context.TODO()); err != nil {
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

	vcol := mg.ar.VertexCollection(mg.graph)
	cursor, err := vcol.Aggregate(context.TODO(), stmt)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())
	result := map[string]interface{}{}
	for cursor.Next(context.TODO()) {
		cursor.Decode(&result)
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
	if err := cursor.Close(context.TODO()); err != nil {
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
		selection := map[string]interface{}{
			"label": label,
		}
		vcol := mg.ar.VertexCollection(mg.graph)
		opts := options.Find()
		opts.SetProjection(map[string]interface{}{"_id": 1, "label": 1})

		cursor, err := vcol.Find(context.TODO(), selection, opts)
		if err == nil {
			defer cursor.Close(context.TODO())
			result := map[string]interface{}{}
			for cursor.Next(context.TODO()) {
				select {
				case <-ctx.Done():
					return
				default:
				}
				cursor.Decode(&result)
				//BUG: return stuff here
			}
			if err := cursor.Close(context.TODO()); err != nil {
				log.Errorln("VertexLabelScan error:", err)
			}
		}
	}()
	return out
}
