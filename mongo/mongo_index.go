package mongo

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//AddVertexIndex add index to vertices
func (mg *Graph) AddVertexIndex(label string, field string) error {
	log.Printf("Adding index: %s.%s", label, field)
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	session := mg.ar.pool.Get()
	session.ResetIndexCache()
	defer mg.ar.pool.Put(session)
	c := mg.ar.VertexCollection(session, mg.graph)
	return c.EnsureIndex(mgo.Index{
		Key:        []string{"label", field},
		Unique:     false,
		DropDups:   false,
		Sparse:     true,
		Background: true,
	})
}

//DeleteVertexIndex delete index from vertices
func (mg *Graph) DeleteVertexIndex(label string, field string) error {
	log.Printf("Deleting index: %s.%s", label, field)
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
	c := mg.ar.VertexCollection(session, mg.graph)
	return c.DropIndex("label", field)
}

//GetVertexIndexList lists indices
func (mg *Graph) GetVertexIndexList() chan aql.IndexID {
	out := make(chan aql.IndexID)

	go func() {
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		defer close(out)

		c := mg.ar.VertexCollection(session, mg.graph)

		// get all unique labels
		labels := []string{}
		pipe := c.Pipe([]bson.M{
			{"$sortByCount": "$label"},
		})
		iter := pipe.Iter()
		defer iter.Close()
		res := map[string]interface{}{}
		for iter.Next(&res) {
			labels = append(labels, res["_id"].(string))
		}
		if err := iter.Err(); err != nil {
			log.Println("GetVertexIndexList error:", err)
		}

		// list indexed fields
		idxList, err := c.Indexes()
		if err != nil {
			log.Printf("Failed to list indices: %s", err)
			return
		}

		for _, idx := range idxList {
			if len(idx.Key) > 1 && idx.Key[0] == "label" {
				f := strings.TrimPrefix(idx.Key[1], "data.")
				for _, l := range labels {
					out <- aql.IndexID{Graph: mg.graph, Label: l, Field: f}
				}
			}
		}
	}()

	return out
}

//GetVertexTermAggregation get count of every term across vertices
func (mg *Graph) GetVertexTermAggregation(ctx context.Context, label string, field string, size uint32) (*aql.AggregationResult, error) {
	log.Printf("Running GetVertexTermAggregation: { label: %s, field: %s size: %v}", label, field, size)
	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	out := &aql.AggregationResult{
		Buckets: []*aql.AggregationResultBucket{},
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

	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
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
		out.SortedInsert(&aql.AggregationResultBucket{Key: term, Value: float64(count)})
		if size > 0 {
			if len(out.Buckets) > int(size) {
				out.Buckets = out.Buckets[:size]
			}
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating: %v", err)
	}
	return out, nil
}

//GetVertexHistogramAggregation get binned counts of a term across vertices
func (mg *Graph) GetVertexHistogramAggregation(ctx context.Context, label string, field string, interval uint32) (*aql.AggregationResult, error) {
	log.Printf("Running GetVertexHistogramAggregation: { label: %s, field: %s interval: %v }", label, field, interval)
	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	out := &aql.AggregationResult{
		Buckets: []*aql.AggregationResultBucket{},
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

	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
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
		out.Buckets = append(out.Buckets, &aql.AggregationResultBucket{Key: term, Value: float64(count)})
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating: %v", err)
	}
	return out, nil
}

//GetVertexPercentileAggregation get percentiles of a term across vertices
func (mg *Graph) GetVertexPercentileAggregation(ctx context.Context, label string, field string, percents []float64) (*aql.AggregationResult, error) {
	log.Printf("Running GetVertexPercentileAggregation: { label: %s, field: %s percents: %v }", label, field, percents)
	namespace := jsonpath.GetNamespace(field)
	if namespace != jsonpath.Current {
		return nil, fmt.Errorf("invalid field path")
	}
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	out := &aql.AggregationResult{
		Buckets: []*aql.AggregationResultBucket{},
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

	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
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
		out.Buckets = append(out.Buckets, &aql.AggregationResultBucket{Key: term, Value: val})
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating: %v", err)
	}

	return out, nil
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (mg *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.Printf("Running VertexLabelScan for label: %s", label)
	out := make(chan string, 100)
	go func() {
		defer close(out)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
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
		if err := iter.Err(); err != nil {
			log.Println("VertexLabelScan error:", err)
		}

	}()
	return out
}

// //AddEdgeIndex add index to edges
// func (mg *Graph) AddEdgeIndex(label string, field string) error {
// 	session := mg.ar.pool.Get()
// 	c := mg.ar.getEdgeCollection(session, mg.graph)
// 	err := c.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
// 	mg.ar.pool.Put(session)
// 	return err
// }

// //DeleteEdgeIndex delete index from edges
// func (mg *Graph) DeleteEdgeIndex(label string, field string) error {
// 	session := mg.ar.pool.Get()
// 	defer mg.ar.pool.Put(session)
// 	ecol := mg.ar.getEdgeCollection(session, mg.graph)
// 	return ecol.DropIndex("label", "data."+field)
// }

// //GetEdgeTermCount get count of every term across edges
// func (mg *Graph) GetEdgeTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
// 	out := make(chan aql.IndexTermCount, 100)
// 	go func() {
// 		defer close(out)
// 		session := mg.ar.pool.Get()
// 		defer mg.ar.pool.Put(session)
// 		ag := []bson.M{
// 			{"$match": bson.M{"label": label}},
// 			{"$group": bson.M{"_id": "$data." + field, "count": bson.M{"$sum": 1}}},
// 		}
// 		ecol := mg.ar.getEdgeCollection(session, mg.graph)
// 		pipe := ecol.Pipe(ag)
// 		iter := pipe.Iter()
// 		defer iter.Close()
// 		result := map[string]interface{}{}
// 		for iter.Next(&result) {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			default:
// 			}
// 			term := structpb.Value{Kind: &structpb.Value_StringValue{StringValue: result["_id"].(string)}}
// 			idxit := aql.IndexTermCount{Term: &term, Count: int32(result["count"].(int))}
// 			out <- idxit
// 		}
// 	}()
// 	return out
// }

// //EdgeLabelScan produces a channel of all edge ids where the edge label matches `label`
// func (mg *Graph) EdgeLabelScan(ctx context.Context, label string) chan string {
// 	out := make(chan string, 100)
// 	go func() {
// 		defer close(out)
// 		session := mg.ar.pool.Get()
// 		defer mg.ar.pool.Put(session)
// 		selection := map[string]interface{}{
// 			"label": label,
// 		}
// 		ecol := mg.ar.getEdgeCollection(session, mg.graph)
// 		iter := ecol.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
// 		defer iter.Close()
// 		result := map[string]interface{}{}
// 		for iter.Next(&result) {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			default:
// 			}
// 			id := result["_id"]
// 			if idb, ok := id.(bson.ObjectId); ok {
// 				out <- idb.String()
// 			} else {
// 				out <- id.(string)
// 			}
// 		}
// 	}()
// 	return out
// }
