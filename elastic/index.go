package elastic

import (
	"context"
	//"fmt"
	"io"
	//"strconv"
	//"strings"

	"github.com/bmeg/grip/gripql"
	//"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	//"github.com/bmeg/grip/protoutil"
	elastic "gopkg.in/olivere/elastic.v5"
)

// AddVertexIndex adds a new field to be indexed
func (es *Graph) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	return nil
}

// DeleteVertexIndex removes a vertex field index
func (es *Graph) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	return nil
}

// GetVertexIndexList gets list if vertex indices
func (es *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	ctx := context.Background()

	o := make(chan *gripql.IndexID)
	go func() {
		defer close(o)

		// get all unique labels
		q := es.client.Search().Index(es.vertexIndex).Type("vertex")
		aggName := "labels.aggregation"
		q = q.Aggregation(aggName, elastic.NewTermsAggregation().Field("label").Size(1000000).OrderByCountDesc())
		res, err := q.Do(ctx)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: label term count failed")
			return
		}

		labels := []string{}
		if agg, found := res.Aggregations.Terms(aggName); found {
			for _, bucket := range agg.Buckets {
				labels = append(labels, bucket.Key.(string))
			}
		}

		// list indexed fields
		mapping, err := es.client.GetMapping().Index(es.vertexIndex).Type("vertex").Do(ctx)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: get field mapping failed")
			return
		}

		var data map[string]interface{}
		if props, ok := mapping[es.vertexIndex].(map[string]interface{}); ok {
			if props, ok = props["mappings"].(map[string]interface{}); ok {
				if props, ok = props["vertex"].(map[string]interface{}); ok {
					if props, ok = props["properties"].(map[string]interface{}); ok {
						if props, ok = props["data"].(map[string]interface{}); ok {
							if props, ok = props["properties"].(map[string]interface{}); ok {
								data = props
							}
						}
					}
				}
			}
		}

		for k := range data {
			for _, l := range labels {
				o <- &gripql.IndexID{Graph: es.graph, Label: l, Field: k}
			}
		}
	}()

	return o
}

/*
// GetVertexTermAggregation returns the count of every term across vertices
func (es *Graph) GetVertexTermAggregation(ctx context.Context, label string, field string, size uint32) (*gripql.AggregationResult, error) {
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

	q := es.client.Search().Index(es.vertexIndex).Type("vertex")
	q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label)))
	aggName := fmt.Sprintf("term.aggregation.%s.%s", label, field)
	if size == 0 {
		size = 1000000
	}
	q = q.Aggregation(aggName,
		elastic.NewTermsAggregation().Field(field+".keyword").Size(int(size)).OrderByCountDesc())
	res, err := q.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("term count failed: %s", err)
	}
	if agg, found := res.Aggregations.Terms(aggName); found {
		for _, bucket := range agg.Buckets {
			term := protoutil.WrapValue(bucket.Key.(string))
			out.SortedInsert(&gripql.AggregationResultBucket{Key: term, Value: float64(bucket.DocCount)})
			if size > 0 {
				if len(out.Buckets) > int(size) {
					out.Buckets = out.Buckets[:size]
				}
			}
		}
	}

	return out, nil
}
*/

/*
//GetVertexHistogramAggregation get binned counts of a term across vertices
func (es *Graph) GetVertexHistogramAggregation(ctx context.Context, label string, field string, interval uint32) (*gripql.AggregationResult, error) {
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

	q := es.client.Search().Index(es.vertexIndex).Type("vertex")
	q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label)))
	aggName := fmt.Sprintf("histogram.aggregation.%s.%s", label, field)
	q = q.Aggregation(aggName,
		elastic.NewHistogramAggregation().Field(field).Interval(float64(interval)).OrderByKeyAsc())
	res, err := q.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("histogram aggregation failed: %s", err)
	}
	if agg, found := res.Aggregations.Histogram(aggName); found {
		for _, bucket := range agg.Buckets {
			term := protoutil.WrapValue(bucket.Key)
			out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: term, Value: float64(bucket.DocCount)})
		}
	}

	return out, nil
}
*/

/*
//GetVertexPercentileAggregation get percentiles of a term across vertices
func (es *Graph) GetVertexPercentileAggregation(ctx context.Context, label string, field string, percents []float64) (*gripql.AggregationResult, error) {
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

	q := es.client.Search().Index(es.vertexIndex).Type("vertex")
	q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label)))
	aggName := fmt.Sprintf("percentile.aggregation.%s.%s", label, field)
	q = q.Aggregation(aggName,
		elastic.NewPercentilesAggregation().Field(field).Percentiles(percents...))
	res, err := q.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("percentile aggregation failed: %s", err)
	}
	if agg, found := res.Aggregations.Percentiles(aggName); found {
		for key, val := range agg.Values {
			keyf, err := strconv.ParseFloat(key, 64)
			if err != nil {
				return nil, fmt.Errorf("percentile key conversion failed: %s", err)
			}
			key := protoutil.WrapValue(keyf)
			out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: key, Value: float64(val)})
		}
	}

	return out, nil
}
*/

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (es *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")

	o := make(chan string, es.pageSize)
	go func() {
		defer close(o)
		if label == "" {
			return
		}
		scroll := es.client.Scroll().
			Index(es.vertexIndex).
			Query(elastic.NewBoolQuery().Must(elastic.NewTermQuery("label", label))).
			Sort("gid", true).
			Size(es.pageSize)
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return // all results retrieved
			}
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Debug("VertexLabelScan: scroll failed")
				return
			}
			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				o <- hit.Id
			}
		}
	}()

	return o
}
