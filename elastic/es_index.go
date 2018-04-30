package elastic

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	elastic "gopkg.in/olivere/elastic.v5"
)

// AddVertexIndex adds a new field to be indexed
func (es *Graph) AddVertexIndex(label string, field string) error {
	log.Printf("Adding index: %s.%s", label, field)
	return nil
}

// DeleteVertexIndex removes a vertex field index
func (es *Graph) DeleteVertexIndex(label string, field string) error {
	log.Printf("Deleting index: %s.%s", label, field)
	return nil
}

// GetVertexIndexList gets list if vertex indices
func (es *Graph) GetVertexIndexList() chan aql.IndexID {
	ctx := context.Background()

	o := make(chan aql.IndexID, 100)
	go func() {
		defer close(o)

		// get all unique labels
		q := es.client.Search().Index(es.vertexIndex).Type("vertex")
		aggName := "labels.aggregation"
		q = q.Aggregation(aggName, elastic.NewTermsAggregation().Field("label").Size(1000000).OrderByCountDesc())
		res, err := q.Do(ctx)
		if err != nil {
			log.Printf("GetVertexIndexList - label term count failed: %s", err)
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
			log.Printf("GetFieldMapping call failed: %s", err)
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
				o <- aql.IndexID{Graph: es.graph, Label: l, Field: k}
			}
		}
	}()

	return o
}

// GetVertexTermAggregation returns the count of every term across vertices
func (es *Graph) GetVertexTermAggregation(ctx context.Context, name string, label string, field string, size uint32) (*aql.NamedAggregationResult, error) {
	log.Printf("Running GetVertexTermCount: { label: %s, field: %s }", label, field)

	out := &aql.NamedAggregationResult{
		Name:    name,
		Buckets: []*aql.AggregationResult{},
	}

	q := es.client.Search().Index(es.vertexIndex).Type("vertex")
	q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label)))
	aggName := fmt.Sprintf("term.aggregation.%s.%s", label, field)
	q = q.Aggregation(aggName,
		elastic.NewTermsAggregation().Field("data."+field+".keyword").Size(int(size)).OrderByCountDesc())
	res, err := q.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("term count failed: %s", err)
	}
	if agg, found := res.Aggregations.Terms(aggName); found {
		for _, bucket := range agg.Buckets {
			term := protoutil.WrapValue(bucket.Key.(string))
			out.Buckets = append(out.Buckets, &aql.AggregationResult{Key: term, Value: float32(bucket.DocCount)})
		}
	}

	return out, nil
}

//GetVertexHistogramAggregation get binned counts of a term across vertices
func (es *Graph) GetVertexHistogramAggregation(ctx context.Context, name string, label string, field string, interval uint32) (*aql.NamedAggregationResult, error) {
	return nil, fmt.Errorf("not implemented")
}

//GetVertexPercentileAggregation get percentiles of a term across vertices
func (es *Graph) GetVertexPercentileAggregation(ctx context.Context, name string, label string, field string, percents []uint32) (*aql.NamedAggregationResult, error) {
	return nil, fmt.Errorf("not implemented")
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (es *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.Printf("Running VertexLabelScan for label: %s", label)

	o := make(chan string, 100)
	go func() {
		defer close(o)
		if label == "" {
			return
		}
		scroll := es.client.Scroll().
			Index(es.vertexIndex).
			Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label))).
			Size(100)
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return // all results retrieved
			}
			if err != nil {
				log.Printf("Scroll call failed: %v", err)
				return
			}
			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				select {
				case <-ctx.Done():
					return
				default:
					o <- hit.Id
				}
			}
		}
	}()

	return o
}
