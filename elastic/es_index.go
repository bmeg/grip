package elastic

import (
	"context"
	"io"
	"log"

	"github.com/bmeg/arachne/aql"
	structpb "github.com/golang/protobuf/ptypes/struct"
	elastic "gopkg.in/olivere/elastic.v5"
)

// AddVertexIndex adds a new field to be indexed
func (es *Graph) AddVertexIndex(label string, field string) error {
	log.Printf("Graph.AddVertexIndex called")
	return nil
}

// DeleteVertexIndex removes a vertex field index
func (es *Graph) DeleteVertexIndex(label string, field string) error {
	log.Printf("Graph.DeleteVertexIndex called")
	return nil
}

// GetVertexIndexList gets list if vertex indices
func (es *Graph) GetVertexIndexList() chan aql.IndexID {
	log.Printf("Running GetVertexIndexList")
	ctx := context.Background()

	o := make(chan aql.IndexID, 100)
	go func() {
		defer close(o)
		q := es.client.GetFieldMapping().Index(es.vertexIndex).Type("vertex")
		res, err := q.Do(ctx)
		if err != nil {
			log.Printf("GetFieldMapping call failed: %s", err)
			return
		}
		log.Printf("GetFieldMapping res: %+v", res)
	}()

	return o
}

// GetVertexTermCount returns the count of every term across vertices
func (es *Graph) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	log.Printf("Running GetVertexTermCount: { label: %s, field: %s }", label, field)

	o := make(chan aql.IndexTermCount, 100)
	go func() {
		defer close(o)
		if field == "" {
			return
		}
		q := es.client.Search().Index(es.vertexIndex).Type("vertex")
		if label != "" {
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label)))
		}
		aggName := "term.aggregation." + field
		// TODO make size an argument
		q = q.Aggregation(aggName,
			elastic.NewTermsAggregation().Field("data."+field+".keyword").Size(1000).OrderByCountDesc())
		res, err := q.Do(ctx)
		if err != nil {
			log.Printf("Vertex term count failed: %s", err)
			return
		}
		if agg, found := res.Aggregations.Terms(aggName); found {
			for _, bucket := range agg.Buckets {
				term := structpb.Value{Kind: &structpb.Value_StringValue{StringValue: bucket.Key.(string)}}
				idxit := aql.IndexTermCount{Term: &term, Count: int32(bucket.DocCount)}
				o <- idxit
			}
		}
	}()

	return o
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
