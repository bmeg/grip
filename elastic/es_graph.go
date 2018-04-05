package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/golang/protobuf/jsonpb"
	"golang.org/x/sync/errgroup"
	elastic "gopkg.in/olivere/elastic.v5"
)

var excludeData = elastic.NewFetchSourceContext(true).Exclude("data")

type ElasticGraph struct {
	url         string
	database    string
	ts          *timestamp.Timestamp
	client      *elastic.Client
	graph       string
	vertexIndex string
	edgeIndex   string
	batchSize   int
}

// Compiler
func (es *ElasticGraph) Compiler() gdbi.Compiler {
	log.Printf("ElasticGraph.Compiler called")
	return core.NewCompiler(es)
}

// GetTimestamp
func (es *ElasticGraph) GetTimestamp() string {
	log.Printf("ElasticGraph.GetTimestamp called")
	return es.ts.Get(es.graph)
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddEdge(edgeArray []*aql.Edge) error {
	log.Printf("ElasticGraph.AddEdge called")
	bulkRequest := es.client.Bulk()
	for _, e := range edgeArray {
		req := elastic.NewBulkIndexRequest().Index(es.edgeIndex).Type(e.Label).Id(e.Gid).Doc(e)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(context.Background())
	return err
}

// AddVertex adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddVertex(vertexArray []*aql.Vertex) error {
	log.Printf("ElasticGraph.AddVertex called")
	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	for _, e := range vertexArray {
		req := elastic.NewBulkIndexRequest().Index(es.vertexIndex).Type(e.Label).Id(e.Gid).Doc(e)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	return err
}

// AddBundle
func (es *ElasticGraph) AddBundle(bundle *aql.Bundle) error {
	log.Printf("ElasticGraph.AddBundle called")
	return nil
}

// AddVertexIndex
func (es *ElasticGraph) AddVertexIndex(label string, field string) error {
	log.Printf("ElasticGraph.AddVertexIndex called")
	return nil
}

// DelEdge
func (es *ElasticGraph) DelEdge(eid string) error {
	log.Printf("ElasticGraph.DelEdge called")
	_, err := es.client.Delete().Index(es.edgeIndex).Id(eid).Do(context.Background())
	return err
}

// DelVertex
func (es *ElasticGraph) DelVertex(vid string) error {
	log.Printf("ElasticGraph.DelVertex called")
	ctx := context.Background()
	// TODO: remove connected edges
	_, err := es.client.Delete().Index(es.vertexIndex).Id(vid).Do(ctx)
	return err
}

// DelBundle
func (es *ElasticGraph) DelBundle(eid string) error {
	log.Printf("ElasticGraph.DelBundle called")
	return nil
}

// DeleteVertexIndex
func (es *ElasticGraph) DeleteVertexIndex(label string, field string) error {
	log.Printf("ElasticGraph.DeleteVertexIndex called")
	return nil
}

// GetEdge
func (es *ElasticGraph) GetEdge(id string, load bool) *aql.Edge {
	log.Printf("ElasticGraph.GetEdge called")
	ctx := context.Background()

	g := es.client.Get().Index(es.edgeIndex).Id(id)
	if !load {
		g = g.FetchSource(true).FetchSourceContext(excludeData)
	}

	res, err := g.Do(ctx)
	if err != nil {
		log.Printf("Failed to get edge: %s", err)
		return nil
	}

	edge := &aql.Edge{}
	err = jsonpb.Unmarshal(bytes.NewReader(*res.Source), edge)
	if err != nil {
		log.Printf("Failed to get unmarshal edge: %s", err)
		return nil
	}

	return edge
}

// GetVertex
func (es *ElasticGraph) GetVertex(id string, load bool) *aql.Vertex {
	log.Printf("ElasticGraph.GetVertex called")
	ctx := context.Background()

	g := es.client.Get().Index(es.vertexIndex).Id(id)
	if !load {
		g = g.FetchSource(true).FetchSourceContext(excludeData)
	}

	res, err := g.Do(ctx)
	if err != nil {
		log.Printf("Failed to get vertex: %s", err)
		return nil
	}

	vertex := &aql.Vertex{}
	err = jsonpb.Unmarshal(bytes.NewReader(*res.Source), vertex)
	if err != nil {
		log.Printf("Failed to get unmarshal vertex: %s", err)
		return nil
	}

	return vertex
}

// GetBundle
func (es *ElasticGraph) GetBundle(id string, load bool) *aql.Bundle {
	log.Printf("ElasticGraph.GetBundle called")
	return nil
}

// GetEdgeList produces a channel of all edges in the graph
func (es *ElasticGraph) GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge {
	log.Printf("ElasticGraph.GetEdgeList called")
	o := make(chan *aql.Edge, 100)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(hits)
		scroll := es.client.Scroll(es.edgeIndex).Size(100)
		if !load {
			scroll = scroll.FetchSource(true).FetchSourceContext(excludeData)
		}
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return fmt.Errorf("Scroll call failed: %v", err)
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					hits <- *hit.Source
				}
			}
		}
		return nil
	})

	// 2nd goroutine receives hits and deserializes them.
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for hit := range hits {
				select {
				default:
					// Deserialize
					edge := &aql.Edge{}
					err := jsonpb.Unmarshal(bytes.NewReader(hit), edge)
					if err != nil {
						return err
					}
					o <- edge

				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		})
	}

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.Printf("Failed to get edge list: %v", err)
		}
		return
	}()

	return o
}

// GetVertexList produces a channel of all vertices in the graph
func (es *ElasticGraph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	log.Printf("ElasticGraph.GetVertexList called")
	o := make(chan *aql.Vertex, 100)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(hits)
		scroll := es.client.Scroll(es.vertexIndex).Size(100)
		if !load {
			scroll = scroll.FetchSource(true).FetchSourceContext(excludeData)
		}
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return fmt.Errorf("Scroll call failed: %v", err)
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					hits <- *hit.Source
				}
			}
		}
		return nil
	})

	// 2nd goroutine receives hits and deserializes them.
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for hit := range hits {
				select {
				default:
					// Deserialize
					vertex := &aql.Vertex{}
					err := jsonpb.Unmarshal(bytes.NewReader(hit), vertex)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal vertex: %v", err)
					}
					o <- vertex

				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		})
	}

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.Printf("Failed to get vertex list: %v", err)
		}
		return
	}()

	return o
}

// GetVertexChannel
func (es *ElasticGraph) GetVertexChannel(req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	log.Printf("ElasticGraph.GetVertexChannel called")
	return nil
}

// GetOutChannel
func (es *ElasticGraph) GetOutChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	log.Printf("ElasticGraph.GetOutChannel called")
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := make(chan []gdbi.ElementLookup, 100)
	g.Go(func() error {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, es.batchSize)
		for req := range req {
			o = append(o, req)
			if len(o) >= es.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, es.batchSize)
			}
		}
		batches <- o
		return nil
	})

	// Find all outgoing edges
	edgeBatches := make(chan []gdbi.ElementLookup, 100)
	g.Go(func() error {
		defer close(edgeBatches)
		for batch := range batches {
			idBatch := make([]interface{}, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			q := es.client.Search().Index(es.edgeIndex)
			if len(edgeLabels) > 0 {
				q = q.Type(edgeLabels...)
			}
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermsQuery("from", idBatch...)))
			q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Include("from", "to"))
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Failed edge query: %s", err)
			}
			if res.TotalHits() > 0 {
				b := []gdbi.ElementLookup{}
				for _, hit := range res.Hits.Hits {
					// Deserialize
					edge := &aql.Edge{}
					err := jsonpb.Unmarshal(bytes.NewReader(*hit.Source), edge)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.From]
					for _, ri := range r {
						ri.Vertex = &aql.Vertex{Gid: edge.To}
						b = append(b, ri)
					}
				}
				edgeBatches <- b
			}
		}
		return nil
	})

	// Collect all identified vertices
	o := make(chan gdbi.ElementLookup, 100)
	g.Go(func() error {
		for batch := range edgeBatches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].Vertex.Gid
				batchMap[batch[i].Vertex.Gid] = append(batchMap[batch[i].Vertex.Gid], batch[i])
			}

			q := es.client.Search().Index(es.vertexIndex)
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewIdsQuery().Ids(idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Failed vertex query: %s", err)
			}
			if res.TotalHits() > 0 {
				for _, hit := range res.Hits.Hits {
					// Deserialize
					vertex := &aql.Vertex{}
					err := jsonpb.Unmarshal(bytes.NewReader(*hit.Source), vertex)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal vertex: %s", err)
					}
					r := batchMap[vertex.Gid]
					for _, ri := range r {
						ri.Vertex = vertex
						o <- ri
					}
				}
			}
		}
		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.Printf("Error: %v", err)
		}
		return
	}()

	return o
}

// GetInChannel
func (es *ElasticGraph) GetInChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	log.Printf("ElasticGraph.GetInChannel called")
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := make(chan []gdbi.ElementLookup, 100)
	g.Go(func() error {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, es.batchSize)
		for req := range req {
			o = append(o, req)
			if len(o) >= es.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, es.batchSize)
			}
		}
		batches <- o
		return nil
	})

	// Find all incoming edges
	edgeBatches := make(chan []gdbi.ElementLookup, 100)
	g.Go(func() error {
		defer close(edgeBatches)
		for batch := range batches {
			idBatch := make([]interface{}, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			q := es.client.Search().Index(es.edgeIndex)
			if len(edgeLabels) > 0 {
				q = q.Type(edgeLabels...)
			}
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermsQuery("to", idBatch...)))
			q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Include("from", "to"))
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Failed edge query: %s", err)
			}
			if res.TotalHits() > 0 {
				b := []gdbi.ElementLookup{}
				for _, hit := range res.Hits.Hits {
					// Deserialize
					edge := &aql.Edge{}
					err := jsonpb.Unmarshal(bytes.NewReader(*hit.Source), edge)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.To]
					for _, ri := range r {
						ri.Vertex = &aql.Vertex{Gid: edge.From}
						b = append(b, ri)
					}
				}
				edgeBatches <- b
			}
		}
		return nil
	})

	// Collect all identified vertices
	o := make(chan gdbi.ElementLookup, 100)
	g.Go(func() error {
		for batch := range edgeBatches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].Vertex.Gid
				batchMap[batch[i].Vertex.Gid] = append(batchMap[batch[i].Vertex.Gid], batch[i])
			}
			q := es.client.Search().Index(es.vertexIndex)
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewIdsQuery().Ids(idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Failed vertex query: %s", err)
			}
			if res.TotalHits() > 0 {
				for _, hit := range res.Hits.Hits {
					// Deserialize
					vertex := &aql.Vertex{}
					err := jsonpb.Unmarshal(bytes.NewReader(*hit.Source), vertex)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal vertex: %s", err)
					}
					r := batchMap[vertex.Gid]
					for _, ri := range r {
						ri.Vertex = vertex
						o <- ri
					}
				}
			}
		}
		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.Printf("Error: %v", err)
		}
		return
	}()

	return o
}

// GetOutEdgeChannel
func (es *ElasticGraph) GetOutEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	log.Printf("ElasticGraph.GetOutEdgeChannel called")
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := make(chan []gdbi.ElementLookup, 100)
	g.Go(func() error {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, es.batchSize)
		for req := range req {
			o = append(o, req)
			if len(o) >= es.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, es.batchSize)
			}
		}
		batches <- o
		return nil
	})

	// Find all outgoing edges
	o := make(chan gdbi.ElementLookup, 100)
	g.Go(func() error {
		for batch := range batches {
			idBatch := make([]interface{}, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			q := es.client.Search().Index(es.edgeIndex)
			if len(edgeLabels) > 0 {
				q = q.Type(edgeLabels...)
			}
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermsQuery("from", idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Failed edge query: %s", err)
			}
			if res.TotalHits() > 0 {
				for _, hit := range res.Hits.Hits {
					// Deserialize
					edge := &aql.Edge{}
					err := jsonpb.Unmarshal(bytes.NewReader(*hit.Source), edge)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.From]
					for _, ri := range r {
						ri.Edge = edge
						o <- ri
					}
				}
			}
		}
		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.Printf("Error: %v", err)
		}
		return
	}()

	return o
}

// GetInEdgeChannel
func (es *ElasticGraph) GetInEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	log.Printf("ElasticGraph.GetInEdgeChannel called")
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := make(chan []gdbi.ElementLookup, 100)
	g.Go(func() error {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, es.batchSize)
		for req := range req {
			o = append(o, req)
			if len(o) >= es.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, es.batchSize)
			}
		}
		batches <- o
		return nil
	})

	// Find all incoming edges
	o := make(chan gdbi.ElementLookup, 100)
	g.Go(func() error {
		for batch := range batches {
			idBatch := make([]interface{}, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			q := es.client.Search().Index(es.edgeIndex)
			if len(edgeLabels) > 0 {
				q = q.Type(edgeLabels...)
			}
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermsQuery("to", idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Failed edge query: %s", err)
			}
			if res.TotalHits() > 0 {
				for _, hit := range res.Hits.Hits {
					// Deserialize
					edge := &aql.Edge{}
					err := jsonpb.Unmarshal(bytes.NewReader(*hit.Source), edge)
					if err != nil {
						return fmt.Errorf("Failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.To]
					for _, ri := range r {
						ri.Edge = edge
						o <- ri
					}
				}
			}
		}
		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.Printf("Error: %v", err)
		}
		return
	}()

	return o
}

// GetOutBundleChannel
func (es *ElasticGraph) GetOutBundleChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	log.Printf("ElasticGraph.GetOutBundleChannel called")
	return nil
}

// GetVertexIndexList
func (es *ElasticGraph) GetVertexIndexList() chan aql.IndexID {
	log.Printf("ElasticGraph.GetVertexIndexList called")
	return nil
}

// GetVertexTermCount
func (es *ElasticGraph) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	log.Printf("ElasticGraph.GetVertexTermCount called")
	return nil
}

// VertexLabelScan
func (es *ElasticGraph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.Printf("ElasticGraph.VertexLabelScan called")
	return nil
}
