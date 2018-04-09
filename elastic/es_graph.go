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
	structpb "github.com/golang/protobuf/ptypes/struct"
	"golang.org/x/sync/errgroup"
	"gopkg.in/mgo.v2/bson"
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
	// Used to batch queries from incoming channels
	batchSize int
	// Not recommended for production. Refresh the relevant primary and replica shards (not the
	// whole index) immediately after the operation occurs, so that the updated
	// document appears in search results immediately.
	syncronous bool
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
	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	if es.syncronous {
		bulkRequest = bulkRequest.Refresh("true")
	}
	for _, e := range edgeArray {
		if e.Gid == "" {
			e.Gid = bson.NewObjectId().Hex()
		}
		req := elastic.NewBulkUpdateRequest().
			Index(es.edgeIndex).
			Type("edge").
			Id(e.Gid).
			Doc(PackEdge(e)).
			DocAsUpsert(true)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		return err
	}
	es.ts.Touch(es.graph)
	return nil
}

// AddVertex adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *ElasticGraph) AddVertex(vertexArray []*aql.Vertex) error {
	log.Printf("ElasticGraph.AddVertex called")
	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	if es.syncronous {
		bulkRequest = bulkRequest.Refresh("true")
	}
	for _, v := range vertexArray {
		if v.Gid == "" {
			return fmt.Errorf("Vertex Gid cannot be an empty string")
		}
		req := elastic.NewBulkUpdateRequest().
			Index(es.vertexIndex).
			Type("vertex").
			Id(v.Gid).
			Doc(PackVertex(v)).
			DocAsUpsert(true)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		return err
	}
	es.ts.Touch(es.graph)
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
	ctx := context.Background()
	_, err := es.client.Delete().Index(es.edgeIndex).Id(eid).Do(ctx)
	if err != nil {
		return err
	}
	es.ts.Touch(es.graph)
	return nil
}

// DelVertex
func (es *ElasticGraph) DelVertex(vid string) error {
	log.Printf("ElasticGraph.DelVertex called")
	ctx := context.Background()
	// TODO: remove connected edges
	_, err := es.client.Delete().Index(es.vertexIndex).Id(vid).Do(ctx)
	if err != nil {
		return err
	}
	es.ts.Touch(es.graph)
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

// GetEdgeList produces a channel of all edges in the graph
func (es *ElasticGraph) GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge {
	log.Printf("ElasticGraph.GetEdgeList called")
	o := make(chan *aql.Edge, 100)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage, 100)
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
	hits := make(chan json.RawMessage, 100)
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

	// Find all vertices
	o := make(chan gdbi.ElementLookup, 100)
	g.Go(func() error {
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			q := es.client.Search().Index(es.vertexIndex)
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewIdsQuery().Ids(idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Vertex query failed: %s", err)
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
			qParts := []elastic.Query{elastic.NewTermsQuery("from", idBatch...)}
			if len(edgeLabels) > 0 {
				labels := make([]interface{}, len(edgeLabels))
				for i, v := range edgeLabels {
					labels[i] = v
				}
				qParts = append(qParts, elastic.NewTermsQuery("label", labels...))
			}
			q = q.Query(elastic.NewBoolQuery().Filter(qParts...))
			q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Include("from", "to"))
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Edge query failed: %s", err)
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
				return fmt.Errorf("Vertex query failed: %s", err)
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
			qParts := []elastic.Query{elastic.NewTermsQuery("to", idBatch...)}
			if len(edgeLabels) > 0 {
				labels := make([]interface{}, len(edgeLabels))
				for i, v := range edgeLabels {
					labels[i] = v
				}
				qParts = append(qParts, elastic.NewTermsQuery("label", labels...))
			}
			q = q.Query(elastic.NewBoolQuery().Filter(qParts...))
			q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Include("from", "to"))
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Edge query failed: %s", err)
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
				return fmt.Errorf("Vertex query failed: %s", err)
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
			qParts := []elastic.Query{elastic.NewTermsQuery("from", idBatch...)}
			if len(edgeLabels) > 0 {
				labels := make([]interface{}, len(edgeLabels))
				for i, v := range edgeLabels {
					labels[i] = v
				}
				qParts = append(qParts, elastic.NewTermsQuery("label", labels...))
			}
			q = q.Query(elastic.NewBoolQuery().Filter(qParts...))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Edge query failed: %s", err)
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
			qParts := []elastic.Query{elastic.NewTermsQuery("to", idBatch...)}
			if len(edgeLabels) > 0 {
				labels := make([]interface{}, len(edgeLabels))
				for i, v := range edgeLabels {
					labels[i] = v
				}
				qParts = append(qParts, elastic.NewTermsQuery("label", labels...))
			}
			q = q.Query(elastic.NewBoolQuery().Filter(qParts...))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Do(ctx)
			if err != nil {
				return fmt.Errorf("Edge query failed: %s", err)
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

// GetVertexIndexList
func (es *ElasticGraph) GetVertexIndexList() chan aql.IndexID {
	log.Printf("ElasticGraph.GetVertexIndexList called")
	return nil
}

// GetVertexTermCount returns the count of every term across vertices
func (es *ElasticGraph) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	log.Printf("ElasticGraph.GetVertexTermCount called")

	o := make(chan aql.IndexTermCount, 100)
	go func() {
		defer close(o)
		if field == "" {
			return
		}
		q := es.client.Count().Index(es.vertexIndex)
		if label != "" {
			q = q.Query(elastic.NewBoolQuery().Filter(elastic.NewTermQuery("label", label)))
		}
		q = q.Df("data." + field)
		res, err := q.Do(ctx)
		if err != nil {
			log.Printf("Vertex term count failed: %s", err)
			return
		}

		term := structpb.Value{Kind: &structpb.Value_StringValue{StringValue: field}}
		idxit := aql.IndexTermCount{Term: &term, Count: int32(res)}
		o <- idxit
	}()

	return o
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (es *ElasticGraph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.Printf("ElasticGraph.VertexLabelScan called")

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
		return
	}()

	return o
}
