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
	"github.com/bmeg/arachne/util"
	"github.com/golang/protobuf/jsonpb"
	"golang.org/x/sync/errgroup"
	elastic "gopkg.in/olivere/elastic.v5"
)

var excludeData = elastic.NewFetchSourceContext(true).Exclude("data")

// Graph is a graph database backended by elastic search
type Graph struct {
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
	synchronous bool
}

// Compiler returns a query compiler that will use elastic search as a backend
func (es *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(es)
}

// GetTimestamp returns the change timestamp of the current graph
func (es *Graph) GetTimestamp() string {
	return es.ts.Get(es.graph)
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *Graph) AddEdge(edgeArray []*aql.Edge) error {
	for _, edge := range edgeArray {
		if edge.Gid == "" {
			edge.Gid = util.UUID()
		}
		err := edge.Validate()
		if err != nil {
			return fmt.Errorf("edge validation failed: %v", err)
		}
	}

	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	if es.synchronous {
		bulkRequest = bulkRequest.Refresh("true")
	}
	for _, e := range edgeArray {
		pe := PackEdge(e)
		script := elastic.NewScript(`ctx._source.gid = params.gid;
                                 ctx._source.label = params.label; 
                                 ctx._source.from = params.from; 
                                 ctx._source.to = params.to; 
                                 ctx._source.data = params.data;`).Params(pe)
		req := elastic.NewBulkUpdateRequest().
			Index(es.edgeIndex).
			Type("edge").
			Id(e.Gid).
			Script(script).
			Upsert(pe)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to add edge: %s", err)
	}
	es.ts.Touch(es.graph)
	return nil
}

// AddVertex adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *Graph) AddVertex(vertexArray []*aql.Vertex) error {
	for _, vertex := range vertexArray {
		err := vertex.Validate()
		if err != nil {
			return fmt.Errorf("vertex validation failed: %v", err)
		}
	}
	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	if es.synchronous {
		bulkRequest = bulkRequest.Refresh("true")
	}
	for _, v := range vertexArray {
		if v.Gid == "" {
			return fmt.Errorf("Vertex Gid cannot be an empty string")
		}
		pv := PackVertex(v)
		script := elastic.NewScript(`ctx._source.gid = params.gid;
                                 ctx._source.label = params.label; 
                                 ctx._source.data = params.data;`).Params(pv)
		req := elastic.NewBulkUpdateRequest().
			Index(es.vertexIndex).
			Type("vertex").
			Id(v.Gid).
			Script(script).
			Upsert(pv)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to add vertex: %s", err)
	}
	es.ts.Touch(es.graph)
	return nil
}

// DelEdge deletes edge `eid`
func (es *Graph) DelEdge(eid string) error {
	ctx := context.Background()
	op := es.client.Delete()
	if es.synchronous {
		op = op.Refresh("true")
	}
	_, err := op.Index(es.edgeIndex).Type("edge").Id(eid).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete edge %s: %s", eid, err)
	}
	es.ts.Touch(es.graph)
	return nil
}

// deleteConnectedEdges deletes edges where `from` or `to` equal `vid`
func (es *Graph) deleteConnectedEdges(vid string) error {
	ctx := context.Background()

	op := es.client.DeleteByQuery()
	if es.synchronous {
		op = op.Refresh("true")
	}
	op = op.Index(es.edgeIndex).Type("edge").Query(
		elastic.NewBoolQuery().Should(elastic.NewTermQuery("from", vid), elastic.NewTermQuery("to", vid)),
	)
	_, err := op.Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete edge(s): %s", err)
	}
	es.ts.Touch(es.graph)
	return nil
}

// DelVertex deletes vertex `vid` and all adjacent edges
func (es *Graph) DelVertex(vid string) error {
	ctx := context.Background()
	op := es.client.Delete()
	if es.synchronous {
		op = op.Refresh("true")
	}
	_, err := op.Index(es.vertexIndex).Type("vertex").Id(vid).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete vertex %s: %s", vid, err)
	}
	es.ts.Touch(es.graph)
	err = es.deleteConnectedEdges(vid)
	if err != nil {
		return err
	}
	return nil
}

// GetEdge gets a specific edge
func (es *Graph) GetEdge(id string, load bool) *aql.Edge {
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
		log.Printf("Failed to unmarshal edge: %s", err)
		return nil
	}

	return edge
}

// GetVertex gets vertex `id`
func (es *Graph) GetVertex(id string, load bool) *aql.Vertex {
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
func (es *Graph) GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage, 100)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(hits)
		scroll := es.client.Scroll(es.edgeIndex).Sort("gid", true).Size(100)
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
	})

	// 2nd goroutine receives hits and deserializes them.
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
func (es *Graph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage, 100)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(hits)
		scroll := es.client.Scroll(es.vertexIndex).Sort("gid", true).Size(100)
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
	})

	// 2nd goroutine receives hits and deserializes them.
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

// GetVertexChannel get a channel that returns all vertices in a graph
func (es *Graph) GetVertexChannel(req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
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
			q = q.Query(elastic.NewBoolQuery().Must(elastic.NewIdsQuery().Ids(idBatch...)))
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

// GetOutChannel gets channel of all vertices connected to element via outgoing edge
func (es *Graph) GetOutChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
			q = q.Query(elastic.NewBoolQuery().Must(qParts...))
			q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Include("from", "to"))
			res, err := q.Sort("gid", true).Do(ctx)
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
			q = q.Query(elastic.NewBoolQuery().Must(elastic.NewIdsQuery().Ids(idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Sort("gid", true).Do(ctx)
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

// GetInChannel gets all vertices connected to lookup elements by incoming edges
func (es *Graph) GetInChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
			q = q.Query(elastic.NewBoolQuery().Must(qParts...))
			q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Include("from", "to"))
			res, err := q.Sort("gid", true).Do(ctx)
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
			q = q.Query(elastic.NewBoolQuery().Must(elastic.NewIdsQuery().Ids(idBatch...)))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Sort("gid", true).Do(ctx)
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

// GetOutEdgeChannel gets all outgoing edges connected to lookup element
func (es *Graph) GetOutEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
			q = q.Query(elastic.NewBoolQuery().Must(qParts...))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Sort("gid", true).Do(ctx)
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

// GetInEdgeChannel gets incoming edges connected to lookup element
func (es *Graph) GetInEdgeChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
			q = q.Query(elastic.NewBoolQuery().Must(qParts...))
			if !load {
				q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
			}
			res, err := q.Sort("gid", true).Do(ctx)
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
