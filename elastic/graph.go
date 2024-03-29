package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
	"github.com/bmeg/grip/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
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
	pageSize    int
}

// Compiler returns a query compiler that will use elastic search as a backend
func (es *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(es, core.IndexStartOptimize) //TODO: probably a better optimizer for vertex label search
}

// GetTimestamp returns the change timestamp of the current graph
func (es *Graph) GetTimestamp() string {
	return es.ts.Get(es.graph)
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (es *Graph) AddEdge(edges []*gdbi.Edge) error {
	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	if es.synchronous {
		bulkRequest = bulkRequest.Refresh("true")
	}
	for _, e := range edges {
		if e.ID == "" {
			return fmt.Errorf("edge gid cannot be an empty string")
		}
		pe := PackEdge(e)
		script := elastic.NewScript(`ctx._source.gid = params.gid;
                                 ctx._source.label = params.label;
                                 ctx._source.from = params.from;
                                 ctx._source.to = params.to;
                                 ctx._source.data = params.data;`).Params(pe)
		req := elastic.NewBulkUpdateRequest().
			Index(es.edgeIndex).
			Type("edge").
			Id(e.ID).
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
func (es *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	ctx := context.Background()

	bulkRequest := es.client.Bulk()
	if es.synchronous {
		bulkRequest = bulkRequest.Refresh("true")
	}
	for _, v := range vertices {
		if v.ID == "" {
			return fmt.Errorf("vertex gid cannot be an empty string")
		}
		pv := PackVertex(v)
		script := elastic.NewScript(`ctx._source.gid = params.gid;
                                 ctx._source.label = params.label;
                                 ctx._source.data = params.data;`).Params(pv)
		req := elastic.NewBulkUpdateRequest().
			Index(es.vertexIndex).
			Type("vertex").
			Id(v.ID).
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

func (es *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	return util.StreamBatch(stream, 50, es.graph, es.AddVertex, es.AddEdge)
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
func (es *Graph) GetEdge(id string, load bool) *gdbi.Edge {
	ctx := context.Background()

	g := es.client.Get().Index(es.edgeIndex).Id(id)
	if !load {
		g = g.FetchSource(true).FetchSourceContext(excludeData)
	}

	res, err := g.Do(ctx)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetEdge")
		return nil
	}

	edge := &gripql.Edge{}
	err = protojson.Unmarshal(*res.Source, edge)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetEdge: unmarshal")
		return nil
	}
	o := gdbi.NewElementFromEdge(edge)
	o.Loaded = load
	return o
}

// GetVertex gets vertex `id`
func (es *Graph) GetVertex(id string, load bool) *gdbi.Vertex {
	ctx := context.Background()

	g := es.client.Get().Index(es.vertexIndex).Id(id)
	if !load {
		g = g.FetchSource(true).FetchSourceContext(excludeData)
	}

	res, err := g.Do(ctx)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetVertex")
		return nil
	}

	vertex := &gripql.Vertex{}
	err = protojson.Unmarshal(*res.Source, vertex)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetVertex: unmarshal")
		return nil
	}

	o := gdbi.NewElementFromVertex(vertex)
	o.Loaded = load
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (es *Graph) GetEdgeList(ctx context.Context, load bool) <-chan *gdbi.Edge {
	o := make(chan *gdbi.Edge, 100)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage, es.pageSize)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(hits)
		scroll := es.client.Scroll(es.edgeIndex).Sort("gid", true).Size(es.pageSize)
		if !load {
			scroll = scroll.FetchSource(true).FetchSourceContext(excludeData)
		}
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return fmt.Errorf("scroll call failed: %v", err)
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				hits <- *hit.Source
			}
		}
	})

	// 2nd goroutine receives hits and deserializes them.
	g.Go(func() error {
		for hit := range hits {
			edge := &gripql.Edge{}
			err := protojson.Unmarshal(hit, edge)
			if err != nil {
				return err
			}
			i := gdbi.NewElementFromEdge(edge)
			i.Loaded = load
			o <- i
		}
		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetEdgeList")
		}
	}()

	return o
}

// GetVertexList produces a channel of all vertices in the graph
func (es *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	o := make(chan *gdbi.Vertex, es.pageSize)

	// 1st goroutine sends individual hits to channel.
	hits := make(chan json.RawMessage, es.pageSize)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(hits)
		scroll := es.client.Scroll(es.vertexIndex).Sort("gid", true).Size(es.pageSize)
		if !load {
			scroll = scroll.FetchSource(true).FetchSourceContext(excludeData)
		}
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return fmt.Errorf("scroll call failed: %v", err)
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				hits <- *hit.Source
			}
		}
	})

	// 2nd goroutine receives hits and deserializes them.
	g.Go(func() error {
		for hit := range hits {
			vertex := &gripql.Vertex{}
			err := protojson.Unmarshal(hit, vertex)
			if err != nil {
				return fmt.Errorf("failed to unmarshal vertex: %v", err)
			}
			i := gdbi.NewElementFromVertex(vertex)
			i.Loaded = load
			o <- i
		}
		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexList")
		}
	}()

	return o
}

// GetVertexChannel get a channel that returns all vertices in a graph
func (es *Graph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := gdbi.LookupBatcher(req, es.pageSize, time.Microsecond)

	// Find all vertices
	o := make(chan gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		for batch := range batches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
				}
			}
			if len(idBatch) > 0 {
				q := es.client.Search().Index(es.vertexIndex).Size(es.pageSize)
				q = q.Query(elastic.NewBoolQuery().Must(elastic.NewIdsQuery().Ids(idBatch...)))
				if !load {
					q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
				}

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					vertex := &gripql.Vertex{}
					err := protojson.Unmarshal(*hit.Source, vertex)
					if err != nil {
						return fmt.Errorf("failed to unmarshal vertex: %s", err)
					}
					r := batchMap[vertex.Gid]
					for _, ri := range r {
						ri.Vertex = gdbi.NewElementFromVertex(vertex)
						ri.Vertex.Loaded = load
						o <- ri
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}

		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel")
		}
	}()

	return o
}

// GetOutChannel gets channel of all vertices connected to element via outgoing edge
func (es *Graph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := gdbi.LookupBatcher(req, es.pageSize, time.Microsecond)

	// Find all outgoing edges
	edgeBatches := make(chan []gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		defer close(edgeBatches)
		for batch := range batches {
			idBatch := make([]interface{}, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int)
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
			}
			b := []gdbi.ElementLookup{}
			if len(idBatch) > 0 {
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
				q = q.Sort("gid", true).Size(es.pageSize)

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					edge := &gripql.Edge{}
					err := protojson.Unmarshal(*hit.Source, edge)
					if err != nil {
						return fmt.Errorf("failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.From]
					batchMapReturnCount[edge.From]++
					for _, ri := range r {
						ri.Vertex = &gdbi.Vertex{ID: edge.To}
						b = append(b, ri)
					}
				}
			}
			if emitNull {
				for id, count := range batchMapReturnCount {
					if count == 0 {
						r := batchMap[id]
						for _, ri := range r {
							ri.Vertex = nil
							b = append(b, ri)
						}
					}
				}
			}
			for i := range signals {
				b = append(b, signals[i])
			}
			edgeBatches <- b
		}

		return nil
	})

	// Collect all identified vertices
	o := make(chan gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		for batch := range edgeBatches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					if batch[i].Vertex != nil {
						idBatch = append(idBatch, batch[i].Vertex.ID)
						batchMap[batch[i].Vertex.ID] = append(batchMap[batch[i].Vertex.ID], batch[i])
					} else if emitNull {
						o <- batch[i]
					}
				}
			}
			if len(idBatch) > 0 {
				q := es.client.Search().Index(es.vertexIndex)
				q = q.Query(elastic.NewBoolQuery().Must(elastic.NewIdsQuery().Ids(idBatch...)))
				if !load {
					q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
				}
				q = q.Sort("gid", true).Size(es.pageSize)

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					vertex := &gripql.Vertex{}
					err := protojson.Unmarshal(*hit.Source, vertex)
					if err != nil {
						return fmt.Errorf("failed to unmarshal vertex: %s", err)
					}
					r := batchMap[vertex.Gid]
					for _, ri := range r {
						ri.Vertex = gdbi.NewElementFromVertex(vertex)
						ri.Vertex.Loaded = load
						o <- ri
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}

		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetOutChannel")
		}
	}()

	return o
}

// GetInChannel gets all vertices connected to lookup elements by incoming edges
func (es *Graph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := gdbi.LookupBatcher(req, es.pageSize, time.Microsecond)

	// Find all incoming edges
	edgeBatches := make(chan []gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		defer close(edgeBatches)
		for batch := range batches {
			idBatch := make([]interface{}, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int)
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
			}
			b := []gdbi.ElementLookup{}
			if len(idBatch) > 0 {
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
				q = q.Sort("gid", true).Size(es.pageSize)

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					edge := &gripql.Edge{}
					err := protojson.Unmarshal(*hit.Source, edge)
					if err != nil {
						return fmt.Errorf("failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.To]
					batchMapReturnCount[edge.To]++
					for _, ri := range r {
						ri.Vertex = &gdbi.Vertex{ID: edge.From}
						b = append(b, ri)
					}
				}
			}
			if emitNull {
				for id, count := range batchMapReturnCount {
					if count == 0 {
						r := batchMap[id]
						for _, ri := range r {
							ri.Vertex = nil
							b = append(b, ri)
						}
					}
				}
			}
			for i := range signals {
				b = append(b, signals[i])
			}
			edgeBatches <- b
		}

		return nil
	})

	// Collect all identified vertices
	o := make(chan gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		for batch := range edgeBatches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					if batch[i].Vertex != nil {
						idBatch = append(idBatch, batch[i].Vertex.ID)
						batchMap[batch[i].Vertex.ID] = append(batchMap[batch[i].Vertex.ID], batch[i])
					} else if emitNull {
						o <- batch[i]
					}
				}
			}
			if len(idBatch) > 0 {
				q := es.client.Search().Index(es.vertexIndex)
				q = q.Query(elastic.NewBoolQuery().Must(elastic.NewIdsQuery().Ids(idBatch...)))
				if !load {
					q = q.FetchSource(true).FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude("data"))
				}
				q = q.Sort("gid", true).Size(es.pageSize)

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					vertex := &gripql.Vertex{}
					err := protojson.Unmarshal(*hit.Source, vertex)
					if err != nil {
						return fmt.Errorf("failed to unmarshal vertex: %s", err)
					}
					r := batchMap[vertex.Gid]
					for _, ri := range r {
						ri.Vertex = gdbi.NewElementFromVertex(vertex)
						ri.Vertex.Loaded = load
						o <- ri
					}
				}
				for i := range signals {
					o <- signals[i]
				}
			}
		}

		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetInChannel")
		}
	}()

	return o
}

// GetOutEdgeChannel gets all outgoing edges connected to lookup element
func (es *Graph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := gdbi.LookupBatcher(req, es.pageSize, time.Microsecond)

	// Find all outgoing edges
	o := make(chan gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		for batch := range batches {
			idBatch := make([]interface{}, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int)
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
			}
			if len(idBatch) > 0 {
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
				q = q.Sort("gid", true).Size(es.pageSize)

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					edge := &gripql.Edge{}
					err := protojson.Unmarshal(*hit.Source, edge)
					if err != nil {
						return fmt.Errorf("failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.From]
					batchMapReturnCount[edge.From]++
					for _, ri := range r {
						ri.Edge = gdbi.NewElementFromEdge(edge)
						ri.Edge.Loaded = load
						o <- ri
					}
				}
			}
			if emitNull {
				for id, count := range batchMapReturnCount {
					if count == 0 {
						r := batchMap[id]
						for _, ri := range r {
							ri.Edge = nil
							o <- ri
						}
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}

		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel")
		}
	}()

	return o
}

// GetInEdgeChannel gets incoming edges connected to lookup element
func (es *Graph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	g, ctx := errgroup.WithContext(ctx)

	// Create query batches
	batches := gdbi.LookupBatcher(req, es.pageSize, time.Microsecond)

	// Find all incoming edges
	o := make(chan gdbi.ElementLookup, es.pageSize)
	g.Go(func() error {
		for batch := range batches {
			idBatch := make([]interface{}, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int)
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
			}

			if len(idBatch) > 0 {
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
				q = q.Sort("gid", true).Size(es.pageSize)

				for hit := range paginateQuery(ctx, q, es.pageSize) {
					// Deserialize
					edge := &gripql.Edge{}
					err := protojson.Unmarshal(*hit.Source, edge)
					if err != nil {
						return fmt.Errorf("failed to unmarshal edge: %s", err)
					}
					r := batchMap[edge.To]
					batchMapReturnCount[edge.To]++
					for _, ri := range r {
						ri.Edge = gdbi.NewElementFromEdge(edge)
						ri.Edge.Loaded = load
						o <- ri
					}
				}
			}
			if emitNull {
				for id, count := range batchMapReturnCount {
					if count == 0 {
						r := batchMap[id]
						for _, ri := range r {
							ri.Edge = nil
							o <- ri
						}
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}

		return nil
	})

	// Check whether any goroutines failed.
	go func() {
		defer close(o)
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel")
		}
	}()

	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (es *Graph) ListVertexLabels() ([]string, error) {
	q := es.client.Search().Index(es.vertexIndex).Type("vertex")
	aggName := "vertex.labels.aggregation"
	q = q.Aggregation(aggName, elastic.NewTermsAggregation().Field("label").Size(1000000))
	res, err := q.Do(context.Background())
	if err != nil {
		return nil, err
	}
	labels := []string{}
	if agg, found := res.Aggregations.Terms(aggName); found {
		for _, bucket := range agg.Buckets {
			labels = append(labels, bucket.Key.(string))
		}
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (es *Graph) ListEdgeLabels() ([]string, error) {
	q := es.client.Search().Index(es.edgeIndex).Type("edge")
	aggName := "edge.labels.aggregation"
	q = q.Aggregation(aggName, elastic.NewTermsAggregation().Field("label").Size(1000000))
	res, err := q.Do(context.Background())
	if err != nil {
		return nil, err
	}
	labels := []string{}
	if agg, found := res.Aggregations.Terms(aggName); found {
		for _, bucket := range agg.Buckets {
			labels = append(labels, bucket.Key.(string))
		}
	}
	return labels, nil
}
