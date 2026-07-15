package arango

import (
	"context"
	"fmt"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
	"github.com/bmeg/grip/util"
)

const (
	fieldID    = "_key"
	fieldLabel = "_label"
	fieldFrom  = "_from"
	fieldTo    = "_to"
)

type Graph struct {
	ar               *GraphDB
	ts               *timestamp.Timestamp
	graph            arangodb.Graph
	graphName        string
	vertexCollection arangodb.VertexCollection
	edgeCollection   arangodb.Edge
	batchSize        int
}

func (g *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(g, core.IndexStartOptimize)
}

func (g *Graph) GetTimestamp() string {
	if g.ts == nil {
		return ""
	}
	return g.ts.Get(g.graphName)
}

func (g *Graph) GetVertex(key string, load bool) *gdbi.Vertex {
	data := map[string]any{}
	if _, err := g.vertexCollection.ReadDocument(context.Background(), key, &data); err != nil {
		return nil
	}
	out := unpackVertex(data)
	if !load {
		out.Data = map[string]any{}
	}
	return out
}

func (g *Graph) GetEdge(key string, load bool) *gdbi.Edge {
	data := map[string]any{}
	if _, err := g.edgeCollection.ReadDocument(context.Background(), key, &data); err != nil {
		return nil
	}
	out := unpackEdge(data)
	if !load {
		out.Data = map[string]any{}
	}
	return out
}

func (g *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	if len(vertices) == 0 {
		return nil
	}
	docs := make([]map[string]any, 0, len(vertices))
	for _, vertex := range vertices {
		docs = append(docs, packVertex(vertex))
	}
	if err := g.ar.execQuery(
		"FOR doc IN @docs UPSERT { _key: doc._key } INSERT doc REPLACE doc IN @@v",
		map[string]any{"docs": docs, "@v": g.vertexCollection.Name()},
	); err != nil {
		return err
	}
	if g.ts != nil {
		g.ts.Touch(g.graphName)
	}
	return nil
}

func (g *Graph) AddEdge(edges []*gdbi.Edge) error {
	if len(edges) == 0 {
		return nil
	}
	docs := make([]map[string]any, 0, len(edges))
	for _, edge := range edges {
		docs = append(docs, packEdge(g.graphName, edge))
	}
	if err := g.ar.execQuery(
		"FOR doc IN @docs UPSERT { _key: doc._key } INSERT doc REPLACE doc IN @@e",
		map[string]any{"docs": docs, "@e": g.edgeCollection.Name()},
	); err != nil {
		return err
	}
	if g.ts != nil {
		g.ts.Touch(g.graphName)
	}
	return nil
}

func (g *Graph) StreamEdges(edgeChan <-chan *gdbi.Edge, batchSize int) error {
	if batchSize <= 0 {
		batchSize = g.batchSize
	}
	batch := make([]*gdbi.Edge, 0, batchSize)
	for edge := range edgeChan {
		batch = append(batch, edge)
		if len(batch) >= batchSize {
			if err := g.AddEdge(batch); err != nil {
				return err
			}
			batch = make([]*gdbi.Edge, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		return g.AddEdge(batch)
	}
	return nil
}

func (g *Graph) StreamVertices(vertChan <-chan *gdbi.Vertex, batchSize int) error {
	if batchSize <= 0 {
		batchSize = g.batchSize
	}
	batch := make([]*gdbi.Vertex, 0, batchSize)
	for vertex := range vertChan {
		batch = append(batch, vertex)
		if len(batch) >= batchSize {
			if err := g.AddVertex(batch); err != nil {
				return err
			}
			batch = make([]*gdbi.Vertex, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		return g.AddVertex(batch)
	}
	return nil
}

func (g *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	return util.StreamBatch(stream, g.batchSize, g.graphName, g.StreamVertices, g.StreamEdges)
}

func (g *Graph) BulkDel(data *gdbi.DeleteData) error {
	if data == nil {
		return nil
	}
	if data.Graph != "" && data.Graph != g.graphName {
		return fmt.Errorf("unexpected graph reference: %s != %s", data.Graph, g.graphName)
	}
	if len(data.Edges) > 0 {
		if err := g.ar.execQuery(
			"FOR key IN @keys REMOVE { _key: key } IN @@e OPTIONS { ignoreErrors: true }",
			map[string]any{"keys": data.Edges, "@e": g.edgeCollection.Name()},
		); err != nil {
			return err
		}
	}
	if len(data.Vertices) > 0 {
		if err := g.ar.execQuery(
			"FOR key IN @keys REMOVE { _key: key } IN @@v OPTIONS { ignoreErrors: true }",
			map[string]any{"keys": data.Vertices, "@v": g.vertexCollection.Name()},
		); err != nil {
			return err
		}
		if err := g.ar.execQuery(
			"LET handles = (FOR key IN @keys RETURN CONCAT(@vertexCollection, '/', key)) FOR e IN @@e FILTER e._from IN handles OR e._to IN handles REMOVE e IN @@e",
			map[string]any{"keys": data.Vertices, "@e": g.edgeCollection.Name(), "vertexCollection": g.vertexCollection.Name()},
		); err != nil {
			return err
		}
	}
	if g.ts != nil && (len(data.Edges) > 0 || len(data.Vertices) > 0) {
		g.ts.Touch(g.graphName)
	}

	return nil
}

func (g *Graph) DelVertex(key string) error {
	if err := g.BulkDel(&gdbi.DeleteData{Graph: g.graphName, Vertices: []string{key}}); err != nil {
		return fmt.Errorf("failed to delete vertex %s: %w", key, err)
	}
	return nil
}

func (g *Graph) DelEdge(key string) error {
	if err := g.BulkDel(&gdbi.DeleteData{Graph: g.graphName, Edges: []string{key}}); err != nil {
		return fmt.Errorf("failed to delete edge %s: %w", key, err)
	}
	return nil
}

func (g *Graph) AddVertexIndex(label string, field string) error {
	return fmt.Errorf("vertex indexes are not yet supported by the arango driver")
}

func (g *Graph) DeleteVertexIndex(label string, field string) error {
	return fmt.Errorf("vertex indexes are not yet supported by the arango driver")
}

func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	out := make(chan *gripql.IndexID)
	log.Warning("GetVertexIndexList: vertex indexes are not yet supported by the arango driver")
	close(out)
	return out
}

func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		res, err := g.ar.queryMaps(
			"FOR v IN @@v FILTER v._label == @label RETURN { id: v._key }",
			map[string]any{"@v": g.vertexCollection.Name(), "label": label},
		)
		if err != nil {
			return
		}
		for _, row := range res {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if id, ok := row["id"].(string); ok {
				out <- id
			}
		}
	}()
	return out
}

func (g *Graph) ListVertexLabels() ([]string, error) {
	res, err := g.ar.queryMaps(
		"FOR v IN @@v COLLECT l = v._label RETURN { label: l }",
		map[string]any{"@v": g.vertexCollection.Name()},
	)
	if err != nil {
		return nil, err
	}
	labels := make([]string, 0, len(res))
	for _, row := range res {
		if label, ok := row["label"].(string); ok && label != "" {
			labels = append(labels, label)
		}
	}
	return labels, nil
}

func (g *Graph) ListEdgeLabels() ([]string, error) {
	res, err := g.ar.queryMaps(
		"FOR e IN @@e COLLECT l = e._label RETURN { label: l }",
		map[string]any{"@e": g.edgeCollection.Name()},
	)
	if err != nil {
		return nil, err
	}
	labels := make([]string, 0, len(res))
	for _, row := range res {
		if label, ok := row["label"].(string); ok && label != "" {
			labels = append(labels, label)
		}
	}
	return labels, nil
}

func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	out := make(chan *gdbi.Vertex, 100)
	go func() {
		defer close(out)
		res, err := g.ar.queryMaps(
			"FOR v IN @@v RETURN v",
			map[string]any{"@v": g.vertexCollection.Name()},
		)
		if err != nil {
			return
		}
		for _, doc := range res {
			select {
			case <-ctx.Done():
				return
			default:
			}
			v := unpackVertex(doc)
			if !load {
				v.Data = map[string]any{}
			}
			out <- v
		}
	}()
	return out
}

func makeStringSet(list []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range list {
		out[item] = struct{}{}
	}
	return out
}

func (g *Graph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(req, g.batchSize, time.Microsecond)
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for batch := range batches {
			ids := make([]string, 0, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					ids = append(ids, batch[i].ID)
				}
			}
			rows, err := g.ar.queryMaps(
				"FOR v IN @@v FILTER v._key IN @ids RETURN v",
				map[string]any{"@v": g.vertexCollection.Name(), "ids": ids},
			)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel")
				continue
			}
			found := map[string]*gdbi.Vertex{}
			for _, row := range rows {
				v := unpackVertex(row)
				if !load {
					v.Data = map[string]any{}
				}
				found[v.ID] = v
			}
			for _, item := range batch {
				if v, ok := found[item.ID]; ok {
					item.Vertex = v
					out <- item
				}
			}
			for _, s := range signals {
				out <- s
			}
		}
	}()
	return out
}

func (g *Graph) getNeighborEdges(reqChan chan gdbi.ElementLookup, dir string, emitNull bool, edgeLabels []string, outEdges bool, load bool) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(reqChan, g.batchSize, time.Microsecond)
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for batch := range batches {
			ids := make([]string, 0, len(batch))
			reqMap := map[string][]gdbi.ElementLookup{}
			returnCount := map[string]int{}
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
					continue
				}
				handle := documentHandle(g.vertexCollection.Name(), batch[i].ID)
				ids = append(ids, handle)
				reqMap[handle] = append(reqMap[handle], batch[i])
				returnCount[handle] = 0
			}
			if len(ids) == 0 {
				for _, s := range signals {
					out <- s
				}
				continue
			}

			filterField := fieldFrom
			targetField := fieldTo
			if dir == "in" {
				filterField = fieldTo
				targetField = fieldFrom
			}

			bind := map[string]any{"@e": g.edgeCollection.Name(), "ids": ids}
			query := "FOR e IN @@e FILTER e." + filterField + " IN @ids"
			if len(edgeLabels) > 0 {
				bind["labels"] = edgeLabels
				query += " FILTER e._label IN @labels"
			}

			if outEdges {
				query += " RETURN { id: e." + filterField + ", edge: e }"
			} else {
				bind["@v"] = g.vertexCollection.Name()
				query += " FOR v IN @@v FILTER v._id == e." + targetField + " RETURN { id: e." + filterField + ", vertex: v }"
			}

			rows, err := g.ar.queryMaps(query, bind)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("getNeighborEdges")
				continue
			}

			for _, row := range rows {
				id, _ := row["id"].(string)
				for _, req := range reqMap[id] {
					if outEdges {
						if edoc, ok := row["edge"].(map[string]any); ok {
							e := unpackEdge(edoc)
							if !load {
								e.Data = map[string]any{}
							}
							req.Edge = e
							returnCount[id]++
							out <- req
						}
					} else {
						if vdoc, ok := row["vertex"].(map[string]any); ok {
							v := unpackVertex(vdoc)
							if !load {
								v.Data = map[string]any{}
							}
							req.Vertex = v
							returnCount[id]++
							out <- req
						}
					}
				}
			}

			if emitNull {
				idSet := makeStringSet(ids)
				for id := range idSet {
					if returnCount[id] == 0 {
						for _, req := range reqMap[id] {
							if outEdges {
								req.Edge = nil
							} else {
								req.Vertex = nil
							}
							out <- req
						}
					}
				}
			}

			for _, s := range signals {
				out <- s
			}
		}
	}()
	return out
}

func (g *Graph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "out", emitNull, edgeLabels, false, load)
}

func (g *Graph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "in", emitNull, edgeLabels, false, load)
}

func (g *Graph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "out", emitNull, edgeLabels, true, load)
}

func (g *Graph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "in", emitNull, edgeLabels, true, load)
}
